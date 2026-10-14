//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/core/ptaherr"
	"ptah.run/internal/dbtarget"
)

// TestCockroachDBRoleOptionsE2E_HappyPath creates a declared role on
// CockroachDB and plans the same schema again.
//
// CockroachDB v26.3.2 has no SUPERUSER and no INHERIT role option and refuses
// NOSUPERUSER and INHERIT as a syntax error, so a CREATE ROLE carrying them
// created no role at all. The second apply is the round trip: the role the
// server reports back has to match its declaration, or the plan is not empty.
func TestCockroachDBRoleOptionsE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	fixture := newCockroachRoleFixture(c, ctx, "ptah_ro_app")
	schema := writeCockroachRoleSchema(c, fixture.role, "login: true\n    create_db: true\n    replication: true")

	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", schema, "--db-url", fixture.targetURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	c.Assert(applied, qt.Contains,
		"CREATE ROLE "+quoteE2EIdent(fixture.role)+" WITH LOGIN CREATEDB NOCREATEROLE REPLICATION;\n")
	c.Assert(cockroachRoleAttributes(c, ctx, fixture.admin, fixture.role), qt.Equals,
		"login=true superuser=false createdb=true createrole=false inherit=true replication=true")

	reapplied, reappliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", schema, "--db-url", fixture.targetURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied, reappliedErr))
	c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// TestCockroachDBRoleOptionsE2E_FailurePath refuses a declared superuser
// before anything reaches the server. Leaving SUPERUSER out would create an
// ordinary role, and every later plan would report the difference again.
func TestCockroachDBRoleOptionsE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	fixture := newCockroachRoleFixture(c, ctx, "ptah_ro_boss")
	schema := writeCockroachRoleSchema(c, fixture.role, "superuser: true")

	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", schema, "--db-url", fixture.targetURL, "--auto-approve",
	})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	c.Assert(err, qt.ErrorMatches, `.*cockroachdb: CREATE ROLE `+fixture.role+`: declares SUPERUSER, `+
		`which CockroachDB has no role option for: a CockroachDB role is a superuser only as a member of admin, .*`)
	c.Assert(cockroachRoleAttributes(c, ctx, fixture.admin, fixture.role), qt.Equals, "")
}

// cockroachRoleFixture is an empty CockroachDB database and the name of a role
// the server does not hold yet.
type cockroachRoleFixture struct {
	admin     *sql.DB
	targetURL string
	role      string
}

// newCockroachRoleFixture creates the database and registers the role for
// cleanup. A role belongs to the cluster rather than the database, so it is
// dropped on its own after the database.
func newCockroachRoleFixture(c *qt.C, ctx context.Context, prefix string) cockroachRoleFixture {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })

	stamp := time.Now().UnixNano()
	role := fmt.Sprintf("%s_%d", prefix, stamp)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
		c.Check(dropErr, qt.IsNil, qt.Commentf("drop role %s", role))
	})
	database := fmt.Sprintf("%s_db_%d", prefix, stamp)
	createE2EDatabase(c, ctx, admin, database)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, database) })
	return cockroachRoleFixture{admin: admin, targetURL: replaceDatabaseName(c, adminURL, database), role: role}
}

// writeCockroachRoleSchema writes a schema file declaring one role with the
// given attribute lines, indented under the role.
func writeCockroachRoleSchema(c *qt.C, role, attributes string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.yaml")
	content := "roles:\n  " + role + ":\n    " + attributes + "\n"
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// cockroachRoleAttributes reads one role's attributes as the server reports
// them, or "" when the server holds no role of that name.
func cockroachRoleAttributes(c *qt.C, ctx context.Context, admin *sql.DB, role string) string {
	c.Helper()
	rows, err := admin.QueryContext(ctx, `
		SELECT format('login=%s superuser=%s createdb=%s createrole=%s inherit=%s replication=%s',
			rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolreplication)
		FROM pg_catalog.pg_roles
		WHERE rolname = $1`, role)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	attributes := ""
	for rows.Next() {
		c.Assert(rows.Scan(&attributes), qt.IsNil)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return attributes
}
