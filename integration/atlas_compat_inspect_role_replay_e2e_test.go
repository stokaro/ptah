//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/cmd/atlas"
)

// TestAtlasCompatInspectRoleReplayE2E is the property stokaro/ptah#1267 names
// in its definition of done: "Ptah's own inspect output replays into a clean
// dev database at exit 0".
//
// The dev database here is a SIBLING of the inspected one, on the same server,
// which is what a dev database normally is and what makes this the hard case.
// Resetting a database does not clear the server's roles, so the roles the
// description is right to name -- the grantee of the schema's own GRANT, and
// the owner that grant's explicit ACL carries -- are all still there when the
// document is replayed. `CREATE ROLE` is not idempotent, so before
// stokaro/ptah#1305's dev-materialization partition this died with
//
//	Error: materialize schema on dev database: failed to execute SQL statement:
//	SQL execution failed: ERROR: role "ptah_user" already exists (SQLSTATE 42710)
//
// measured on PostgreSQL 17.10 against exactly the fixture below.
//
// Scoping the description is what makes the round trip a FIXED POINT rather
// than merely non-fatal: the replayed dev database receives the same grants, so
// the same roles are referenced there, so the second document names the same
// roles as the first. The assertion is therefore equality of the two documents
// and not just a non-empty second one.
//
// Roles are not excluded from either inspect, unlike
// [runAtlasCompatInspect]'s `--exclude *[type=role]`. The exclusion is what
// that test needs to reach the extension question; here it would remove the
// subject.
func TestAtlasCompatInspectRoleReplayE2E(t *testing.T) {
	adminURL := requirePostgresE2EDatabaseURL(t)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	stamp := time.Now().UnixNano()
	sourceName := fmt.Sprintf("ptah_role_replay_src_%d", stamp)
	devName := fmt.Sprintf("ptah_role_replay_dev_%d", stamp)
	roleName := fmt.Sprintf("ptah_role_replay_reader_%d", stamp)

	// The role is cluster-scoped, so it outlives both databases and PostgreSQL
	// refuses to drop it while a grant in either still depends on it. Its
	// cleanup is registered FIRST so that it runs LAST.
	createReplayRole(c, ctx, adminDB, roleName)
	defer dropReplayRole(c, context.Background(), adminDB, roleName)
	createE2EDatabase(c, ctx, adminDB, sourceName)
	defer dropE2EDatabase(c, context.Background(), adminDB, sourceName)
	createE2EDatabase(c, ctx, adminDB, devName)
	defer dropE2EDatabase(c, context.Background(), adminDB, devName)

	sourceURL := replaceDatabaseName(c, adminURL, sourceName)
	devURL := replaceDatabaseName(c, adminURL, devName)

	seedRoleReplayDB(c, ctx, sourceURL, roleName)

	inspected, _ := runRoleAwareCompatInspect(c, sourceURL, "")
	c.Assert(inspected, qt.Contains, fmt.Sprintf("role %q", roleName),
		qt.Commentf("the fixture's own grantee is missing, so the replay would prove nothing"))

	documentPath := filepath.Join(t.TempDir(), "inspected.hcl")
	c.Assert(os.WriteFile(documentPath, []byte(inspected), 0o600), qt.IsNil)

	replayed, diagnostics := runRoleAwareCompatInspect(c, "file://"+documentPath, devURL)

	// The note is the disclosure half: a role the dev database was not given is
	// described as the server has it, and an operator is told which.
	c.Assert(diagnostics, qt.Contains, fmt.Sprintf("%q", roleName))
	c.Assert(diagnostics, qt.Contains, "already exist on the server hosting the dev database")

	c.Assert(replayed, qt.Contains, fmt.Sprintf("role %q", roleName))
	c.Assert(replayed, qt.Contains, "permission {")
	c.Assert(replayed, qt.Equals, inspected)
}

// TestAtlasCompatInspectCreatesARoleTheServerLacksE2E is the control in the
// other direction, and it is what keeps the partition from becoming "a dev
// database never creates a role".
//
// A document naming a role the server has never seen still gets it, because
// that case never produced SQLSTATE 42710 in the first place -- it is how the
// same document materializes on a second cluster or on an empty CI catalog.
// Skipping it would break the replay in a way #1267 never asked for: the GRANT
// that follows would then name a role nothing on that server declares.
func TestAtlasCompatInspectCreatesARoleTheServerLacksE2E(t *testing.T) {
	adminURL := requirePostgresE2EDatabaseURL(t)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	stamp := time.Now().UnixNano()
	devName := fmt.Sprintf("ptah_role_new_dev_%d", stamp)
	roleName := fmt.Sprintf("ptah_role_new_reader_%d", stamp)

	createE2EDatabase(c, ctx, adminDB, devName)
	defer dropE2EDatabase(c, context.Background(), adminDB, devName)
	defer dropReplayRole(c, context.Background(), adminDB, roleName)

	devURL := replaceDatabaseName(c, adminURL, devName)

	assertRoleAbsent(c, ctx, adminDB, roleName)

	document := fmt.Sprintf(`schema "public" {
}

role %q {
  inherit = true
}

table "widgets" {
  schema = schema.public
  column "id" {
    type = integer
  }
  primary_key {
    columns = [column.id]
  }
}

permission {
  to = role.%s
  for = table.widgets
  privileges = ["SELECT"]
}
`, roleName, roleName)

	documentPath := filepath.Join(t.TempDir(), "declared.hcl")
	c.Assert(os.WriteFile(documentPath, []byte(document), 0o600), qt.IsNil)

	rendered, diagnostics := runRoleAwareCompatInspect(c, "file://"+documentPath, devURL)

	c.Assert(rendered, qt.Contains, fmt.Sprintf("role %q", roleName))
	c.Assert(diagnostics, qt.Not(qt.Contains), "already exist on the server hosting the dev database")
	assertRolePresent(c, ctx, adminDB, roleName)
}

// createReplayRole creates the cluster-scoped role the fixture grants to.
func createReplayRole(c *qt.C, ctx context.Context, adminDB *sql.DB, roleName string) {
	c.Helper()

	_, err := adminDB.ExecContext(ctx, "CREATE ROLE "+quoteE2EIdent(roleName)+" NOLOGIN")
	c.Assert(err, qt.IsNil)
}

// seedRoleReplayDB creates the one table that grants to the fixture's role,
// then verifies through the catalog that the grant landed. A fixture whose
// GRANT silently did nothing would make the whole replay vacuous.
func seedRoleReplayDB(c *qt.C, ctx context.Context, dbURL, roleName string) {
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE TABLE orders (id integer PRIMARY KEY, note text)")
	c.Assert(err, qt.IsNil)
	// #nosec -- roleName is an identifier generated by this test and quoted by quoteE2EIdent.
	_, err = db.ExecContext(ctx, "GRANT SELECT ON TABLE orders TO "+quoteE2EIdent(roleName))
	c.Assert(err, qt.IsNil)

	var granted bool
	c.Assert(db.QueryRowContext(ctx,
		"SELECT has_table_privilege($1, 'orders', 'SELECT')", roleName,
	).Scan(&granted), qt.IsNil)
	c.Assert(granted, qt.IsTrue,
		qt.Commentf("the catalog reports no grant to %q", roleName))
}

// dropReplayRole removes the cluster-scoped role the fixture created. It runs
// after both databases are dropped, so nothing depends on the role any more.
func dropReplayRole(c *qt.C, ctx context.Context, adminDB *sql.DB, roleName string) {
	c.Helper()

	_, err := adminDB.ExecContext(ctx, "DROP ROLE IF EXISTS "+quoteE2EIdent(roleName))
	c.Assert(err, qt.IsNil)
}

// assertRoleAbsent is the precondition of the control: a role the server
// already had would make "it was created" unfalsifiable.
func assertRoleAbsent(c *qt.C, ctx context.Context, adminDB *sql.DB, roleName string) {
	c.Helper()

	var present int
	c.Assert(adminDB.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_roles WHERE rolname = $1", roleName,
	).Scan(&present), qt.IsNil)
	c.Assert(present, qt.Equals, 0)
}

// assertRolePresent reads the catalog rather than the rendered document.
// Rendered is not applied: a document naming the role proves only that the
// renderer wrote it down.
func assertRolePresent(c *qt.C, ctx context.Context, adminDB *sql.DB, roleName string) {
	c.Helper()

	var present int
	c.Assert(adminDB.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_roles WHERE rolname = $1", roleName,
	).Scan(&present), qt.IsNil)
	c.Assert(present, qt.Equals, 1)
}

// runRoleAwareCompatInspect runs `schema inspect` on the compatibility surface
// and returns the rendered document and the diagnostics stream, failing the
// test on a non-nil error.
func runRoleAwareCompatInspect(c *qt.C, sourceURL, devURL string) (rendered, diagnostics string) {
	c.Helper()

	args := []string{
		"schema", "inspect",
		"--url", sourceURL,
		"--format", "{{ hcl . }}",
	}
	args = append(args, devURLArgs(devURL)...)

	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", errOut.String()))

	return strings.TrimSpace(out.String()) + "\n", errOut.String()
}
