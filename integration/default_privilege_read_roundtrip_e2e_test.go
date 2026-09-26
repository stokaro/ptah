//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// TestDefaultPrivilegeReadRoundTripE2E_HappyPath reads a database whose
// default privileges live in the connection's default schema, renders it,
// applies the rendered SQL to a fresh database and compares the two.
//
// A table in the default schema is spelled unqualified; a default privilege is
// not, because its schema is the IN SCHEMA clause. A read that blanks the
// default schema of a pg_default_acl row, as it does for a table, stops both
// `ptah db read` and `ptah-compat schema inspect --format '{{ sql . }}'` at
// `ALTER DEFAULT PRIVILEGES requires a schema` (stokaro/ptah#3732).
//
// The fixture carries a grant and a revoke for tables and for functions, in
// the default schema, in a second schema, and in the global form, which has no
// IN SCHEMA. The global entries are not described -- the model has no spelling
// for them, and every desired-state source refuses one -- so the read must
// neither fail on them nor render a statement for them, and both read surfaces
// name them in a note on stderr (stokaro/ptah#3737). The schema-scoped entries
// must arrive in the fresh database exactly as pg_default_acl holds them in
// the source.
//
// CockroachDB and YugabyteDB are in the table because the same reader serves
// them and both hold the same pg_default_acl rows for this fixture.
func TestDefaultPrivilegeReadRoundTripE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			fixture := newDefaultPrivilegeFixture(c, ctx, test.engine)

			read, readErr, err := runPtahSplitStreams(ctx, []string{"db", "read", "--db-url", fixture.sourceURL})
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
			c.Assert(defaultPrivilegeStatements(read), qt.DeepEquals, fixture.statements("public"))
			c.Assert(readErr, qt.Contains, fixture.globalNote())

			inspected, inspectErr, err := runCompatSQLInspect(ctx, fixture.sourceURL)
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", inspectErr))
			c.Assert(defaultPrivilegeStatements(inspected), qt.DeepEquals, fixture.statements("public", "app"))
			c.Assert(inspectErr, qt.Contains, fixture.globalNote())

			both, bothErr, err := runPtahSplitStreams(ctx, []string{
				"db", "read", "--db-url", fixture.sourceURL, "--schemas", "public,app",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", bothErr))
			c.Assert(defaultPrivilegeStatements(both), qt.DeepEquals, fixture.statements("public", "app"))
			rendered := filepath.Join(c.TempDir(), "rendered.sql")
			c.Assert(os.WriteFile(rendered, []byte(both), 0o600), qt.IsNil)

			// The source against its own description. With the schema blanked,
			// a declared grantor makes the comparison plan a REVOKE for every
			// such row, which the renderer refuses the same way.
			settled, settledErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "compare", "--schema-file", rendered, "--db-url", fixture.sourceURL,
				"--schemas", "public,app", "--exit-code",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled, settledErr))

			applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", rendered, "--db-url", fixture.targetURL,
				"--schemas", "public,app", "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))

			compared, comparedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "compare", "--schema-file", rendered, "--db-url", fixture.targetURL,
				"--schemas", "public,app", "--exit-code",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, comparedErr))
			c.Assert(compared, qt.Contains, "No schema differences detected.")

			source := fixture.schemaScopedACL(c, ctx, fixture.sourceURL)
			c.Assert(source, qt.HasLen, 3, qt.Commentf("the fixture holds %v", source))
			c.Assert(fixture.globalEntries(c, ctx, fixture.sourceURL), qt.Equals, 2,
				qt.Commentf("without global entries in the source, the read never met one"))
			c.Assert(fixture.schemaScopedACL(c, ctx, fixture.targetURL), qt.DeepEquals, source)
		})
	}
}

// defaultPrivilegeFixture is a source database holding default privileges, an
// empty target database on the same server, and the three roles they name.
type defaultPrivilegeFixture struct {
	sourceURL string
	targetURL string
	grantor   string
	reader    string
	executor  string
}

// newDefaultPrivilegeFixture creates both databases and the roles, and seeds
// the source. A role belongs to the server rather than to a database, so its
// cleanup is registered first and runs after both databases are gone, when
// nothing depends on it any more.
func newDefaultPrivilegeFixture(c *qt.C, ctx context.Context, engine dbtarget.Engine) defaultPrivilegeFixture {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	stamp := time.Now().UnixNano()
	fixture := defaultPrivilegeFixture{
		grantor:  fmt.Sprintf("ptah_dp_owner_%d", stamp),
		reader:   fmt.Sprintf("ptah_dp_reader_%d", stamp),
		executor: fmt.Sprintf("ptah_dp_executor_%d", stamp),
	}
	roles := []string{fixture.grantor, fixture.reader, fixture.executor}
	for _, role := range roles {
		_, err := admin.ExecContext(ctx, "CREATE ROLE "+quoteE2EIdent(role)+" NOLOGIN")
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() {
			_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
			c.Check(dropErr, qt.IsNil, qt.Commentf("drop role %s", role))
		})
	}

	source := fmt.Sprintf("ptah_dp_source_%d", stamp)
	target := fmt.Sprintf("ptah_dp_target_%d", stamp)
	for _, name := range []string{source, target} {
		createE2EDatabase(c, ctx, admin, name)
		c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	}
	fixture.sourceURL = replaceDatabaseName(c, adminURL, source)
	fixture.targetURL = replaceDatabaseName(c, adminURL, target)

	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, fixture.sourceURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	for _, statement := range fixture.seed() {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return fixture
}

// seed is a grant and a revoke for tables and for functions in the default
// schema, a grant in a second schema, and a grant and a revoke in the global
// form. Each revoke takes back only part of what was granted, so the source
// ends up holding something the read has to carry.
func (f defaultPrivilegeFixture) seed() []string {
	grantor := "ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(f.grantor)
	reader := quoteE2EIdent(f.reader)
	executor := quoteE2EIdent(f.executor)
	return []string{
		"CREATE SCHEMA app",
		grantor + " IN SCHEMA public GRANT SELECT, INSERT ON TABLES TO " + reader,
		grantor + " IN SCHEMA public GRANT UPDATE ON TABLES TO " + reader + " WITH GRANT OPTION",
		grantor + " IN SCHEMA public REVOKE INSERT ON TABLES FROM " + reader,
		grantor + " IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO " + executor,
		grantor + " IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO " + reader,
		grantor + " IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM " + reader,
		grantor + " IN SCHEMA app GRANT SELECT ON TABLES TO " + reader,
		grantor + " GRANT SELECT ON TABLES TO " + reader,
		grantor + " REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
	}
}

// statements is what a description of the named schemas renders for the
// seed: the schema-scoped entries that survive their revokes, and nothing for
// the global ones. Sorted, as [defaultPrivilegeStatements] is.
func (f defaultPrivilegeFixture) statements(schemas ...string) []string {
	prefix := "ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(f.grantor) + " IN SCHEMA "
	reader := quoteE2EIdent(f.reader)
	bySchema := map[string][]string{
		"public": {
			prefix + `"public" GRANT EXECUTE ON FUNCTIONS TO ` + quoteE2EIdent(f.executor) + ";",
			prefix + `"public" GRANT SELECT ON TABLES TO ` + reader + ";",
			prefix + `"public" GRANT UPDATE ON TABLES TO ` + reader + " WITH GRANT OPTION;",
		},
		"app": {prefix + `"app" GRANT SELECT ON TABLES TO ` + reader + ";"},
	}
	var want []string
	for _, schema := range schemas {
		want = append(want, bySchema[schema]...)
	}
	slices.Sort(want)
	return want
}

// globalNote is the note a read of the source prints for the seed's two global
// entries, the grant on tables and the revoke on functions. The source's
// catalog is what says they exist; see [defaultPrivilegeFixture.globalEntries].
func (f defaultPrivilegeFixture) globalNote() string {
	return "note: 2 global default privileges, set by ALTER DEFAULT PRIVILEGES without IN SCHEMA," +
		" are not described, because no schema source can declare one; a description applied" +
		" to another database does not carry them: FUNCTIONS for " + f.grantor +
		", TABLES for " + f.grantor + ".\n"
}

// schemaScopedACL is every schema-scoped pg_default_acl row of one database,
// as grantor, schema, object class and the ACL the server stores. The ACL text
// is compared verbatim between two databases on one server, so a privilege, a
// grantee or a grant option lost on the way shows up as a different string.
func (f defaultPrivilegeFixture) schemaScopedACL(c *qt.C, ctx context.Context, dbURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
		SELECT pg_get_userbyid(d.defaclrole) || ' ' || n.nspname || ' ' ||
			d.defaclobjtype::text || ' ' || d.defaclacl::text
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		WHERE pg_get_userbyid(d.defaclrole) = $1
		ORDER BY 1`, f.grantor)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var entries []string
	for rows.Next() {
		var entry string
		c.Assert(rows.Scan(&entry), qt.IsNil)
		entries = append(entries, entry)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return entries
}

// globalEntries counts the grantor's global pg_default_acl rows, the ones
// recorded with defaclnamespace 0.
func (f defaultPrivilegeFixture) globalEntries(c *qt.C, ctx context.Context, dbURL string) int {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var count int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_default_acl
		WHERE defaclnamespace = 0 AND pg_get_userbyid(defaclrole) = $1`, f.grantor,
	).Scan(&count), qt.IsNil)
	return count
}

// defaultPrivilegeStatements is every ALTER DEFAULT PRIVILEGES line of a
// rendered description, sorted: the native read and the compatibility inspect
// order their statements differently, and the order is not what is tested.
func defaultPrivilegeStatements(rendered string) []string {
	lines := slices.DeleteFunc(strings.Split(rendered, "\n"), func(line string) bool {
		return !strings.HasPrefix(line, "ALTER DEFAULT PRIVILEGES")
	})
	slices.Sort(lines)
	return lines
}

// runCompatSQLInspect runs `schema inspect --format '{{ sql . }}'` on the
// compatibility surface, the second command stokaro/ptah#3732 names.
func runCompatSQLInspect(ctx context.Context, dbURL string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"schema", "inspect", "--url", dbURL, "--format", "{{ sql . }}"})
	err = cmd.ExecuteContext(ctx)
	return out.String(), errOut.String(), err
}
