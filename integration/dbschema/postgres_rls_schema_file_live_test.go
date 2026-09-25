//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// These tests carry FORCE ROW LEVEL SECURITY and a restrictive policy from a
// SQL schema file through the whole path: parse, model, render, the server,
// the catalog read and the comparison. Each flag changes what a role can read,
// which only the server can show, so the flags are also measured in rows.

// rlsFileFixture is a throwaway schema, a non-superuser role that owns its
// table, and the connection that set them up. A superuser bypasses row-level
// security even when a table forces it, so the rows are read as the owner role.
type rlsFileFixture struct {
	conn   *dbschema.DatabaseConnection
	schema string
	owner  string
}

func newRLSFileFixture(c *qt.C) rlsFileFixture {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	f := rlsFileFixture{
		conn:   conn,
		schema: fmt.Sprintf("ptah_rlsfile_%d", suffix),
		owner:  fmt.Sprintf("ptah_rlsfile_owner_%d", suffix),
	}
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+f.schema+`"`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE ROLE "`+f.owner+`" NOLOGIN NOSUPERUSER NOBYPASSRLS`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		cleanup := context.Background()
		_, dropSchema := conn.ExecContext(cleanup, `DROP SCHEMA IF EXISTS "`+f.schema+`" CASCADE`)
		c.Check(dropSchema, qt.IsNil)
		_, dropRole := conn.ExecContext(cleanup, `DROP ROLE IF EXISTS "`+f.owner+`"`)
		c.Check(dropRole, qt.IsNil)
		dbschema.CloseAndWarn(conn)
	})
	return f
}

// load reads a schema file the way `--schema-file` does.
func (f rlsFileFixture) load(c *qt.C, body string) *schemamodel.Database {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(strings.ReplaceAll(body, "S.", `"`+f.schema+`".`)), 0o600), qt.IsNil)
	db, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	return db
}

func (f rlsFileFixture) exec(c *qt.C, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

func (f rlsFileFixture) compare(c *qt.C, desired *schemamodel.Database) *difftypes.SchemaDiff {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	return schemadiff.CompareWithDialect(desired, live, platform.Postgres)
}

// visibleTenants is what the owner role reads from the table, in order.
func (f rlsFileFixture) visibleTenants(c *qt.C) string {
	c.Helper()
	ctx := c.Context()
	tx, err := f.conn.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `SET LOCAL ROLE "`+f.owner+`"`)
	c.Assert(err, qt.IsNil)
	var tenants string
	c.Assert(tx.QueryRowContext(ctx,
		`SELECT coalesce(string_agg(tenant, ',' ORDER BY tenant), '') FROM "`+f.schema+`".docs`,
	).Scan(&tenants), qt.IsNil)
	return tenants
}

func assertNoRLSChanges(c *qt.C, diff *difftypes.SchemaDiff) {
	c.Helper()
	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.RLSPoliciesModified))
	c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSForceChanged, qt.HasLen, 0)
}

const rlsFileTable = `CREATE TABLE S.docs (id integer PRIMARY KEY, tenant text NOT NULL);
`

// rlsFilePolicies is a permissive policy admitting two tenants and a second
// policy over one of them, whose kind the argument names.
//
// The roles and expressions are written the way the catalog reports them.
// This comparison runs without a server to normalize a declared expression, so
// any other spelling would be a difference in the text, not in the flags these
// tests are about.
func rlsFilePolicies(kind string) string {
	return `CREATE POLICY docs_two ON S.docs TO PUBLIC USING ((tenant = ANY (ARRAY['a'::text, 'b'::text])));
CREATE POLICY docs_a ON S.docs AS ` + kind + ` TO PUBLIC USING ((tenant = 'a'::text));
`
}

// TestPostgresLiveSQLSchemaFileForceAndRestrictiveConverge applies a schema
// file that forces row-level security and declares a restrictive policy, reads
// the database back and compares: nothing is left to do. The rows show that
// both flags reached the server: the owner is bound by the policies, and the
// restrictive one narrows what the permissive one admits.
func TestPostgresLiveSQLSchemaFileForceAndRestrictiveConverge(t *testing.T) {
	c := qt.New(t)
	f := newRLSFileFixture(c)
	// FORCE comes before ENABLE on purpose: the two meet in one enablement
	// whichever order the file writes them in.
	desired := f.load(c, rlsFileTable+`ALTER TABLE S.docs FORCE ROW LEVEL SECURITY;
ALTER TABLE S.docs ENABLE ROW LEVEL SECURITY;
`+rlsFilePolicies("RESTRICTIVE"))

	statements, err := renderer.GetOrderedCreateStatements(desired, platform.Postgres)
	c.Assert(err, qt.IsNil)
	f.exec(c, statements)
	f.exec(c, []string{
		`INSERT INTO "` + f.schema + `".docs VALUES (1, 'a'), (2, 'b'), (3, 'c')`,
		`ALTER TABLE "` + f.schema + `".docs OWNER TO "` + f.owner + `"`,
		`GRANT USAGE ON SCHEMA "` + f.schema + `" TO "` + f.owner + `"`,
	})

	assertNoRLSChanges(c, f.compare(c, desired))
	c.Assert(f.visibleTenants(c), qt.Equals, "a")
}

// TestPostgresLiveRLSFlagsChangeIsPlanned moves each flag on a table that keeps
// row-level security enabled, plans the change from the comparison, applies it,
// and reads the rows again. Each step's rows are the proof that the planned
// statement did what the flag means.
func TestPostgresLiveRLSFlagsChangeIsPlanned(t *testing.T) {
	c := qt.New(t)
	f := newRLSFileFixture(c)
	forcedRestrictive := f.load(c, rlsFileTable+`ALTER TABLE S.docs ENABLE ROW LEVEL SECURITY;
ALTER TABLE S.docs FORCE ROW LEVEL SECURITY;
`+rlsFilePolicies("RESTRICTIVE"))
	unforcedRestrictive := f.load(c, rlsFileTable+`ALTER TABLE S.docs ENABLE ROW LEVEL SECURITY;
`+rlsFilePolicies("RESTRICTIVE"))
	forcedPermissive := f.load(c, rlsFileTable+`ALTER TABLE S.docs ENABLE ROW LEVEL SECURITY;
ALTER TABLE S.docs FORCE ROW LEVEL SECURITY;
`+rlsFilePolicies("PERMISSIVE"))

	statements, err := renderer.GetOrderedCreateStatements(unforcedRestrictive, platform.Postgres)
	c.Assert(err, qt.IsNil)
	f.exec(c, statements)
	f.exec(c, []string{
		`INSERT INTO "` + f.schema + `".docs VALUES (1, 'a'), (2, 'b'), (3, 'c')`,
		`ALTER TABLE "` + f.schema + `".docs OWNER TO "` + f.owner + `"`,
		`GRANT USAGE ON SCHEMA "` + f.schema + `" TO "` + f.owner + `"`,
	})
	// Not forced: the owner reads past every policy.
	c.Assert(f.visibleTenants(c), qt.Equals, "a,b,c")

	steps := []struct {
		name        string
		desired     *schemamodel.Database
		wantForce   []string
		wantVisible string
	}{
		{
			name:        "force on",
			desired:     forcedRestrictive,
			wantForce:   []string{"FORCE ROW LEVEL SECURITY"},
			wantVisible: "a",
		},
		{
			name:        "restrictive to permissive",
			desired:     forcedPermissive,
			wantForce:   nil,
			wantVisible: "a,b",
		},
		{
			name:        "back to restrictive and force off",
			desired:     unforcedRestrictive,
			wantForce:   []string{"NO FORCE ROW LEVEL SECURITY"},
			wantVisible: "a,b,c",
		},
	}
	for _, step := range steps {
		diff := rlsPart(f.compare(c, step.desired))
		planned, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)
		c.Assert(err, qt.IsNil, qt.Commentf("step %s", step.name))
		c.Assert(forceStatements(planned), qt.DeepEquals, step.wantForce, qt.Commentf("step %s", step.name))
		f.exec(c, planned)
		assertNoRLSChanges(c, f.compare(c, step.desired))
		c.Assert(f.visibleTenants(c), qt.Equals, step.wantVisible, qt.Commentf("step %s", step.name))
	}
}

// rlsPart keeps the row-level security half of a comparison. The read covers
// the server's roles and grants too, and planning those against a shared
// server would drop what other tests and users own.
func rlsPart(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		RLSPoliciesAdded:        diff.RLSPoliciesAdded,
		RLSPoliciesRemoved:      diff.RLSPoliciesRemoved,
		RLSPoliciesModified:     diff.RLSPoliciesModified,
		RLSEnabledTablesAdded:   diff.RLSEnabledTablesAdded,
		RLSEnabledTablesRemoved: diff.RLSEnabledTablesRemoved,
		RLSForceChanged:         diff.RLSForceChanged,
	}
}

// forceStatements keeps the FORCE clause of each planned statement that changes
// the flag, so a step can say which way it moved and that nothing else did.
func forceStatements(statements []string) []string {
	var out []string
	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			if strings.HasPrefix(line, "--") || !strings.Contains(line, "FORCE ROW LEVEL SECURITY") {
				continue
			}
			_, clause, _ := strings.Cut(line, `docs" `)
			out = append(out, strings.TrimSuffix(clause, ";"))
		}
	}
	return out
}
