//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A row-security policy on a table in a default schema other than public,
// whose expression reads a relation in a later schema of the path by its bare
// name (stokaro/ptah#5033). PostgreSQL prints that relation without a schema,
// and a policy the read found on the default schema has to pair with its
// declaration in every verb, or the verb plans the policy again on every run.

// policySearchPathTables is what every database here starts from: the schema
// reporting, public.orders, and reporting.audit with row security on.
var policySearchPathTables = []string{
	"CREATE SCHEMA reporting",
	"CREATE TABLE public.orders (id integer PRIMARY KEY)",
	"CREATE TABLE reporting.audit (id integer PRIMARY KEY)",
	"ALTER TABLE reporting.audit ENABLE ROW LEVEL SECURITY",
}

// TestSchemaApplyPairsAPolicyAlongTheSearchPathE2E applies a declared policy,
// then applies, compares and generates again: each later run finds the policy
// it created and plans nothing. Without the pairing the second apply plans
// CREATE POLICY for a policy that exists, or ALTER POLICY on every run, and
// compare and generate report the same change.
func TestSchemaApplyPairsAPolicyAlongTheSearchPathE2E(t *testing.T) {
	tests := []struct {
		name       string
		searchPath string
		// scope is what each verb is given beside the URL.
		scope    []string
		declared string
	}{
		{
			name:       "declared on reporting.audit, the path naming two schemas",
			searchPath: "reporting,public",
			scope:      []string{"--schemas", "public,reporting"},
			declared: `CREATE SCHEMA reporting;
CREATE TABLE public.orders (id integer PRIMARY KEY);
CREATE TABLE reporting.audit (id integer PRIMARY KEY);
ALTER TABLE reporting.audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON reporting.audit USING (EXISTS (SELECT 1 FROM orders));
`,
		},
		{
			name:       "declared on audit, the schema left to a one-schema path",
			searchPath: "reporting",
			declared: `CREATE TABLE audit (id integer PRIMARY KEY);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (EXISTS (SELECT 1 FROM public.orders));
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := newPolicySearchPathDatabase(c, policySearchPathTables)
			target := withURLParameter(database, "search_path", test.searchPath)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.declared), 0o600), qt.IsNil)
			apply := slices.Concat([]string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve"}, test.scope)

			applied := runPtahIn(c, apply...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, `CREATE POLICY "audit_visible"`)
			c.Assert(searchPathPolicies(c, database), qt.Equals, "reporting.audit.audit_visible")

			reapplied := runPtahIn(c, apply...)

			c.Assert(reapplied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied.Stdout, reapplied.Stderr))
			c.Assert(reapplied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")

			compared := runPtahIn(c, slices.Concat([]string{"schema", "compare", "--db-url", target, "--schema-file", schema, "--exit-code"}, test.scope)...)

			c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
			c.Assert(compared.Stdout, qt.Contains, "No schema differences detected.\n")

			migrations := c.TempDir()
			generated := runPtahIn(c, slices.Concat([]string{"migrations", "generate", "--db-url", target, "--schema-file", schema,
				"--migrations-dir", migrations}, test.scope)...)

			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			c.Assert(generated.Stdout, qt.Contains, "no migration files generated")
			written, err := os.ReadDir(migrations)
			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 0)
		})
	}
}

// TestSchemaApplyRefusesAViewChangeAPolicyAlongTheSearchPathReadsE2E changes
// the body of a materialized view the policy reads by its bare name. The
// rebuild's DROP ... CASCADE would take the policy, so apply refuses before
// the server changes, as compare and generate do. A policy apply did not pair
// with its declaration escapes the refusal, and the CASCADE drops it before a
// CREATE POLICY puts it back.
func TestSchemaApplyRefusesAViewChangeAPolicyAlongTheSearchPathReadsE2E(t *testing.T) {
	c := qt.New(t)
	database := newPolicySearchPathDatabase(c, append(slices.Clone(policySearchPathTables),
		"CREATE MATERIALIZED VIEW public.order_totals AS SELECT id FROM public.orders",
		"CREATE POLICY audit_visible ON reporting.audit USING (EXISTS (SELECT 1 FROM order_totals))",
	))
	target := withURLParameter(database, "search_path", "reporting,public")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(`CREATE SCHEMA reporting;
CREATE TABLE public.orders (id integer PRIMARY KEY);
CREATE MATERIALIZED VIEW public.order_totals AS SELECT id, id * 2 AS doubled FROM public.orders;
CREATE TABLE reporting.audit (id integer PRIMARY KEY);
ALTER TABLE reporting.audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON reporting.audit USING (EXISTS (SELECT 1 FROM order_totals));
`), 0o600), qt.IsNil)

	got := runPtahIn(c, "schema", "apply", "--db-url", target, "--schemas", "public,reporting", "--schema-file", schema, "--auto-approve")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "ptah.run/pgpolicy/policy reporting.audit.audit_visible reads it, and the drop's CASCADE would drop it too")
	c.Assert(searchPathPolicies(c, database), qt.Equals, "reporting.audit.audit_visible")
	var columns int
	db := openPolicySearchPathDatabase(c, database)
	c.Assert(db.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.order_totals'::regclass AND attnum > 0`).Scan(&columns), qt.IsNil)
	c.Assert(columns, qt.Equals, 1)
}

// newPolicySearchPathDatabase creates a database, dropped when the test ends,
// runs setup in it, and returns its URL, which pins no search_path.
func newPolicySearchPathDatabase(c *qt.C, setup []string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin := openPolicySearchPathDatabase(c, adminURL)
	name := fmt.Sprintf("ptah_policy_path_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	database := replaceDatabaseName(c, adminURL, name)
	for _, statement := range setup {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	return database
}

// openPolicySearchPathDatabase opens a connection closed when the test ends.
func openPolicySearchPathDatabase(c *qt.C, database string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

// searchPathPolicies lists the database's policies as schema.table.policy,
// comma-separated in name order.
func searchPathPolicies(c *qt.C, database string) string {
	c.Helper()
	var policies string
	err := openPolicySearchPathDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(schemaname || '.' || tablename || '.' || policyname, ', '
			ORDER BY schemaname, tablename, policyname), '')
		FROM pg_policies`).Scan(&policies)
	c.Assert(err, qt.IsNil)
	return policies
}
