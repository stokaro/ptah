package schemaload_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/schemaload"
	"go.5x5.cz/ptah/core/renderer"
)

// oneTableTwoPolicySpellings declares a table without a schema and then names
// that table twice, once bare and once with the default schema. PostgreSQL
// reads both as `public.orders`, so this is one policy declared twice, not two
// policies. Measured on PostgreSQL 17.10, replaying these four statements
// exits 3 with `ERROR:  policy "p" for table "orders" already exists`
// (stokaro/ptah#1276).
const oneTableTwoPolicySpellings = `CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON orders        FOR ALL TO PUBLIC USING (tenant_id = 1);
CREATE POLICY p ON public.orders FOR ALL TO PUBLIC USING (tenant_id = 2);
`

// TestLoad_SQLSchemaFileFoldsTwoSpellingsOfOnePolicysTable pins the surface
// where invalid DDL escapes. `ptah schema render --schema-file` renders the
// loaded schema directly, with no comparison in between, so a schema carrying
// both spellings emitted two CREATE POLICY statements against one table and
// the database rejected the second.
//
// The survivor must be the first declaration: deduplication keeps the first,
// and a row-level-security predicate that silently changes from `tenant_id =
// 1` to `tenant_id = 2` is the same defect wearing a quieter coat.
func TestLoad_SQLSchemaFileFoldsTwoSpellingsOfOnePolicysTable(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(oneTableTwoPolicySpellings), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Name, qt.Equals, "p")
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "orders")
	c.Assert(database.RLSPolicies[0].UsingExpression, qt.Equals, "tenant_id = 1")

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(createPolicyStatements(statements), qt.DeepEquals, []string{
		"CREATE POLICY \"p\" ON \"orders\" FOR ALL TO PUBLIC\n    USING (tenant_id = 1)\n;",
	})
}

// TestLoad_SQLSchemaFileKeepsOnePolicyNamePerTable is the control the fold
// must not swallow: two tables in one schema may each carry a policy called
// `p`, and PostgreSQL keeps both rows in pg_policy.
func TestLoad_SQLSchemaFileKeepsOnePolicyNamePerTable(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE alpha_orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
CREATE TABLE zeta_orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE alpha_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE zeta_orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON alpha_orders FOR ALL TO PUBLIC USING (tenant_id = 1);
CREATE POLICY p ON zeta_orders  FOR ALL TO PUBLIC USING (tenant_id = 2);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 2)

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(createPolicyStatements(statements), qt.DeepEquals, []string{
		"CREATE POLICY \"p\" ON \"alpha_orders\" FOR ALL TO PUBLIC\n    USING (tenant_id = 1)\n;",
		"CREATE POLICY \"p\" ON \"zeta_orders\" FOR ALL TO PUBLIC\n    USING (tenant_id = 2)\n;",
	})
}

// createPolicyStatements keeps the rendered CREATE POLICY statements, trimmed,
// so a fixture asserts on the policies rather than on the whole schema.
func createPolicyStatements(statements []string) []string {
	trimmed := make([]string, 0, len(statements))
	for _, statement := range statements {
		trimmed = append(trimmed, strings.TrimSpace(statement))
	}
	return slices.DeleteFunc(trimmed, func(statement string) bool {
		return !strings.HasPrefix(statement, "CREATE POLICY ")
	})
}
