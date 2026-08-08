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

// oneTableTwoPolicyCases spells the same table three ways: declared as
// `orders`, enabled as `ORDERS`, and carrying `p` under both spellings. An
// unquoted PostgreSQL identifier folds to lower case, so all three are
// `public.orders`. Measured on PostgreSQL 17.10, replaying these four
// statements exits 3 with `ERROR:  policy "p" for table "orders" already
// exists` (stokaro/ptah#1276).
const oneTableTwoPolicyCases = `CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE ORDERS ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON orders  FOR ALL TO PUBLIC USING (tenant_id = 1);
CREATE POLICY p ON ORDERS  FOR ALL TO PUBLIC USING (tenant_id = 2);
`

// TestLoad_SQLSchemaFileFoldsACaseVariantOfOnePolicysTable is the case-variant
// half of the same surface. The SQL reader stores an identifier as it was
// written, so `ORDERS` arrived as a table of its own: the render carried a
// second CREATE POLICY and an ALTER TABLE against a table nothing declared,
// and PostgreSQL answered `relation "ORDERS" does not exist`.
//
// Every statement here must name `orders`, because that is the table the
// schema declares and the only one the render may reach.
func TestLoad_SQLSchemaFileFoldsACaseVariantOfOnePolicysTable(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(oneTableTwoPolicyCases), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "orders")
	c.Assert(database.RLSPolicies[0].UsingExpression, qt.Equals, "tenant_id = 1")
	c.Assert(database.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables[0].Table, qt.Equals, "orders")

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(createPolicyStatements(statements), qt.DeepEquals, []string{
		"CREATE POLICY \"p\" ON \"orders\" FOR ALL TO PUBLIC\n    USING (tenant_id = 1)\n;",
	})
	c.Assert(rowLevelSecurityStatements(statements), qt.DeepEquals, []string{
		`ALTER TABLE "orders" ENABLE ROW LEVEL SECURITY;`,
	})
}

// TestLoad_SQLSchemaFileKeepsACaseVariantDeclaredFirst pins the half a
// deduplication key alone cannot reach. Deduplication keeps the first
// declaration, so when the variant spelling comes first the survivor still has
// to name the declared table.
func TestLoad_SQLSchemaFileKeepsACaseVariantDeclaredFirst(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON ORDERS FOR ALL TO PUBLIC USING (tenant_id = 2);
CREATE POLICY p ON orders FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(createPolicyStatements(statements), qt.DeepEquals, []string{
		"CREATE POLICY \"p\" ON \"orders\" FOR ALL TO PUBLIC\n    USING (tenant_id = 2)\n;",
	})
}

// TestLoad_SQLSchemaFileBindsToTheDeclaredCase shows the rule is "name the
// declared table", not "lower-case everything". The SQL reader keeps
// `CREATE TABLE ORDERS` as `ORDERS`, so a policy written `ON orders` has to
// reach `ORDERS` -- the only table this schema declares. Rendered the other
// way the statements name a table nothing declared, and PostgreSQL 17.10
// answers `relation "orders" does not exist`.
func TestLoad_SQLSchemaFileBindsToTheDeclaredCase(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE ORDERS (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON orders FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "ORDERS")

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(createPolicyStatements(statements), qt.DeepEquals, []string{
		"CREATE POLICY \"p\" ON \"ORDERS\" FOR ALL TO PUBLIC\n    USING (tenant_id = 1)\n;",
	})
	c.Assert(rowLevelSecurityStatements(statements), qt.DeepEquals, []string{
		`ALTER TABLE "ORDERS" ENABLE ROW LEVEL SECURITY;`,
	})
}

// TestLoad_SQLSchemaFileKeepsTwoTablesDifferingOnlyInCase is the boundary the
// case fold must not cross. A quoted identifier keeps its case, so `orders`
// and `"ORDERS"` are two tables; PostgreSQL 17.10 accepts a policy called `p`
// on each and reports both rows in pg_policy.
func TestLoad_SQLSchemaFileKeepsTwoTablesDifferingOnlyInCase(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
CREATE TABLE "ORDERS" (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE "ORDERS" ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON orders FOR ALL TO PUBLIC USING (tenant_id = 1);
CREATE POLICY p ON "ORDERS" FOR ALL TO PUBLIC USING (tenant_id = 2);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 2)

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(createPolicyStatements(statements), qt.DeepEquals, []string{
		"CREATE POLICY \"p\" ON \"orders\" FOR ALL TO PUBLIC\n    USING (tenant_id = 1)\n;",
		"CREATE POLICY \"p\" ON \"ORDERS\" FOR ALL TO PUBLIC\n    USING (tenant_id = 2)\n;",
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
	return statementsWithPrefix(statements, "CREATE POLICY ")
}

// rowLevelSecurityStatements keeps the rendered RLS enablement statements. The
// policy and the enablement name the same table, and a render is only
// replayable when both spell it the way the schema declares it.
func rowLevelSecurityStatements(statements []string) []string {
	return statementsWithPrefix(statements, "ALTER TABLE ")
}

func statementsWithPrefix(statements []string, prefix string) []string {
	trimmed := make([]string, 0, len(statements))
	for _, statement := range statements {
		trimmed = append(trimmed, strings.TrimSpace(statement))
	}
	return slices.DeleteFunc(trimmed, func(statement string) bool {
		return !strings.HasPrefix(statement, prefix)
	})
}
