package schemaload_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/schemaload"
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

// TestLoad_SQLSchemaFileDoesNotFoldOntoACasePreservingTable is the direction
// the fold must not run. The SQL reader keeps `CREATE TABLE "ORDERS"` as
// `ORDERS` and discards the quoting, so a declaration spelled `ORDERS` is
// indistinguishable from one written `"ORDERS"` -- and to PostgreSQL those are
// two different relations, only one of which a reference written `orders`
// reaches. Folding the declaration up to meet the reference therefore has to be
// wrong for one of two inputs Ptah cannot tell apart, and being wrong for the
// quoted one moves an access-control declaration onto a relation the author did
// not name.
//
// Measured on PostgreSQL 17.10 with `-v ON_ERROR_STOP=1`, this exact file exits
// 3 with `relation "orders" does not exist`. The render must say the same thing:
// every statement keeps `orders`, so replaying it reproduces the database's own
// answer instead of quietly succeeding against `ORDERS`.
func TestLoad_SQLSchemaFileDoesNotFoldOntoACasePreservingTable(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE "ORDERS" (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON orders FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "orders")
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

// TestLoad_SQLSchemaFileFoldsOntoALowerCaseTableDeclaredUnquoted is the other
// half of the same boundary, and the reason the fold exists at all. The
// declaration here is already its own folded form, so a reference written
// `ORDERS` reaches it exactly as PostgreSQL says it does, and the render names
// the declared table. The two tests differ only in the case of the CREATE
// TABLE, which is what makes the rule one-directional rather than absent.
func TestLoad_SQLSchemaFileFoldsOntoALowerCaseTableDeclaredUnquoted(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE ORDERS ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON ORDERS FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "orders")
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

// TestLoad_SQLSchemaFileKeepsAQuotedReferenceOffAnUnquotedTable is the
// direction the case fold must not run, seen from the side the fold could not
// tell apart.
//
// `CREATE POLICY p ON "ORDERS"` names the relation `ORDERS`. A table declared
// `orders` is a different relation, and PostgreSQL says so: measured on
// PostgreSQL 17.10 with `-v ON_ERROR_STOP=1`, this file exits 1 with
// `relation "ORDERS" does not exist`. Folding the reference down to the
// declaration instead rendered `CREATE POLICY "p" ON "orders"`, which the same
// server accepts with exit 0 and a pg_policy row on `public.orders` -- an
// access-control declaration moved onto a relation the author did not name
// (stokaro/ptah#1311).
//
// The reference therefore keeps `ORDERS` and the render reproduces the
// database's own answer.
func TestLoad_SQLSchemaFileKeepsAQuotedReferenceOffAnUnquotedTable(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON "ORDERS" FOR ALL TO PUBLIC USING (tenant_id = 1);
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
}

// TestLoad_SQLSchemaFileFoldsOnlyTheUnquotedHalfOfAQualifiedReference is the
// half the whole-string fold could not reach.
//
// `"App".ORDERS` is two components with two different answers: the quoted
// schema keeps its case, the unquoted table loses its own. PostgreSQL 17.10
// resolves it to `App.orders` and accepts the policy with exit 0 against a
// table created as `"App".orders`. Folding the complete identity instead asked
// whether `App.ORDERS` folds to itself, which it does not because of the
// schema, so the reference kept `App.ORDERS` and the render was answered with
// `relation "App.ORDERS" does not exist` (exit 1, measured).
func TestLoad_SQLSchemaFileFoldsOnlyTheUnquotedHalfOfAQualifiedReference(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE SCHEMA "App";
CREATE TABLE "App".orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE "App".ORDERS ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON "App".ORDERS FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "App.orders")
	c.Assert(database.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables[0].Table, qt.Equals, "App.orders")

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(createPolicyStatements(statements), qt.DeepEquals, []string{
		"CREATE POLICY \"p\" ON \"App\".\"orders\" FOR ALL TO PUBLIC\n    USING (tenant_id = 1)\n;",
	})
	c.Assert(rowLevelSecurityStatements(statements), qt.DeepEquals, []string{
		`ALTER TABLE "App"."orders" ENABLE ROW LEVEL SECURITY;`,
	})
}

// TestLoad_SQLSchemaFileFoldsASCIIOnly pins which fold the reference resolver
// uses, because the two candidates agree on every fixture that came before
// this one.
//
// `catalogPostgresIdentifierPart` folds an unquoted component with
// `identifier.ComparisonASCIIInsensitive`. Substituting
// `identifier.ComparisonUnicodeInsensitive` -- `strings.ToLower`, which does
// fold `Ä` to `ä` -- passed the entire suite. The two differ only outside
// ASCII, and until this fixture nothing outside ASCII was ever loaded.
//
// PostgreSQL folds ASCII only. Measured on PostgreSQL 17.10 with
// server_encoding UTF8, against a database holding both `Ä` and `ä`:
// `CREATE POLICY p ON Ä` exits 0 with its pg_policy row on `public.Ä`, not on
// `public.ä`. Both relations exist in this fixture for the same reason they did
// in that measurement: under a Unicode fold the reference resolves to the OTHER
// declared table rather than failing, so the declaration is silently relocated
// and nothing reports an error -- the shape of stokaro/ptah#1311.
//
// This row stops at the loaded schema and does not assert the render. The
// PostgreSQL renderer widens identifier bytes to runes when it splits a
// qualified identifier (`core/renderer/internal/dialects/postgres/postgres.go`,
// `splitQualifiedIdentifier`), so `Ä` is emitted as two mojibake characters;
// `sqlite` and `mssql` carry the same line. That is a separate defect, and
// asserting the render here would write it into a baseline instead of leaving
// it to be fixed.
func TestLoad_SQLSchemaFileFoldsASCIIOnly(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE "Ä" (id INTEGER PRIMARY KEY, tenant_id INTEGER);
CREATE TABLE "ä" (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE Ä ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON Ä FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)

	c.Assert(tableNames(database.Tables), qt.DeepEquals, []string{"Ä", "ä"})
	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "Ä")
	c.Assert(database.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(database.RLSEnabledTables[0].Table, qt.Equals, "Ä")
}

// tableNames projects the loaded tables onto their declared names, so a
// fixture can say which relations exist without asserting the whole schema.
func tableNames(tables []goschema.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
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
