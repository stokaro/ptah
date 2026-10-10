package goschema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
)

// writeTestFile writes a parseable Go source file into a directory this test
// owns, and returns its path.
//
// It writes into t.TempDir() rather than into the package directory, which is
// what these tests did. A .go file that exists in core/goschema for the length
// of one test is a file every CONCURRENT build of a package that imports
// core/goschema can see -- and `go test ./...` builds packages in parallel, so
// another package's test binary can be compiling this one while the file is
// created and removed. That race produced, on the Windows runner:
//
//	..\..\core\goschema\parser.go:17:2: cannot find package
//	open ...\core\goschema\test_rls_integration.go: The system cannot find the file specified.
//
// in cmd/ptah-compat, whose test shells out to `go build`. The file it names is
// this one, written by this package's tests (stokaro/ptah#1749).
func writeTestFile(c *qt.C, name, content string) string {
	c.Helper()
	path := filepath.Join(c.TB.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(content), 0600), qt.IsNil)
	return path
}

func TestRLSAndFunctionIntegration_EndToEnd(t *testing.T) {
	c := qt.New(t)

	// Create a test Go file content with RLS and function annotations
	testGoContent := `package testpkg

//ptah:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;" comment="Sets the current tenant context for RLS"
//ptah:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;" comment="Gets the current tenant ID from session"
//ptah:schema:rls:enable table="users" comment="Enable RLS for multi-tenant isolation"
//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
//ptah:schema:table name="users" comment="User accounts table"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `

	//ptah:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string ` + "`json:\"tenant_id\" db:\"tenant_id\"`" + `

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string ` + "`json:\"email\" db:\"email\"`" + `

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string ` + "`json:\"name\" db:\"name\"`" + `
}

//ptah:schema:rls:enable table="products" comment="Enable RLS for product isolation"
//ptah:schema:rls:policy name="product_tenant_isolation" table="products" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Ensures products are isolated by tenant"
//ptah:schema:table name="products" comment="Product catalog table"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `

	//ptah:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string ` + "`json:\"tenant_id\" db:\"tenant_id\"`" + `

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string ` + "`json:\"name\" db:\"name\"`" + `

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true" check="price > 0"
	Price float64 ` + "`json:\"price\" db:\"price\"`" + `

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
	UserID int64 ` + "`json:\"user_id\" db:\"user_id\"`" + `
}
`

	// Write the test file
	testFile := writeTestFile(c, "test_rls_integration.go", testGoContent)

	// Parse the file
	database := mustParseFile(c, testFile)

	// Verify functions were parsed correctly
	c.Assert(database.Functions, qt.HasLen, 2)

	setTenantFunc := findFunction(database.Functions, "set_tenant_context")
	c.Assert(setTenantFunc, qt.IsNotNil)
	// Parameters and Returns are canonicalized to lowercase so they line up
	// with what pg_get_function_arguments / pg_get_function_result emit.
	c.Assert(setTenantFunc.Parameters, qt.Equals, "tenant_id_param text")
	c.Assert(setTenantFunc.Returns, qt.Equals, "void")
	c.Assert(setTenantFunc.Language, qt.Equals, "plpgsql")
	c.Assert(setTenantFunc.Security, qt.Equals, "DEFINER")
	c.Assert(setTenantFunc.Comment, qt.Equals, "Sets the current tenant context for RLS")

	getTenantFunc := findFunction(database.Functions, "get_current_tenant_id")
	c.Assert(getTenantFunc, qt.IsNotNil)
	c.Assert(getTenantFunc.Returns, qt.Equals, "text")
	c.Assert(getTenantFunc.Language, qt.Equals, "plpgsql")
	c.Assert(getTenantFunc.Volatility, qt.Equals, "STABLE")
	c.Assert(getTenantFunc.Comment, qt.Equals, "Gets the current tenant ID from session")

	// Verify the row-level security annotations reached the row-security owner.
	c.Assert(ownerSwitches(c, &database), qt.DeepEquals, map[string]pgpolicy.DesiredTableState{
		"users":    {Enabled: true, Comment: "Enable RLS for multi-tenant isolation", StructName: "User"},
		"products": {Enabled: true, Comment: "Enable RLS for product isolation", StructName: "Product"},
	})
	c.Assert(ownerPolicies(c, &database), qt.DeepEquals, map[string]pgpolicy.DesiredPolicy{
		"public.users.user_tenant_isolation": {
			Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Name: "inventario_app"}},
			Using: new("tenant_id = get_current_tenant_id()"), Comment: "Ensures users can only access their tenant's data", StructName: "User",
		},
		"public.products.product_tenant_isolation": {
			Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Name: "inventario_app"}},
			Using: new("tenant_id = get_current_tenant_id()"), WithCheck: new("tenant_id = get_current_tenant_id()"),
			Comment: "Ensures products are isolated by tenant", StructName: "Product",
		},
	})

	// Generate PostgreSQL SQL and verify it contains the expected statements
	statements, err := builtin.GetOrderedCreateStatements(&database, "postgresql")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Not(qt.HasLen), 0)

	sqlOutput := legacyRenderedSQL(strings.Join(statements, "\n"))

	// Verify function creation SQL
	// Types lowercased by Function.Canonicalize to match Postgres' canonical form.
	c.Assert(sqlOutput, qt.Contains, "CREATE FUNCTION set_tenant_context(tenant_id_param text)")
	c.Assert(sqlOutput, qt.Contains, "RETURNS void")
	c.Assert(sqlOutput, qt.Contains, "LANGUAGE plpgsql SECURITY DEFINER")
	c.Assert(sqlOutput, qt.Contains, "PERFORM set_config('app.current_tenant_id', tenant_id_param, false)")

	c.Assert(sqlOutput, qt.Contains, "CREATE FUNCTION get_current_tenant_id()")
	// The annotation omits security=, so the parser canonicalizes it to
	// INVOKER (PostgreSQL's default). Emitting it explicitly is harmless and
	// makes a later DEFINER → INVOKER switch work via CREATE OR REPLACE.
	c.Assert(sqlOutput, qt.Contains, "LANGUAGE plpgsql SECURITY INVOKER STABLE")
	c.Assert(sqlOutput, qt.Contains, "current_setting('app.current_tenant_id', true)")

	// Verify RLS enablement SQL
	c.Assert(sqlOutput, qt.Contains, "ALTER TABLE users ENABLE ROW LEVEL SECURITY")
	c.Assert(sqlOutput, qt.Contains, "ALTER TABLE products ENABLE ROW LEVEL SECURITY")

	// Verify RLS policy creation SQL
	c.Assert(sqlOutput, qt.Contains, "CREATE POLICY user_tenant_isolation ON users")
	c.Assert(sqlOutput, qt.Contains, "FOR ALL TO inventario_app")
	c.Assert(sqlOutput, qt.Contains, "USING (tenant_id = get_current_tenant_id())")

	c.Assert(sqlOutput, qt.Contains, "CREATE POLICY product_tenant_isolation ON products")
	c.Assert(sqlOutput, qt.Contains, "WITH CHECK (tenant_id = get_current_tenant_id())")

	// Verify table creation SQL is still present
	c.Assert(sqlOutput, qt.Contains, "CREATE TABLE users")
	c.Assert(sqlOutput, qt.Contains, "CREATE TABLE products")
	c.Assert(sqlOutput, qt.Contains, "FOREIGN KEY (user_id) REFERENCES users(id)")
}

// mysqlRowSecuritySource declares a function, a table and the table's
// row-level security, the last two with the given scope attribute.
func mysqlRowSecuritySource(scope string) string {
	return `package testpkg

//ptah:schema:function name="test_func" returns="INTEGER" language="sql"
//ptah:schema:rls:enable table="test_table" ` + scope + `
//ptah:schema:rls:policy name="test_policy" table="test_table" for="ALL" to="app_user" using="user_id = current_user_id()" ` + scope + `
//ptah:schema:table name="test_table"
type TestTable struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}
`
}

// TestRLSAndFunctionIntegration_MySQLLeavesScopedRowSecurityOut pins that a
// MySQL target renders the objects it hosts as real statements and leaves out
// row-level security the declaration scoped to PostgreSQL.
//
// The function is real DDL: row-level security really is a PostgreSQL-only
// surface, but a stored function is not, and MySQL 26.7.0 accepts one. It is
// asserted here as executable DDL so that a `-- CREATE FUNCTION ... not
// supported in MySQL` comment -- a claim about the server that the server
// contradicts -- fails this test.
func TestRLSAndFunctionIntegration_MySQLLeavesScopedRowSecurityOut(t *testing.T) {
	c := qt.New(t)
	testFile := writeTestFile(c, "test_mysql_skip.go", mysqlRowSecuritySource(`dialects="postgres"`))
	database := mustParseFile(c, testFile)

	statements, err := builtin.GetOrderedCreateStatements(&database, "mysql")

	c.Assert(err, qt.IsNil)
	sqlOutput := legacyRenderedSQL(strings.Join(statements, "\n"))
	c.Assert(sqlOutput, qt.Not(qt.Contains), "POLICY")
	c.Assert(sqlOutput, qt.Not(qt.Contains), "ROW LEVEL SECURITY")
	// legacyRenderedSQL strips the backtick quoting, hence the bare name.
	executable := executableSQL(sqlOutput)
	c.Assert(executable, qt.Contains, "CREATE FUNCTION test_func() RETURNS integer")
	c.Assert(sqlOutput, qt.Contains, "CREATE TABLE test_table")
	c.Assert(sqlOutput, qt.Contains, "id INTEGER PRIMARY KEY")
}

// TestRLSAndFunctionIntegration_MySQLRefusesUnscopedRowSecurity_FailurePath
// pins that a MySQL target refuses PostgreSQL row-level security declared
// without a scope, and says how to scope it. A render that left it out would
// report a table secured that the target leaves open.
func TestRLSAndFunctionIntegration_MySQLRefusesUnscopedRowSecurity_FailurePath(t *testing.T) {
	c := qt.New(t)
	testFile := writeTestFile(c, "test_mysql_skip.go", mysqlRowSecuritySource(""))
	database := mustParseFile(c, testFile)

	statements, err := builtin.GetOrderedCreateStatements(&database, "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*PostgreSQL policy "test_policy" on table public.test_table cannot be planned on mysql; `+
		`scope its declaration to the targets that host it, as dialects="postgres,cockroachdb,yugabytedb" does in a Go annotation`)
	c.Assert(statements, qt.IsNil)
}

// Helper functions
func findFunction(functions []schemamodel.Function, name string) *schemamodel.Function {
	for _, f := range functions {
		if f.Name == name {
			return &f
		}
	}
	return nil
}
