//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// policyBindingFixture is a schema holding orders(tenant_id, owner_id) and
// invoices(tenant_id), a second schema holding the schema-bound functions fn
// and fn2, and the security policy tenancy there, filtering both tables
// through fn.
type policyBindingFixture struct {
	conn        *dbschema.DatabaseConnection
	schema, rls string
}

func newPolicyBindingFixture(c *qt.C) policyBindingFixture {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.SQLServer))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	fixture := policyBindingFixture{conn: conn, schema: fmt.Sprintf("ptah_bind_%d", suffix), rls: fmt.Sprintf("ptah_bind_rls_%d", suffix)}
	s, r := quoteSQLServerIdentifier(fixture.schema), quoteSQLServerIdentifier(fixture.rls)
	c.Cleanup(func() {
		ctx := context.Background()
		for _, statement := range []string{
			"DROP SECURITY POLICY IF EXISTS " + r + ".[tenancy]",
			"DROP TABLE IF EXISTS " + s + ".[orders]", "DROP TABLE IF EXISTS " + s + ".[invoices]",
			"DROP FUNCTION IF EXISTS " + r + ".[fn]", "DROP FUNCTION IF EXISTS " + r + ".[fn2]",
			"DROP SCHEMA IF EXISTS " + s, "DROP SCHEMA IF EXISTS " + r,
		} {
			_, _ = conn.ExecContext(ctx, statement)
		}
		dbschema.CloseAndWarn(conn)
	})
	for _, statement := range []string{
		"EXEC('CREATE SCHEMA " + s + "')", "EXEC('CREATE SCHEMA " + r + "')",
		"CREATE TABLE " + s + ".[orders] (tenant_id int NULL, owner_id int NULL)",
		"CREATE TABLE " + s + ".[invoices] (tenant_id int NULL)",
		"EXEC('CREATE FUNCTION " + r + ".[fn](@t int) RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS ok WHERE @t IS NOT NULL OR @t IS NULL')",
		"EXEC('CREATE FUNCTION " + r + ".[fn2](@t int) RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS ok WHERE @t IS NOT NULL OR @t IS NULL')",
		"CREATE SECURITY POLICY " + r + ".[tenancy] ADD FILTER PREDICATE " + r + ".[fn](tenant_id) ON " + s + ".[orders], " +
			"ADD FILTER PREDICATE " + r + ".[fn](tenant_id) ON " + s + ".[invoices] WITH (STATE = ON, SCHEMABINDING = ON)",
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	return fixture
}

// predicate filters table through function, its argument spelled as given.
func (f policyBindingFixture) predicate(function, table, argument string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: f.rls, Name: function},
		Arguments: []string{argument}, Table: mssqlschema.ObjectName{Schema: f.schema, Name: table}}
}

// observed is the policy as the reader reports it: the catalog's bracketed
// arguments, enabled and schema bound.
func (f policyBindingFixture) observed() *mssqlschema.ObservedSecurityPolicy {
	return &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{
		f.predicate("fn", "orders", "[tenant_id]"), f.predicate("fn", "invoices", "[tenant_id]"),
	}, Enabled: true, SchemaBinding: true}
}

// read reads the schemas named and adds the policy as the reader reports it.
// The policy lives in the schema of its functions, which the read leaves out:
// reading it would report the functions too, and a declaration that does not
// manage them would drop them.
func (f policyBindingFixture) read(c *qt.C, schemas ...string) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), f.conn, schemas)
	c.Assert(err, qt.IsNil)
	live.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(mssqlschema.ObservedSecurityPolicyObject(
		mssqlschema.SecurityPolicyRef(f.rls, "tenancy"), *f.observed()))))
	live.FeatureCoverage = must.Must(mssqlschema.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return live
}

// desired declares the tables named, with the columns the fixture gives
// them, and the policy with the predicates given.
func (f policyBindingFixture) desired(tables []string, dropColumn string, predicates ...mssqlschema.Predicate) *schemamodel.Database {
	columns := map[string][]string{"orders": {"tenant_id", "owner_id"}, "invoices": {"tenant_id"}}
	db := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(mssqlschema.DesiredSecurityPolicyObject(
			mssqlschema.SecurityPolicyRef(f.rls, "tenancy"), mssqlschema.DesiredSecurityPolicy{Predicates: predicates})))),
		FeatureCoverage: must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	for _, table := range tables {
		db.Tables = append(db.Tables, schemamodel.Table{StructName: table, Name: table, Schema: f.schema})
		for _, column := range slices.DeleteFunc(slices.Clone(columns[table]), func(name string) bool { return name == dropColumn }) {
			db.Fields = append(db.Fields, schemamodel.Field{StructName: table, FieldName: column, Name: column, Type: "INT", Nullable: true})
		}
	}
	return db
}

// apply plans a diff on SQL Server and runs each statement as its own batch,
// as the migrator does.
func (f policyBindingFixture) apply(c *qt.C, diff *difftypes.SchemaDiff) []string {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "sqlserver")
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := f.conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	return statements
}

// boundTables lists the tables the policy binds, as the catalog reports them.
func (f policyBindingFixture) boundTables(c *qt.C) []string {
	c.Helper()
	rows, err := f.conn.QueryContext(context.Background(), `SELECT OBJECT_NAME(sp.target_object_id) FROM sys.security_predicates sp
		JOIN sys.security_policies p ON p.object_id = sp.object_id WHERE p.name = 'tenancy' AND SCHEMA_NAME(p.schema_id) = @p1
		ORDER BY 1`, f.rls)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var tables []string
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// TestSQLServerLivePolicyBindings_DropsATableTheDeclarationReleases pins the
// supported path end to end: the declaration stops binding invoices and drops
// it, the comparison plans the policy change beside the drop, and the plan
// releases the binding before DROP TABLE, which SQL Server refuses while a
// policy binds the table.
func TestSQLServerLivePolicyBindings_DropsATableTheDeclarationReleases(t *testing.T) {
	c := qt.New(t)
	f := newPolicyBindingFixture(c)
	desired := f.desired([]string{"orders"}, "", f.predicate("fn", "orders", "tenant_id"))

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, f.read(c, f.schema),
		catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	f.apply(c, diff)

	c.Assert(sqlServerLiveTableExists(t, f.conn, f.schema, "invoices"), qt.IsFalse)
	c.Assert(f.boundTables(c), qt.DeepEquals, []string{"orders"})
}

// TestSQLServerLivePolicyBindings_DropsAFunctionThePolicyReleases pins the
// order for functions: a plan that moves the policy to fn2 and drops fn
// releases fn first, since SQL Server refuses DROP FUNCTION while a
// schema-bound policy calls it.
func TestSQLServerLivePolicyBindings_DropsAFunctionThePolicyReleases(t *testing.T) {
	c := qt.New(t)
	f := newPolicyBindingFixture(c)
	after := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		f.predicate("fn2", "orders", "tenant_id"), f.predicate("fn2", "invoices", "tenant_id")}}
	diff := &difftypes.SchemaDiff{
		FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: f.rls + ".fn"}}},
		FeatureChanges: []schemaext.ChangeRecord{{Subject: mssqlschema.SecurityPolicyRef(f.rls, "tenancy"), Value: &mssqldiff.SecurityPolicy{
			Before: f.observed(), After: after, Access: mssqldiff.Assess(identifier.ForDialect("sqlserver"), f.observed(), after)}}},
	}

	f.apply(c, diff)

	var functions int
	c.Assert(f.conn.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sys.objects WHERE name = 'fn' AND SCHEMA_NAME(schema_id) = @p1", f.rls).Scan(&functions), qt.IsNil)
	c.Assert(functions, qt.Equals, 0)
	c.Assert(f.boundTables(c), qt.DeepEquals, []string{"invoices", "orders"})
}

// TestSQLServerLivePolicyBindings_RefusesToDropWhatThePolicyBinds pins each
// refusal against the server's own answer. Each row drops an object the
// policy binds: SQL Server refuses the statement, and the comparison refuses
// the plan that would carry it, while the declaration keeps the policy as it
// is.
func TestSQLServerLivePolicyBindings_RefusesToDropWhatThePolicyBinds(t *testing.T) {
	tests := []struct {
		name       string
		statement  func(policyBindingFixture) string
		serverErr  string
		tables     []string
		dropColumn string
		schemas    func(policyBindingFixture) []string
		wantErr    string
	}{
		{name: "a table", tables: []string{"orders"}, schemas: tableSchema, serverErr: "because it is being referenced by object 'tenancy'",
			statement: func(f policyBindingFixture) string {
				return "DROP TABLE " + quoteSQLServerIdentifier(f.schema) + ".[invoices]"
			},
			wantErr: `(?s).*security-policy .*tenancy binds table .*invoices, which the plan drops.*`},
		{name: "a column", tables: []string{"orders", "invoices"}, dropColumn: "tenant_id", schemas: tableSchema, serverErr: "is dependent on column 'tenant_id'",
			statement: func(f policyBindingFixture) string {
				return "ALTER TABLE " + quoteSQLServerIdentifier(f.schema) + ".[orders] DROP COLUMN tenant_id"
			},
			wantErr: `(?s).*security-policy .*tenancy binds column .*tenant_id, which the plan drops.*`},
		{name: "a function", tables: []string{"orders", "invoices"}, schemas: bothSchemas, serverErr: "because it is being referenced by object 'tenancy'",
			statement: func(f policyBindingFixture) string {
				return "DROP FUNCTION " + quoteSQLServerIdentifier(f.rls) + ".[fn]"
			},
			wantErr: `(?s).*security-policy .*tenancy binds function .*\.fn, which the plan drops.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPolicyBindingFixture(c)
			desired := f.desired(test.tables, test.dropColumn, f.predicate("fn", "orders", "tenant_id"), f.predicate("fn", "invoices", "tenant_id"))

			_, serverErr := f.conn.ExecContext(t.Context(), test.statement(f))
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, f.read(c, test.schemas(f)...),
				catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}, nil, must.Must(builtin.New()))

			c.Assert(serverErr, qt.ErrorMatches, `(?s).*`+test.serverErr+`.*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// tableSchema reads the tables alone, and bothSchemas the functions too.
func tableSchema(f policyBindingFixture) []string { return []string{f.schema} }
func bothSchemas(f policyBindingFixture) []string { return []string{f.schema, f.rls} }
