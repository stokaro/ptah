package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/oratype"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// These tests build the Oracle type owner's models themselves, rather than read
// them from a source, and drive them through the bundled runtime the commands
// use.

// oracleTypesWithTable declares two domains and an object type beside a table
// whose column one domain types.
func oracleTypesWithTable() *schemamodel.Database {
	object := func(kind schemaext.Kind, name string, value schemaext.Value) schemaext.Object {
		return must.Must(oratype.DesiredObject(oratype.Ref(kind, "", name), value))
	}
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "email_d"},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(
			object(oratype.DomainKind, "score_d", &oratype.DesiredDomain{BaseType: "NUMBER(5,2)", Check: "VALUE BETWEEN 0 AND 100"}),
			object(oratype.DomainKind, "email_d", &oratype.DesiredDomain{BaseType: "VARCHAR2(255)", NotNull: true, Comment: "mail"}),
			object(oratype.ObjectTypeKind, "addr_t", &oratype.DesiredObjectType{Attributes: []oratype.Attribute{
				{Name: "street", Type: "VARCHAR2(80)"}, {Name: "zip", Type: "NUMBER(10,2)"},
			}}),
		)),
		FeatureCoverage: must.Must(oratype.CompleteCoverage(schemaext.Desired)),
	}
	schemamodel.Finalize(database)
	return database
}

// TestOracleRendersDeclaredTypes pins the order and the spelling of a
// whole-schema render: the domains by name, then the object types, all before
// the table whose column names one.
func TestOracleRendersDeclaredTypes(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(oracleTypesWithTable(), platform.Oracle)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 4)
	c.Assert(statements[0], qt.Equals, "-- mail\nCREATE DOMAIN IF NOT EXISTS email_d AS VARCHAR2(255) NOT NULL;\n")
	c.Assert(statements[1], qt.Equals, "CREATE DOMAIN IF NOT EXISTS score_d AS NUMBER(5,2) CHECK (VALUE BETWEEN 0 AND 100);\n")
	c.Assert(statements[2], qt.Equals, "CREATE OR REPLACE TYPE addr_t AS OBJECT (street VARCHAR2(80), zip NUMBER(10,2));\n")
	c.Assert(statements[3], qt.Contains, "CREATE TABLE users")
}

// TestOracleTypeRefusals pins where a declared Oracle type cannot go: Oracle 21
// has no CREATE DOMAIN, and refuses the domain with the scope advice of
// stokaro/ptah#1717, and a target other than Oracle refuses every type by the
// owner's off-target policy.
func TestOracleTypeRefusals(t *testing.T) {
	tests := []struct {
		name   string
		target string
		caps   capability.Capabilities
		want   string
	}{
		{name: "Oracle 21", target: platform.Oracle, caps: capability.Oracle21(),
			want: `(?s).*oracle: CREATE DOMAIN email_d: this target does not create it, and a column declared with the type would be ` +
				`left naming something the server has no definition of; scope the declaration to the dialects that host it.*`},
		{name: "PostgreSQL", target: platform.Postgres, caps: capability.Postgres18(), want: offTarget("postgres")},
		{name: "MySQL", target: platform.MySQL, caps: capability.MySQL84(), want: offTarget("mysql")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(oracleTypesWithTable(), test.target, test.caps)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestOraclePlansTypesAroundTheTables pins where a migration plan puts the
// owner's statements among the common ones: the types are created before the
// table whose column names one, and dropped after it, since Oracle refuses to
// drop a type a column still uses.
func TestOraclePlansTypesAroundTheTables(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	info := catalog.ServerInfo{Dialect: platform.Oracle, Capabilities: capability.Oracle23()}
	empty := &catalog.Database{FeatureCoverage: must.Must(oratype.CompleteCoverage(schemaext.Observed))}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), oracleTypesWithTable(), empty, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.Oracle,
		planext.Options{Capabilities: info.Capabilities})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 4)
	c.Assert(statements[0], qt.Equals, "-- mail\nCREATE DOMAIN IF NOT EXISTS email_d AS VARCHAR2(255) NOT NULL")
	c.Assert(statements[1], qt.Equals, "CREATE DOMAIN IF NOT EXISTS score_d AS NUMBER(5,2) CHECK (VALUE BETWEEN 0 AND 100)")
	c.Assert(statements[2], qt.Equals, "CREATE OR REPLACE TYPE addr_t AS OBJECT (street VARCHAR2(80), zip NUMBER(10,2))")
	c.Assert(statements[3], qt.Contains, "CREATE TABLE users")

	held := &catalog.Database{
		Tables: []catalog.Table{{Name: "USERS", Columns: []catalog.Column{{Name: "ID", DataType: "NUMBER", IsNullable: "NO", IsPrimaryKey: true}}}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			must.Must(oratype.ObservedObject(oratype.Ref(oratype.DomainKind, "", "EMAIL_D"), &oratype.ObservedDomain{BaseType: "VARCHAR2(255)"})),
			must.Must(oratype.ObservedObject(oratype.Ref(oratype.ObjectTypeKind, "", "ADDR_T"),
				&oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "STREET", Type: "VARCHAR2(80)"}}})),
		)),
		FeatureCoverage: must.Must(oratype.CompleteCoverage(schemaext.Observed)),
	}
	desired := &schemamodel.Database{FeatureCoverage: must.Must(oratype.CompleteCoverage(schemaext.Desired))}

	teardown, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, held, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, teardown, platform.Oracle,
		planext.Options{Capabilities: info.Capabilities})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 3)
	c.Assert(statements[0], qt.Contains, "DROP TABLE IF EXISTS USERS PURGE")
	c.Assert(statements[1], qt.Equals, "DROP DOMAIN EMAIL_D")
	c.Assert(statements[2], qt.Equals, "DROP TYPE ADDR_T")
}

// offTarget is the owner's refusal of the first declared type on target.
func offTarget(target string) string {
	return `Oracle domain email_d cannot be planned on ` + target + `; scope its declaration to the targets that host it, ` +
		`as dialects="oracle" does in a Go annotation`
}

// TestOracleTypeFixturesRenderOnOracle pins each operation's statement on the
// Oracle target, the one that owns them.
func TestOracleTypeFixturesRenderOnOracle(t *testing.T) {
	for _, fixture := range []extensionFixture{oraTypeCreateDomainFixture(), oraTypeReplaceObjectTypeFixture(), oraTypeDropFixture()} {
		t.Run(string(fixture.payload.Kind()), func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(platform.Oracle, &ast.ExtensionStatement{Payload: fixture.payload})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, fixture.wantSQL)
		})
	}
}
