package pgdefaultprivilegesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

const alterDefault = "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app "

func readSQL(c *qt.C, text string) schemamodel.Database {
	c.Helper()
	db, _, err := sqlschema.ReadWith(c.Context(), []byte(text), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	return db
}

func TestSQL_OperationsKeepTheirMeaning(t *testing.T) {
	tests := []struct {
		name, text string
		want       pgdefaultprivilege.Operation
	}{
		{name: "plain grant preserves order", text: alterDefault + "GRANT SELECT, INSERT, UPDATE ON TABLES TO reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT", "INSERT", "UPDATE"}}},
		{name: "global table grant", text: "ALTER DEFAULT PRIVILEGES FOR ROLE owner GRANT SELECT ON TABLES TO reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT"}}},
		{name: "scoped sequence grant", text: alterDefault + "GRANT USAGE ON SEQUENCES TO reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"USAGE"}}},
		{name: "scoped function grant", text: alterDefault + "GRANT EXECUTE ON FUNCTIONS TO reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"EXECUTE"}}},
		{name: "types to public", text: alterDefault + "GRANT USAGE ON TYPES TO PUBLIC;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TYPES", Grantee: "PUBLIC"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"USAGE"}}},
		{name: "lowercase keywords", text: "alter default privileges for role owner in schema app grant select on tables to reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT"}}},
		{name: "scoped revoke", text: alterDefault + "REVOKE SELECT ON TABLES FROM reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, Action: pgdefaultprivilege.RevokeRights, Privileges: []string{"SELECT"}}},
		{name: "multiple revokes from public", text: alterDefault + "REVOKE SELECT, UPDATE ON SEQUENCES FROM PUBLIC;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "PUBLIC"}, Action: pgdefaultprivilege.RevokeRights, Privileges: []string{"SELECT", "UPDATE"}}},
		{name: "grant with options", text: alterDefault + "GRANT SELECT, INSERT ON TABLES TO reader WITH GRANT OPTION;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT", "INSERT"}, WithOption: true}},
		{name: "option revoke", text: alterDefault + "REVOKE GRANT OPTION FOR SELECT ON TABLES FROM reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, Action: pgdefaultprivilege.RevokeGrantOptions, Privileges: []string{"SELECT"}}},
		{name: "global public", text: "ALTER DEFAULT PRIVILEGES FOR USER Owner REVOKE EXECUTE ON FUNCTIONS FROM public;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}, Action: pgdefaultprivilege.RevokeRights, Privileges: []string{"EXECUTE"}}},
		{name: "large objects", text: "ALTER DEFAULT PRIVILEGES FOR ROLE owner GRANT SELECT ON LARGE OBJECTS TO reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "LARGE OBJECTS", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT"}}},
		{name: "global schemas", text: "ALTER DEFAULT PRIVILEGES FOR ROLE owner GRANT USAGE ON SCHEMAS TO reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "SCHEMAS", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"USAGE"}}},
		{name: "all privileges", text: alterDefault + "GRANT ALL PRIVILEGES ON SEQUENCES TO reader;", want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"ALL"}}},
		{name: "identifier whitespace", text: `ALTER DEFAULT PRIVILEGES FOR ROLE " owner " IN SCHEMA " app " GRANT SELECT ON TABLES TO " reader ";`, want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: " owner ", Schema: " app ", ObjectType: "TABLES", Grantee: " reader "}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT"}}},
		{name: "literal quote delimiters", text: `ALTER DEFAULT PRIVILEGES FOR ROLE """owner""" GRANT SELECT ON TABLES TO """reader""";`, want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: `"owner"`, ObjectType: "TABLES", Grantee: `"reader"`}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT"}}},
		{name: "quoted names", text: `ALTER DEFAULT PRIVILEGES FOR ROLE "App Owner" IN SCHEMA "App" GRANT USAGE ON TYPES TO "App ""Reader";`, want: pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "App Owner", Schema: "App", ObjectType: "TYPES", Grantee: "App \"Reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"USAGE"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.text, parser.WithDialect("postgres"), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			statement, ok := statements.Statements[0].(*ast.ExtensionStatement)
			c.Assert(ok, qt.IsTrue)
			c.Assert(statement.Payload, qt.DeepEquals, &test.want)
			rendered := must.Must(builtin.RenderSQL("postgres", statement))
			again := must.Must(parser.NewParser(rendered, parser.WithDialect("postgres"), parser.WithSQL(builtintest.Runtime().SQL())).Parse())
			c.Assert(again.Statements[0].(*ast.ExtensionStatement).Payload, qt.DeepEquals, statement.Payload)
		})
	}
}

func TestSQL_OwnerCoverageAndCommonObjectsSurvive(t *testing.T) {
	c := qt.New(t)
	db := readSQL(c, "CREATE TABLE app.docs (id integer);\n"+alterDefault+"GRANT SELECT ON TABLES TO reader;")
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Fields, qt.HasLen, 1)
	objects := must.Must(db.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(db.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, objects[0].Ref).State, qt.Equals, schemaext.Complete)
	c.Assert(must.Must(pgdefaultprivilege.IdentityOf(objects[0].Ref)), qt.DeepEquals, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"})
}
