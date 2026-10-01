package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// PostgreSQL applies each privilege to every named object and grantee.
func TestParserGrantLists(t *testing.T) {
	tests := []struct {
		name, sql, objectType              string
		objects, arguments, roles, columns []string
		withOption                         bool
	}{
		{name: "schema roles", sql: "GRANT USAGE ON SCHEMA public TO erun_tenant, erun_operations;", objectType: "SCHEMA", objects: []string{"public"}, arguments: []string{""}, roles: []string{"erun_tenant", "erun_operations"}},
		{name: "table product", sql: "GRANT SELECT ON users, tenants TO tenant, operations WITH GRANT OPTION;", objectType: "TABLE", objects: []string{"users", "tenants"}, arguments: []string{"", ""}, roles: []string{"tenant", "operations"}, withOption: true},
		{name: "column product", sql: "GRANT UPDATE (name) ON users, tenants TO tenant, operations;", objectType: "TABLE", objects: []string{"users", "tenants"}, arguments: []string{"", ""}, roles: []string{"tenant", "operations"}, columns: []string{"name"}},
		{name: "routine overloads", sql: "GRANT EXECUTE ON FUNCTION app.f(integer, text), app.f(uuid) TO tenant, operations;", objectType: "FUNCTION", objects: []string{"app.f", "app.f"}, arguments: []string{"integer, text", "uuid"}, roles: []string{"tenant", "operations"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, len(test.objects)*len(test.roles))
			for i, object := range test.objects {
				for j, role := range test.roles {
					grant, ok := statements.Statements[i*len(test.roles)+j].(*ast.GrantPrivilegeNode)
					c.Assert(ok, qt.IsTrue)
					c.Assert(grant.ObjectType, qt.Equals, test.objectType)
					c.Assert(grant.ObjectName, qt.Equals, object)
					c.Assert(grant.Arguments, qt.Equals, test.arguments[i])
					c.Assert(grant.Role, qt.Equals, role)
					c.Assert(grant.Columns, qt.DeepEquals, test.columns)
					c.Assert(grant.WithOption, qt.Equals, test.withOption)
				}
			}
		})
	}
}

// Incomplete lists must not silently grant to only their valid prefix.
func TestParserGrantListsFailurePath(t *testing.T) {
	tests := []struct{ name, sql string }{
		{name: "missing target", sql: "GRANT SELECT ON users, TO tenant;"},
		{name: "missing role", sql: "GRANT SELECT ON users TO tenant,;"},
		{name: "missing routine arguments", sql: "GRANT EXECUTE ON FUNCTION f(uuid), g TO tenant;"},
		{name: "column privilege on schema", sql: "GRANT UPDATE (name) ON SCHEMA public TO tenant, operations;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()
			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
		})
	}
}
