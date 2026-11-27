package modelast_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtinlowering"
	"ptah.run/internal/modelast"
	"ptah.run/internal/sqlschema"
)

// optionStatements records the permissions of the render in their execution
// order, including the difference between a privilege and its grant option.
func optionStatements(c *qt.C, database schemamodel.Database, dialect string) []string {
	c.Helper()
	var statements []string
	lowering := builtinlowering.ForTarget(c.Context(), nil, dialect, capability.ForDialect(dialect))
	err := modelast.WalkDatabase(database, dialect, func(node ast.Node) error {
		switch typed := node.(type) {
		case *ast.GrantPrivilegeNode:
			statements = append(statements, fmt.Sprintf("GRANT %s ON %s %s (%s) option=%t",
				strings.Join(typed.Privileges, ","), typed.ObjectType, typed.ObjectName, strings.Join(typed.Columns, ","), typed.WithOption))
		case *ast.RevokePrivilegeNode:
			statements = append(statements, fmt.Sprintf("REVOKE %s ON %s %s (%s) option=%t",
				strings.Join(typed.Privileges, ","), typed.ObjectType, typed.ObjectName, strings.Join(typed.Columns, ","), typed.GrantOptionFor))
		}
		return nil
	}, lowering)
	c.Assert(err, qt.IsNil)
	return statements
}

func TestCollectDatabase_PreservesDeclaredGrantOptionRevokes(t *testing.T) {
	for _, test := range []struct {
		dialect string
		want    []string
	}{
		{dialect: "postgres", want: []string{"GRANT SELECT ON TABLE t () option=false", "REVOKE SELECT ON TABLE t () option=true"}},
		{dialect: "mysql", want: []string{"GRANT SELECT ON TABLE t () option=false", "REVOKE SELECT ON TABLE t () option=true"}},
		{dialect: "mariadb", want: []string{"GRANT SELECT ON TABLE t () option=false", "REVOKE SELECT ON TABLE t () option=true"}},
		{dialect: "sqlserver", want: []string{"REVOKE SELECT ON TABLE t () option=true", "GRANT SELECT ON TABLE t () option=false"}},
		{dialect: "mssql", want: []string{"REVOKE SELECT ON TABLE t () option=true", "GRANT SELECT ON TABLE t () option=false"}},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte("GRANT SELECT ON t TO reader WITH GRANT OPTION;\n"+
				"REVOKE GRANT OPTION FOR SELECT ON t FROM reader;"), test.dialect)
			c.Assert(err, qt.IsNil)

			c.Assert(optionStatements(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// A table revoke removes options on its columns too. The column grant must
// follow the whole-table pair even when the model lists the column first.
func TestCollectDatabase_TableOptionRevokesPrecedeColumnGrants(t *testing.T) {
	for _, test := range []struct {
		dialect string
		want    []string
	}{
		{dialect: "postgres", want: []string{
			"GRANT SELECT ON TABLE t () option=false", "REVOKE SELECT ON TABLE t () option=true",
			"GRANT SELECT ON TABLE t (kept) option=true", "GRANT UPDATE ON TABLE t (taken) option=false",
			"REVOKE UPDATE ON TABLE t (taken) option=true",
		}},
		{dialect: "sqlserver", want: []string{
			"REVOKE SELECT ON TABLE t () option=true", "GRANT SELECT ON TABLE t () option=false",
			"GRANT SELECT ON TABLE t (kept) option=true", "REVOKE UPDATE ON TABLE t (taken) option=true",
			"GRANT UPDATE ON TABLE t (taken) option=false",
		}},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			database := schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", OnTable: "t", Columns: []string{"kept"}, Privileges: []string{"SELECT"}, WithOption: true},
				{Role: "reader", OnTable: "t", Columns: []string{"taken"}, Privileges: []string{"UPDATE"}, GrantOptionRevoked: true},
				{Role: "reader", OnTable: "t", Privileges: []string{"SELECT"}, GrantOptionRevoked: true},
			}}

			c.Assert(optionStatements(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

func TestCollectDatabase_OrdinaryGrantsDoNotRevokeOptions(t *testing.T) {
	for _, dialect := range []string{"postgres", "mysql", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			database := schemamodel.Database{Grants: []schemamodel.Grant{{
				Role: "reader", OnTable: "t", Privileges: []string{"SELECT"},
			}}}

			c.Assert(optionStatements(c, database, dialect), qt.DeepEquals, []string{"GRANT SELECT ON TABLE t () option=false"})
		})
	}
}

// MySQL and MariaDB hold one option per grantee and object, including column
// privileges. A render must finish every grant before taking that option once.
func TestCollectDatabase_MySQLFamilyGroupsObjectGrantOptionRevokes(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range []struct {
			name, document string
			want           []string
		}{
			{
				name: "two privileges on one table",
				document: "GRANT SELECT, INSERT ON t TO reader WITH GRANT OPTION; " +
					"REVOKE GRANT OPTION FOR SELECT, INSERT ON t FROM reader;",
				want: []string{
					"GRANT SELECT ON TABLE t () option=false", "GRANT INSERT ON TABLE t () option=false",
					"REVOKE SELECT,INSERT ON TABLE t () option=true",
				},
			},
			{
				name: "table and column privileges share an option",
				document: "GRANT SELECT ON t TO reader; REVOKE GRANT OPTION FOR SELECT ON t FROM reader; " +
					"GRANT UPDATE (id) ON t TO reader; REVOKE GRANT OPTION FOR UPDATE (id) ON t FROM reader;",
				want: []string{
					"GRANT SELECT ON TABLE t () option=false", "GRANT UPDATE ON TABLE t (id) option=false",
					"REVOKE SELECT,UPDATE ON TABLE t () option=true",
				},
			},
			{
				name: "column-only privileges share a table option",
				document: "GRANT SELECT (id) ON t TO reader; REVOKE GRANT OPTION FOR SELECT (id) ON t FROM reader; " +
					"GRANT UPDATE (name) ON t TO reader; REVOKE GRANT OPTION FOR UPDATE (name) ON t FROM reader;",
				want: []string{
					"GRANT SELECT ON TABLE t (id) option=false", "GRANT UPDATE ON TABLE t (name) option=false",
					"REVOKE SELECT,UPDATE ON TABLE t () option=true",
				},
			},
			{
				name: "a later ordinary grant precedes the revoke",
				document: "GRANT SELECT ON t TO reader; REVOKE GRANT OPTION FOR SELECT ON t FROM reader; " +
					"GRANT INSERT ON t TO reader;",
				want: []string{
					"GRANT SELECT ON TABLE t () option=false", "GRANT INSERT ON TABLE t () option=false",
					"REVOKE SELECT ON TABLE t () option=true",
				},
			},
			{
				name: "another object has its own revoke",
				document: "GRANT SELECT ON t TO reader; REVOKE GRANT OPTION FOR SELECT ON t FROM reader; " +
					"GRANT SELECT ON u TO reader; REVOKE GRANT OPTION FOR SELECT ON u FROM reader;",
				want: []string{
					"GRANT SELECT ON TABLE t () option=false", "GRANT SELECT ON TABLE u () option=false",
					"REVOKE SELECT ON TABLE t () option=true", "REVOKE SELECT ON TABLE u () option=true",
				},
			},
			{
				name: "another grantee has its own revoke",
				document: "GRANT SELECT ON t TO reader; REVOKE GRANT OPTION FOR SELECT ON t FROM reader; " +
					"GRANT SELECT ON t TO other; REVOKE GRANT OPTION FOR SELECT ON t FROM other;",
				want: []string{
					"GRANT SELECT ON TABLE t () option=false", "GRANT SELECT ON TABLE t () option=false",
					"REVOKE SELECT ON TABLE t () option=true", "REVOKE SELECT ON TABLE t () option=true",
				},
			},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				database, _, err := sqlschema.Read([]byte(test.document), dialect)
				c.Assert(err, qt.IsNil)

				c.Assert(optionStatements(c, database, dialect), qt.DeepEquals, test.want)
			})
		}
	}
}
