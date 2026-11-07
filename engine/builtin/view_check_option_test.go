package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// A view's check option follows its body. WITH CHECK OPTION is the CASCADED
// level on PostgreSQL, MySQL and MariaDB, and a node that asks for LOCAL is
// written WITH LOCAL CHECK OPTION there.
func TestRenderSQL_ViewCheckOption_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		body    string
		local   bool
		want    string
	}{
		{name: "PostgreSQL", dialect: "postgres", body: "SELECT id FROM t", want: "CREATE VIEW \"v\" AS\nSELECT id FROM t\nWITH CHECK OPTION\n;\n"},
		{name: "PostgreSQL, local", dialect: "postgres", body: "SELECT id FROM t", local: true, want: "CREATE VIEW \"v\" AS\nSELECT id FROM t\nWITH LOCAL CHECK OPTION\n;\n"},
		{name: "PostgreSQL, a body read with its semicolon", dialect: "postgres", body: " SELECT id\n   FROM t;", local: true,
			want: "CREATE VIEW \"v\" AS\nSELECT id\n   FROM t\nWITH LOCAL CHECK OPTION\n;\n"},
		{name: "MySQL, local", dialect: "mysql", body: "SELECT id FROM t", local: true, want: "CREATE VIEW `v` AS\nSELECT id FROM t\nWITH LOCAL CHECK OPTION\n;\n"},
		{name: "MySQL, a body with a semicolon", dialect: "mysql", body: "SELECT id FROM t ;", want: "CREATE VIEW `v` AS\nSELECT id FROM t\nWITH CHECK OPTION\n;\n"},
		{name: "MariaDB, local", dialect: "mariadb", body: "SELECT id FROM t", local: true, want: "CREATE VIEW `v` AS\nSELECT id FROM t\nWITH LOCAL CHECK OPTION\n;\n"},
		{name: "SQL Server", dialect: "sqlserver", body: "SELECT id FROM t", want: "CREATE VIEW [v] AS\nSELECT id FROM t\nWITH CHECK OPTION\n;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, &ast.CreateViewNode{Name: "v", Body: test.body, WithCheck: true, LocalCheck: test.local})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// SQL Server and Oracle take WITH CHECK OPTION alone, so a node that asks for
// LOCAL is refused there rather than written with the stricter check.
func TestRenderSQL_ViewCheckOption_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "SQL Server", dialect: "sqlserver"},
		{name: "Oracle", dialect: "oracle"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, &ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t", WithCheck: true, LocalCheck: true})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*view v asks for WITH LOCAL CHECK OPTION, and this target's check option has no LOCAL level.*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
