package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// TestRenderSQL_ColumnPrivilege_HappyPath pins the spelling of a privilege
// limited to columns on PostgreSQL, MySQL and MariaDB: the list follows each
// privilege. The grant option is the grantee's at the table on MySQL and
// MariaDB, so revoking it names no column.
func TestRenderSQL_ColumnPrivilege_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    string
	}{
		{
			name:    "a grant on two columns",
			dialect: platform.Postgres,
			node: ast.NewGrantPrivilege("wpmgr_app", "TABLE", "proposals", []string{"UPDATE"}).
				SetColumns([]string{"state", "decided_at"}),
			want: `GRANT UPDATE ("state", "decided_at") ON TABLE "proposals" TO "wpmgr_app";` + "\n",
		},
		{
			name:    "two privileges sharing a column, revoked",
			dialect: platform.Postgres,
			node:    ast.NewRevokePrivilege("app", "TABLE", "t", []string{"SELECT", "UPDATE"}).SetColumns([]string{"Label"}),
			want:    `REVOKE SELECT ("Label"), UPDATE ("Label") ON TABLE "t" FROM "app";` + "\n",
		},
		{
			name:    "a MySQL grant on two columns",
			dialect: platform.MySQL,
			node: ast.NewGrantPrivilege("app", "TABLE", "db.t", []string{"SELECT", "UPDATE"}).
				SetColumns([]string{"name", "Label"}),
			want: "GRANT SELECT (`name`, `Label`), UPDATE (`name`, `Label`) ON `db`.`t` TO `app`;\n",
		},
		{
			name:    "a MariaDB grant on a view's column with the grant option",
			dialect: platform.MariaDB,
			node: ast.NewGrantPrivilege("app", "VIEW", "v", []string{"SELECT"}).
				SetColumns([]string{"name"}).SetWithOption(true),
			want: "GRANT SELECT (`name`) ON `v` TO `app` WITH GRANT OPTION;\n",
		},
		{
			name:    "a MySQL column REFERENCES",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"REFERENCES"}).SetColumns([]string{"id"}),
			want:    "GRANT REFERENCES (`id`) ON `t` TO `app`;\n",
		},
		{
			name:    "a MySQL revoke on a column",
			dialect: platform.MySQL,
			node:    ast.NewRevokePrivilege("app", "TABLE", "db.t", []string{"INSERT"}).SetColumns([]string{"secret"}),
			want:    "REVOKE INSERT (`secret`) ON `db`.`t` FROM `app`;\n",
		},
		{
			name:    "a MariaDB grant option revoked from a column grant",
			dialect: platform.MariaDB,
			node: ast.NewRevokePrivilege("app", "TABLE", "db.t", []string{"SELECT"}).
				SetColumns([]string{"name"}).SetGrantOptionFor(true),
			want: "REVOKE GRANT OPTION ON `db`.`t` FROM `app`;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_ColumnPrivilege_FailurePath pins that a renderer refuses by
// name a column list it has no spelling for, or one the engine cannot keep.
// Dropping the list would grant on the whole table.
func TestRenderSQL_ColumnPrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "MySQL, a privilege with no column form",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"DELETE"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*GRANT DELETE \(a\) ON t: mysql grants only SELECT, INSERT, UPDATE and REFERENCES on columns.*`,
		},
		{
			name:    "MariaDB, a revoke of a privilege with no column form",
			dialect: platform.MariaDB,
			node:    ast.NewRevokePrivilege("app", "TABLE", "t", []string{"SELECT", "ALTER"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*REVOKE ALTER \(a\) ON t: mariadb grants only SELECT, INSERT, UPDATE and REFERENCES on columns.*`,
		},
		{
			name:    "MySQL, columns of a database",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "SCHEMA", "db", []string{"SELECT"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*GRANT \(a\) ON SCHEMA db: a column privilege is on a table or a view.*`,
		},
		{
			name:    "SQL Server",
			dialect: platform.SQLServer,
			node:    ast.NewRevokePrivilege("app", "TABLE", "t", []string{"SELECT"}).SetColumns([]string{"a", "b"}),
			wantErr: `(?s).*REVOKE \(a, b\) ON t: column privileges are modeled for PostgreSQL, MySQL and MariaDB only, not sqlserver.*`,
		},
		{
			name:    "Oracle",
			dialect: platform.Oracle,
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"UPDATE"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*GRANT \(a\) ON t: column privileges are modeled for PostgreSQL, MySQL and MariaDB only, not oracle.*`,
		},
		{
			name:    "ClickHouse",
			dialect: platform.ClickHouse,
			node:    ast.NewGrantPrivilege("app", "TABLE", "db.t", []string{"SELECT"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*GRANT \(a\) ON db.t: column privileges are modeled for PostgreSQL, MySQL and MariaDB only, not clickhouse.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestGetOrderedCreateStatements_RevokesComeBeforeGrants pins the order a
// rendered schema replays in. A REVOKE of a table privilege also takes it off
// every column, measured on PostgreSQL 18, so the column GRANT has to come
// after it or the rendered file revokes what it has just granted.
func TestGetOrderedCreateStatements_RevokesComeBeforeGrants(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "P", Name: "proposals"}},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "P", Name: "state", Type: "TEXT"},
		},
		Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "proposals", Columns: []string{"state"}}},
		RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "proposals"}},
	}

	statements, err := builtin.GetOrderedCreateStatements(db, platform.Postgres)

	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	revoke := strings.Index(rendered, `REVOKE UPDATE ON TABLE "proposals"`)
	grant := strings.Index(rendered, `GRANT UPDATE ("state") ON TABLE "proposals"`)
	c.Assert(revoke >= 0, qt.IsTrue, qt.Commentf("%s", rendered))
	c.Assert(grant > revoke, qt.IsTrue, qt.Commentf("%s", rendered))
}
