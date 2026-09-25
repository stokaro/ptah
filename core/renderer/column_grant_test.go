package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// TestRenderSQL_ColumnPrivilege_HappyPath pins the PostgreSQL spelling of a
// privilege limited to columns: the list follows each privilege.
func TestRenderSQL_ColumnPrivilege_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a grant on two columns",
			node: ast.NewGrantPrivilege("wpmgr_app", "TABLE", "proposals", []string{"UPDATE"}).
				SetColumns([]string{"state", "decided_at"}),
			want: `GRANT UPDATE ("state", "decided_at") ON TABLE "proposals" TO "wpmgr_app";` + "\n",
		},
		{
			name: "two privileges sharing a column, revoked",
			node: ast.NewRevokePrivilege("app", "TABLE", "t", []string{"SELECT", "UPDATE"}).SetColumns([]string{"Label"}),
			want: `REVOKE SELECT ("Label"), UPDATE ("Label") ON TABLE "t" FROM "app";` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.Postgres, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_ColumnPrivilege_FailurePath pins that a renderer with no column
// grant spelling refuses the node by name. Dropping the list would grant on
// the whole table.
func TestRenderSQL_ColumnPrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "MySQL",
			dialect: platform.MySQL,
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"UPDATE"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*GRANT \(a\) ON t: column privileges are modeled for PostgreSQL only, not mysql.*`,
		},
		{
			name:    "SQL Server",
			dialect: platform.SQLServer,
			node:    ast.NewRevokePrivilege("app", "TABLE", "t", []string{"SELECT"}).SetColumns([]string{"a", "b"}),
			wantErr: `(?s).*REVOKE \(a, b\) ON t: column privileges are modeled for PostgreSQL only, not sqlserver.*`,
		},
		{
			name:    "Oracle",
			dialect: platform.Oracle,
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"UPDATE"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*GRANT \(a\) ON t: column privileges are modeled for PostgreSQL only, not oracle.*`,
		},
		{
			name:    "ClickHouse",
			dialect: platform.ClickHouse,
			node:    ast.NewGrantPrivilege("app", "TABLE", "db.t", []string{"SELECT"}).SetColumns([]string{"a"}),
			wantErr: `(?s).*GRANT \(a\) ON db.t: column privileges are modeled for PostgreSQL only, not clickhouse.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(test.dialect, test.node)

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

	statements, err := renderer.GetOrderedCreateStatements(db, platform.Postgres)

	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	revoke := strings.Index(rendered, `REVOKE UPDATE ON TABLE "proposals"`)
	grant := strings.Index(rendered, `GRANT UPDATE ("state") ON TABLE "proposals"`)
	c.Assert(revoke >= 0, qt.IsTrue, qt.Commentf("%s", rendered))
	c.Assert(grant > revoke, qt.IsTrue, qt.Commentf("%s", rendered))
}
