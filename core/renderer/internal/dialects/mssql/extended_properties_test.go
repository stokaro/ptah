package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

// TestExtendedProperty_WritesTheAddressLevelByLevel pins the three scopes as
// three different statements.
//
// A renderer that always wrote all three levels would be ACCEPTED by the
// server and would address something else, which is the failure this table
// exists to catch: an empty @level2name is not the same as no level 2.
func TestExtendedProperty_WritesTheAddressLevelByLevel(t *testing.T) {
	tests := []struct {
		name string
		node *ast.ExtendedPropertyNode
		want string
	}{
		{
			// No level at all is the database's own property, and the
			// procedure takes it that way: an empty @level0name would be a
			// property on a schema called "", which it also accepts and which
			// belongs to nothing.
			name: "database scope passes no level",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "ptah_db").
				SetOwner("", "", "").SetValue("on"),
			want: "EXEC sp_addextendedproperty @name = N'ptah_db', @value = N'on';",
		},
		{
			name: "schema scope passes level 0 alone",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "ptah_flag").
				SetOwner("app", "", "").SetValue("on"),
			want: "EXEC sp_addextendedproperty @name = N'ptah_flag', @value = N'on', " +
				"@level0type = N'SCHEMA', @level0name = N'app';",
		},
		{
			name: "table scope adds level 1",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "ptah_flag").
				SetOwner("app", "docs", "").SetValue("on"),
			want: "EXEC sp_addextendedproperty @name = N'ptah_flag', @value = N'on', " +
				"@level0type = N'SCHEMA', @level0name = N'app', " +
				"@level1type = N'TABLE', @level1name = N'docs';",
		},
		{
			name: "column scope adds level 2",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "ptah_flag").
				SetOwner("app", "docs", "title").SetValue("on"),
			want: "EXEC sp_addextendedproperty @name = N'ptah_flag', @value = N'on', " +
				"@level0type = N'SCHEMA', @level0name = N'app', " +
				"@level1type = N'TABLE', @level1name = N'docs', " +
				"@level2type = N'COLUMN', @level2name = N'title';",
		},
		{
			name: "an update names the update procedure",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyUpdate, "ptah_flag").
				SetOwner("app", "docs", "").SetValue("off"),
			want: "EXEC sp_updateextendedproperty @name = N'ptah_flag', @value = N'off', " +
				"@level0type = N'SCHEMA', @level0name = N'app', " +
				"@level1type = N'TABLE', @level1name = N'docs';",
		},
		{
			// sp_dropextendedproperty does not take @value, and passing one
			// answers `Procedure or function sp_dropextendedproperty has too
			// many arguments specified`.
			name: "a drop passes no value",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyDrop, "ptah_flag").
				SetOwner("app", "docs", "").SetValue("ignored"),
			want: "EXEC sp_dropextendedproperty @name = N'ptah_flag', " +
				"@level0type = N'SCHEMA', @level0name = N'app', " +
				"@level1type = N'TABLE', @level1name = N'docs';",
		},
		{
			// Every argument is a string literal, so a quote in a name or a
			// value is doubled rather than bracketed. Bracketing the object
			// name would write a property onto an object literally called
			// `[docs]`.
			name: "quotes are doubled in every literal",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "it's").
				SetOwner("o'brien", "d'oh", "c'est").SetValue("va'lue"),
			want: "EXEC sp_addextendedproperty @name = N'it''s', @value = N'va''lue', " +
				"@level0type = N'SCHEMA', @level0name = N'o''brien', " +
				"@level1type = N'TABLE', @level1name = N'd''oh', " +
				"@level2type = N'COLUMN', @level2name = N'c''est';",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(
				platform.SQLServer, capability.SQLServer2022(), test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, test.want)
		})
	}
}

// TestExtendedProperty_RefusesAnAddressItCannotCompose holds the two shapes
// that have no statement, refused before a plan is reviewed rather than at the
// server.
func TestExtendedProperty_RefusesAnAddressItCannotCompose(t *testing.T) {
	tests := []struct {
		name    string
		node    *ast.ExtendedPropertyNode
		wantErr string
	}{
		{
			name: "a table without a schema",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "ptah_flag").
				SetOwner("", "docs", "").SetValue("on"),
			wantErr: "names table",
		},
		{
			name: "a column without a table",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "ptah_flag").
				SetOwner("app", "", "title").SetValue("on"),
			wantErr: "names column",
		},
		{
			name: "an operation with no procedure",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyOperation("rename"), "ptah_flag").
				SetOwner("app", "docs", "").SetValue("on"),
			wantErr: "unknown operation",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := renderer.RenderSQLWithCapabilities(
				platform.SQLServer, capability.SQLServer2022(), test.node)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.wantErr)
		})
	}
}

// TestExtendedProperty_EveryOtherDialectSkipsRatherThanFails pins the
// asymmetry that must not exist.
//
// schemamodel.ExtendedProperty carries no dialect scope, exactly as
// schemamodel.Synonym does not, so a schema declaring one has to render on every
// target. A dialect that returned an error instead would make one schema
// renderable on five targets and fatal on the sixth.
func TestExtendedProperty_EveryOtherDialectSkipsRatherThanFails(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "clickhouse", dialect: platform.ClickHouse},
		{name: "oracle", dialect: platform.Oracle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "ptah_flag").
				SetOwner("app", "docs", "").SetValue("on")

			out, err := renderer.RenderSQLWithCapabilities(
				test.dialect, capability.ForDialect(test.dialect), node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ptah_flag")
			c.Assert(out, qt.Not(qt.Contains), "sp_addextendedproperty")
		})
	}
}
