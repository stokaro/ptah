package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/renderer/internal/dialects/clickhouse"
	"ptah.run/core/renderer/internal/dialects/mariadb"
	"ptah.run/core/renderer/internal/dialects/mssql"
	"ptah.run/core/renderer/internal/dialects/mysql"
	"ptah.run/core/renderer/internal/dialects/oracle"
	"ptah.run/core/renderer/internal/dialects/postgres"
	"ptah.run/core/renderer/internal/dialects/sqlite"
	"ptah.run/internal/yqlparse"
)

func TestYQLPathPrivilegesRenderWithoutChangingTheirTargets(t *testing.T) {
	c := qt.New(t)
	statements, err := yqlparse.Parse("GRANT SELECT ON `shop/a.b` TO readers WITH GRANT OPTION; REVOKE GRANT OPTION FOR SELECT ON `shop/a.b` FROM readers;")
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities("ydb", capability.YDB262(), statements)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "GRANT 'ydb.generic.read', 'ydb.access.grant' ON `shop/a.b` TO `readers`;")
	c.Assert(sql, qt.Contains, "REVOKE 'ydb.generic.read', 'ydb.access.grant' ON `shop/a.b` FROM `readers`;")
}

// Both the public wrapper and direct visitors must refuse an untyped YDB path
// on another engine. Treating its dots as qualification changes its target.
func TestYQLPathPrivilegesRefusedOnOtherDialects(t *testing.T) {
	for _, test := range []struct {
		name   string
		render func(ast.Node) (string, error)
	}{
		{"postgres", postgres.New().Render}, {"mysql", mysql.New().Render},
		{"mariadb", mariadb.New().Render}, {"sqlserver", mssql.New().Render},
		{"oracle", oracle.New().Render}, {"clickhouse", clickhouse.New().Render},
		{"sqlite", sqlite.New().Render},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, node := range []ast.Node{
				ast.NewGrantPrivilege("readers", "PATH", "shop/a.b", []string{"SELECT"}),
				ast.NewRevokePrivilege("readers", "PATH", "shop/a.b", []string{"SELECT"}),
			} {
				c := qt.New(t)
				_, err := test.render(node)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				_, err = renderer.RenderSQL(test.name, node)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			}
		})
	}
}
