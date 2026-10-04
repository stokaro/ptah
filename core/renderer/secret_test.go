package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// secretNodes are the three secret statements, as nodes, with the subject the
// renderer's central check names each by.
var secretNodes = []struct {
	node    ast.Node
	subject string
}{
	{node: ast.NewCreateSecret("ext.pw", "PTAH_SECRET_PW"), subject: "secret ext.pw"},
	{node: ast.NewAlterSecret("ext.pw", "PTAH_SECRET_PW"), subject: "ALTER SECRET ext.pw"},
	{node: ast.NewDropSecret("ext.pw"), subject: "DROP SECRET ext.pw"},
}

// secretlessTargets are the targets of every engine but YDB, which has the one
// secret Ptah models.
var secretlessTargets = []struct {
	dialect string
	caps    capability.Capabilities
}{
	{dialect: platform.Postgres, caps: capability.Postgres18()},
	{dialect: platform.MySQL, caps: capability.MySQL84()},
	{dialect: platform.MariaDB, caps: capability.MariaDB1011()},
	{dialect: platform.SQLite, caps: capability.SQLite3()},
	{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
	{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
	{dialect: platform.Oracle, caps: capability.Oracle23()},
}

// TestRender_Secret_FailurePath refuses a secret on every target without the
// secrets key, through the whole-schema render and each secret node alike:
// built as nothing, the declaration would report a secret created that the
// target does not hold, and an external data source naming it would fail at
// its first read.
func TestRender_Secret_FailurePath(t *testing.T) {
	schema := &schemamodel.Database{Secrets: []schemamodel.Secret{{Name: "pw", Schema: "ext", ValueEnv: "PTAH_SECRET_PW"}}}
	targets := append(secretlessTargets, struct {
		dialect string
		caps    capability.Capabilities
	}{dialect: platform.YDB, caps: capability.YDB251()})
	for _, test := range targets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `secret ext\.pw, which requires target capability secrets, unavailable on this \w+ target`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			for _, node := range secretNodes {
				sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, node.node)
				c.Assert(err, qt.ErrorMatches, node.subject+`, which requires target capability secrets, unavailable on this \w+ target`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// A caller that claims the secrets key for a target whose renderer writes no
// secret passes the central check, and the renderer refuses the node itself,
// naming itself.
func TestRender_Secret_RenderersWithoutSecretsRefuse(t *testing.T) {
	for _, test := range secretlessTargets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			for _, node := range secretNodes {
				sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps.With(capability.Secrets, true), node.node)
				c.Assert(err, qt.ErrorMatches, node.subject+`: the \w+ renderer writes no secret; a secret needs target `+
					`capability secrets, which only YDB has`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// A secret whose path a declared table holds is refused before anything is
// written: YDB keeps one object at a path, and answers `unexpected path type`
// for the second.
func TestRender_Secret_OnATablePath(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "T", Name: "pw", Schema: "ext"}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		Secrets: []schemamodel.Secret{{Name: "pw", Schema: "ext", ValueEnv: "PTAH_SECRET_PW"}},
	}
	schemamodel.Finalize(schema)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB262())

	c.Assert(err, qt.ErrorMatches, "secret ext.pw has the path of a declared table, and YDB keeps one object at a path "+
		"\\(`unexpected path type`\\)")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)
}

// A declared secret renders ahead of the tables, so an external data source
// that names it finds it, and its value is the reference to its variable.
func TestRender_Secret_HappyPath(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "T", Name: "notes", Schema: "app"}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		Secrets: []schemamodel.Secret{{Name: "pw", Schema: "ext", ValueEnv: "PTAH_SECRET_PW"}},
	}
	schemamodel.Finalize(schema)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB262())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n",
		"CREATE TABLE `app/notes` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n",
	})
}
