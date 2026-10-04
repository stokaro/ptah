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

// topicNodes are the three topic statements, as nodes, with the subject the
// renderer's central check names each by.
var topicNodes = []struct {
	node    ast.Node
	subject string
}{
	{node: ast.NewCreateTopic("app.events", ast.TopicSpec{}), subject: "topic app.events"},
	{node: ast.NewAlterTopic("app.events", ast.TopicSpec{RetentionPeriod: "PT2H"}, ast.TopicSpec{}), subject: "ALTER TOPIC app.events"},
	{node: ast.NewDropTopic("app.events"), subject: "DROP TOPIC app.events"},
}

// TestRender_Topic_FailurePath refuses a topic on every target without
// topics, through the whole-schema render and each topic node alike: built as
// nothing, the declaration would report a topic applied that the target does
// not have.
func TestRender_Topic_FailurePath(t *testing.T) {
	schema := &schemamodel.Database{Topics: []schemamodel.Topic{{Name: "events", Schema: "app"}}}
	tests := []struct {
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
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.Topics, false)},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `topic app\.events, which requires target capability topics, unavailable on this \w+ target`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			for _, node := range topicNodes {
				sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, node.node)
				c.Assert(err, qt.ErrorMatches, node.subject+`, which requires target capability topics, unavailable on this \w+ target`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// A caller that claims the topics key for a target whose renderer writes no
// topic passes the central check, and the renderer refuses the node itself,
// naming itself.
func TestRender_Topic_RenderersWithoutTopicsRefuse(t *testing.T) {
	tests := []struct {
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
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			for _, node := range topicNodes {
				sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps.With(capability.Topics, true), node.node)
				c.Assert(err, qt.ErrorMatches, node.subject+`: the \w+ renderer writes no topic; a topic needs target `+
					`capability topics, which only YDB has`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// The YDB renderer checks the topics key itself, so a caller that visits a
// topic node with it, past the central check, is refused on a target without
// the key, the drop included.
func TestRender_Topic_YDBRendererChecksTheKey(t *testing.T) {
	caps := capability.YDB262().With(capability.Topics, false)
	for _, node := range topicNodes {
		t.Run(node.subject, func(t *testing.T) {
			c := qt.New(t)
			visitor, err := renderer.NewRendererWithCapabilities(platform.YDB, caps)
			c.Assert(err, qt.IsNil)

			visitErr := node.node.Accept(visitor)

			c.Assert(visitErr, qt.ErrorMatches, `topic app\.events, which requires target capability topics, unavailable on this ydb target`)
			c.Assert(visitErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(visitor.Output(), qt.Equals, "")
		})
	}
}

// A schema built by hand, rather than parsed, is held to the parse's rules
// when it renders: a consumer named twice is refused, as YDB refuses it.
func TestRender_Topic_ConsumerNamedTwice(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{Topics: []schemamodel.Topic{{Name: "events", Spec: ast.TopicSpec{
		Consumers: []ast.TopicConsumerSpec{{Name: "c"}, {Name: "c", Important: true}},
	}}}}

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB262())

	c.Assert(err, qt.ErrorMatches, `topic events: two of its consumers are named "c", and YDB names a consumer once per `+
		"topic \\(`Consumer c defined more than once`\\)")
	c.Assert(statements, qt.IsNil)
}

// A topic whose path a declared table holds is refused before anything is
// written: YDB keeps one object at a path, and answers `unexpected path type`
// for the second.
func TestRender_Topic_OnATablePath(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "events", Schema: "app"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		Topics: []schemamodel.Topic{{Name: "events", Schema: "app"}},
	}
	schemamodel.Finalize(schema)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB262())

	c.Assert(err, qt.ErrorMatches, "topic app.events has the path of a declared table, and YDB keeps one object at a path "+
		"\\(`unexpected path type`\\)")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)
}

// A declared topic renders after the tables, with its consumers and the
// settings it names.
func TestRender_Topic_HappyPath(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "notes", Schema: "app"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		Topics: []schemamodel.Topic{{Name: "events", Schema: "app", Spec: ast.TopicSpec{
			RetentionPeriod: "PT2H", Consumers: []ast.TopicConsumerSpec{{Name: "billing", Important: true}},
		}}},
	}
	schemamodel.Finalize(schema)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB251())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE `app/notes` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n",
		"CREATE TOPIC `app/events` (CONSUMER `billing` WITH (important = TRUE)) WITH (retention_period = Interval('PT2H'));\n",
	})
}
