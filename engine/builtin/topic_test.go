package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// topicSchema declares one topic beside a table.
func topicSchema(schema, name string, spec ydbtopic.Spec) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "T", Name: "notes", Schema: "app"}},
		Fields:          []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbtopic.DesiredObject(schema, name, "", spec))),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	schemamodel.Finalize(database)
	return database
}

// TestRender_Topic_HappyPath writes a declared topic through its owner ahead
// of every other statement, with its consumers and the settings it names, on
// every whole-schema entry point. A topic depends on nothing but its path.
func TestRender_Topic_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := topicSchema("ext", "events", ydbtopic.Spec{RetentionPeriod: "PT2H",
		Consumers: []ydbtopic.ConsumerSpec{{Name: "billing", Important: true}}})

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, platform.YDB, capability.YDB251())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TOPIC `ext/events` (CONSUMER `billing` WITH (important = TRUE)) WITH (retention_period = Interval('PT2H'));\n",
		"CREATE TABLE `app/notes` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n",
	})
	assertOwnedSchemaEntryPoints(c, database, capability.YDB251(),
		"CREATE TOPIC `ext/events` (CONSUMER `billing` WITH (important = TRUE)) WITH (retention_period = Interval('PT2H'));")
}

// TestRender_Topic_FailurePath refuses a declared topic on every target
// without a selected, enabled owner: built as nothing, the declaration would
// report a topic created that the target does not hold. A topic whose path a
// declared table holds is refused too, since YDB keeps one object at a path,
// and so is a consumer named twice, which YDB refuses.
func TestRender_Topic_FailurePath(t *testing.T) {
	type target struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *schemamodel.Database
		wantErr  string
		wantIs   error
	}
	tests := []target{
		{name: "ydb without topics", dialect: platform.YDB, caps: capability.YDB262().With(capability.Topics, false),
			database: topicSchema("ext", "events", ydbtopic.Spec{}),
			wantErr:  `topic ext/events, which requires target capability topics, unavailable on this ydb target`, wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "a table's path", dialect: platform.YDB, caps: capability.YDB262(),
			database: topicSchema("app", "notes", ydbtopic.Spec{}),
			wantErr:  `topic create conflicts with create at scheme path ptah\.run/ydb/scheme-path app\.notes`, wantIs: ptaherr.ErrInvalidSchemaDiff},
	}
	for _, other := range secretlessTargets {
		tests = append(tests, target{name: other.dialect, dialect: other.dialect, caps: other.caps.With(capability.Topics, true),
			database: topicSchema("ext", "events", ydbtopic.Spec{}),
			wantErr:  `unsupported feature: feature objects are not registered for target "` + other.dialect + `"`, wantIs: ptaherr.ErrUnsupportedFeature})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.database, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRender_Topic_ConsumerNamedTwice refuses a declaration built by hand,
// rather than parsed, that names one consumer twice, as YDB refuses it.
func TestRender_Topic_ConsumerNamedTwice(t *testing.T) {
	c := qt.New(t)
	database := topicSchema("", "events", ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c"}, {Name: "c", Important: true}}})

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, platform.YDB, capability.YDB262())

	c.Assert(err, qt.ErrorMatches, `.*two of its consumers are named "c", and YDB names a consumer once per topic `+
		"\\(`Consumer c defined more than once`\\)")
	c.Assert(statements, qt.IsNil)
}

// TestRender_TopicOperation_HappyPath renders each statement on a topic
// through YDB's owner, an added consumer included: it names only the consumer
// and resets nothing.
func TestRender_TopicOperation_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
		want    string
	}{
		{name: "a drop", payload: &ydbast.Topic{Schema: "app", Name: "events", Change: ydbdiff.Topic{Before: &ydbtopic.Observed{}}},
			want: "DROP TOPIC `app/events`;\n"},
		{name: "an added consumer", payload: &ydbast.TopicConsumer{Schema: "app", Name: "events",
			Consumer: ydbtopic.ConsumerSpec{Name: "worker", SupportedCodecs: []string{"gzip"}}},
			want: "ALTER TOPIC `app/events` ADD CONSUMER `worker` WITH (supported_codecs = 'gzip');\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), &ast.ExtensionStatement{Payload: test.payload})
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestRender_TopicOperation_NonOwningRenderersRefuse hands each statement on
// a topic to every renderer but YDB's: each refuses the payload through the
// common extension boundary without knowing what it is.
func TestRender_TopicOperation_NonOwningRenderersRefuse(t *testing.T) {
	for _, test := range secretlessTargets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			for _, operation := range []struct {
				payload ast.ExtensionPayload
				kind    string
			}{
				{payload: &ydbast.Topic{Name: "events", Change: ydbdiff.Topic{After: &ydbtopic.Desired{}}}, kind: `ptah\.run/ydb/topic-operation`},
				{payload: &ydbast.Topic{Name: "events", Change: ydbdiff.Topic{Before: &ydbtopic.Observed{}}}, kind: `ptah\.run/ydb/topic-operation`},
				{payload: &ydbast.TopicConsumer{Name: "events", Consumer: ydbtopic.ConsumerSpec{Name: "worker"}}, kind: `ptah\.run/ydb/topic-consumer-operation`},
			} {
				sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps.With(capability.Topics, true), &ast.ExtensionStatement{Payload: operation.payload})
				c.Assert(err, qt.ErrorMatches, `target "`+test.dialect+`" does not support extension "`+operation.kind+`" in role "statement"`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// TestTopicFeatures_RefuseUnsupportedTargets refuses a declared topic, and a
// topic change, on every target but YDB: the shared model no longer
// enumerates topics, so no other path can drop one silently.
func TestTopicFeatures_RefuseUnsupportedTargets(t *testing.T) {
	for _, test := range secretlessTargets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{})))}
			c.Assert(builtin.ValidateSchema(desired, test.dialect), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{}, catalog.ServerInfo{Dialect: test.dialect}, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
			change := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("", "events"),
				Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{}}}}}
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, change, test.dialect, planner.Options{})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}
