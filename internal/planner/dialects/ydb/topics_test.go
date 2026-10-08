package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateMigrationAST_Topics_HappyPath pins where topics sit in a plan:
// a dropped topic first, so a table created under its path finds the path
// free, and a created or changed topic last, after the tables are dropped, so
// a topic created under a dropped table's path finds it free. Measured on
// 26.2.1.14 and 25.1.4.7: a path names one object (`unexpected path type` for
// a topic over a table), and each scheme statement runs as a query of its own.
func TestGenerateMigrationAST_Topics_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "queue",
			Table:  schemamodel.Table{StructName: "Q", Name: "queue"},
			Fields: []schemamodel.Field{{StructName: "Q", Name: "id", Type: "BIGINT", Primary: true}},
		}},
		TablesRemoved: difftypes.TableRemovals{{Name: "events", Current: observedFeeds(t, "", "events")}},
		TopicsAdded:   difftypes.TopicChanges{{Name: "events", Spec: ast.TopicSpec{RetentionPeriod: "PT2H"}}},
		TopicsRemoved: difftypes.TopicChanges{{Name: "queue"}},
		TopicsModified: []difftypes.TopicDiff{{Name: "audit",
			Desired: ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "fresh"}}},
			Current: ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "gone"}}}}},
	}

	got := render(c, capability.YDB251(), diff)

	c.Assert(got, qt.Equals, "DROP TOPIC `queue`;\n"+
		"CREATE TABLE `queue` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n"+
		"DROP TABLE `events`;\n"+
		"CREATE TOPIC `events` WITH (retention_period = Interval('PT2H'));\n"+
		"ALTER TOPIC `audit` DROP CONSUMER `gone`, ADD CONSUMER `fresh`;\n")
}

// A topic change YDB cannot make is refused before any node is returned:
// through the key a line lacks, and with YDB's own answer for a change no
// line makes in place.
func TestGenerateMigrationAST_Topics_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a topic on a target without topics",
			caps: capability.YDB262().With(capability.Topics, false),
			diff: &difftypes.SchemaDiff{TopicsAdded: difftypes.TopicChanges{{Name: "events"}}},
			want: "topic events, which requires target capability topics, unavailable on this ydb target",
		},
		{
			name: "an availability period added on 25.1",
			caps: capability.YDB251(),
			diff: &difftypes.SchemaDiff{TopicsModified: []difftypes.TopicDiff{{Name: "events",
				Desired: ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "c", AvailabilityPeriod: "PT1H"}}},
				Current: ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "c"}}}}}},
			want: `consumer "c" of topic events takes availability_period, which requires target capability ` +
				"topic_consumer_availability_period, unavailable on this ydb target",
		},
		{
			name: "auto-partitioning disabled again",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{TopicsModified: []difftypes.TopicDiff{{Name: "events",
				Desired: ast.TopicSpec{}, Current: ast.TopicSpec{AutoPartitioningStrategy: "paused"}}}},
			want: "topic events: its auto-partitioning is paused and is declared disabled, and YDB does not disable " +
				"auto-partitioning once it is on \\(`Can't disable auto partitioning.`\\); declare auto_partitioning_strategy " +
				"paused to stop it",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
