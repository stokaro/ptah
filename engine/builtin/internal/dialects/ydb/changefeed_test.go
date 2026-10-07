package ydb_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// withChangefeeds is a table t keyed on an Int64 id, with the given
// changefeeds and indexes.
func withChangefeeds(changefeeds []ast.ChangefeedSpec, indexes ...*ast.IndexNode) *ast.CreateTableNode {
	table := &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
		ast.NewColumn("id", "BIGINT").SetPrimary(), ast.NewColumn("a", "TEXT"),
	}, Changefeeds: changefeeds}
	for _, index := range indexes {
		table.AddIndex(index)
	}
	return table
}

// TestRender_Changefeed_HappyPath pins how a changefeed is written: never in
// CREATE TABLE, which takes none on any line, but by an ALTER TABLE after it,
// one per changefeed, and its consumers by an ALTER TOPIC each once the topic
// exists. Each rendering was applied to local-ydb 26.2.1.14 and 25.1.4.7 one
// statement per query and read back through DescribeTable and DescribeTopic.
func TestRender_Changefeed_HappyPath(t *testing.T) {
	updates := ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT12H",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", Important: true}}}
	keys := ast.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "two changefeeds of a new table, after its index",
			caps: capability.YDB251(),
			node: withChangefeeds([]ast.ChangefeedSpec{updates, keys}, &ast.IndexNode{Name: "i", Columns: []string{"a"}}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `i` GLOBAL SYNC ON (`a`)\n" +
				");\n" +
				"ALTER TABLE `t` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON', " +
				"RETENTION_PERIOD = Interval('PT12H'));\n" +
				"ALTER TOPIC `t/updates` ADD CONSUMER `audit` WITH (important = TRUE);\n" +
				"ALTER TABLE `t` ADD CHANGEFEED `keys` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON');\n",
		},
		{
			name: "a changefeed added to a table that exists",
			caps: capability.YDB262(),
			node: alter(&ast.AddChangefeedOperation{Changefeed: ast.ChangefeedSpec{Name: "f", Mode: "NEW_IMAGE",
				Format: "JSON", UserSIDs: true, SchemaChanges: true, TopicAutoPartitioning: true}}),
			want: "ALTER TABLE `t` ADD CHANGEFEED `f` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', " +
				"USER_SIDS = TRUE, SCHEMA_CHANGES = TRUE, TOPIC_AUTO_PARTITIONING = 'ENABLED');\n",
		},
		{
			name: "a changefeed dropped",
			caps: capability.YDB251(),
			node: alter(&ast.DropChangefeedOperation{Name: "f"}),
			want: "ALTER TABLE `t` DROP CHANGEFEED `f`;\n",
		},
		{
			name: "a topic changed in place",
			caps: capability.YDB262(),
			node: alter(&ast.AlterChangefeedTopicOperation{
				Changefeed: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON",
					Consumers: []ast.TopicConsumerSpec{{Name: "late", AvailabilityPeriod: "PT1H"}}},
				Previous: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT6H",
					Consumers: []ast.TopicConsumerSpec{{Name: "old"}}},
			}),
			want: "ALTER TOPIC `t/f` SET (retention_period = Interval('P1D'));\n" +
				"ALTER TOPIC `t/f` DROP CONSUMER `old`;\n" +
				"ALTER TOPIC `t/f` ADD CONSUMER `late` WITH (availability_period = Interval('PT1H'));\n",
		},
		{
			name: "a topic of several partitions on a Uint64 key",
			caps: capability.YDB262(),
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT UNSIGNED").SetPrimary()},
				Changefeeds: []ast.ChangefeedSpec{{Name: "f", Mode: "UPDATES", Format: "JSON", TopicMinActivePartitions: 2}}},
			want: "CREATE TABLE `t` (\n" +
				"    `id` Uint64 NOT NULL,\n" +
				"    PRIMARY KEY (`id`)\n" +
				");\n" +
				"ALTER TABLE `t` ADD CHANGEFEED `f` WITH (MODE = 'UPDATES', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 2);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_Changefeed_RefusesByCapability names the key a target lacks, so a
// line that gains the option is a preset change.
func TestRender_Changefeed_RefusesByCapability(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantKey capability.Capability
		wantErr string
	}{
		{
			name: "USER_SIDS on 25.4", caps: capability.YDB254(),
			node:    withChangefeeds([]ast.ChangefeedSpec{{Name: "f", Mode: "UPDATES", Format: "JSON", UserSIDs: true}}),
			wantKey: capability.ChangefeedUserSIDs,
			wantErr: `changefeed "f" of table "t" takes USER_SIDS, which requires target capability changefeed_user_sids, .*`,
		},
		{
			name: "an auto-partitioned topic added on 25.1", caps: capability.YDB251(),
			node:    alter(&ast.AddChangefeedOperation{Changefeed: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON", TopicAutoPartitioning: true}}),
			wantKey: capability.ChangefeedTopicAutoPartitioning,
			wantErr: `changefeed "f" of table "t" takes TOPIC_AUTO_PARTITIONING, which requires .*`,
		},
		{
			name: "an availability period on 25.3", caps: capability.YDB253(),
			node: alter(&ast.AlterChangefeedTopicOperation{
				Changefeed: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON",
					Consumers: []ast.TopicConsumerSpec{{Name: "c", AvailabilityPeriod: "PT1H"}}},
				Previous: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON"},
			}),
			wantKey: capability.TopicConsumerAvailabilityPeriod,
			wantErr: `consumer "c" of changefeed "f" of table "t" takes availability_period, which requires .*`,
		},
		{
			name: "a drop without the key", caps: capability.YDB262().With(capability.Changefeeds, false),
			node:    alter(&ast.DropChangefeedOperation{Name: "f"}),
			wantKey: capability.Changefeeds,
			wantErr: `dropping changefeed "f" of table "t", which requires target capability changefeeds, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(test.wantKey))
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRender_Changefeed_FailurePath refuses what YDB refuses on every line,
// with the server's reason, before any statement is written.
func TestRender_Changefeed_FailurePath(t *testing.T) {
	plain := ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON"}
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "a changefeed named after an index",
			node:    withChangefeeds([]ast.ChangefeedSpec{{Name: "i", Mode: "UPDATES", Format: "JSON"}}, &ast.IndexNode{Name: "i", Columns: []string{"a"}}),
			wantErr: `table "t": changefeed "i" has the name of one of its indexes, .*`,
		},
		{
			name:    "Debezium in UPDATES mode",
			node:    withChangefeeds([]ast.ChangefeedSpec{{Name: "f", Mode: "UPDATES", Format: "DEBEZIUM_JSON"}}),
			wantErr: `changefeed "f" of table "t": YDB writes DEBEZIUM_JSON in every mode but UPDATES .*`,
		},
		{
			name: "several topic partitions on a Utf8 key",
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("id", "TEXT").SetPrimary()},
				Changefeeds: []ast.ChangefeedSpec{{Name: "f", Mode: "UPDATES", Format: "JSON", TopicMinActivePartitions: 2}}},
			wantErr: `changefeed "f" of table "t": its topic starts with 2 partitions, .* not Utf8`,
		},
		{
			name: "an option changed in place",
			node: alter(&ast.AlterChangefeedTopicOperation{Changefeed: plain,
				Previous: ast.ChangefeedSpec{Name: "f", Mode: "KEYS_ONLY", Format: "JSON"}}),
			wantErr: `changefeed "f" of table "t": YDB changes no option of a changefeed in place .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}
