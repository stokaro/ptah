package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// Each row is a topic statement YDB accepts and keeps less of than it says,
// or one YDB 26.2 keeps as it was and 25.1 refuses.
func TestYDBRules_ReportTopicSettingsKeptAsNothing(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "a topic setting reset", sql: "ALTER TOPIC events RESET (retention_period);",
			want: []string{"0001_t.up.sql:1:YD113"}},
		{name: "a consumer setting reset", sql: "ALTER TOPIC events ALTER CONSUMER c RESET (read_from);",
			want: []string{"0001_t.up.sql:1:YD113"}},
		{name: "a storage limit", sql: "CREATE TOPIC events WITH (retention_storage_mb = 1024);",
			want: []string{"0001_t.up.sql:1:YD114"}},
		{name: "a partition count limit set later", sql: "ALTER TOPIC events SET (partition_count_limit = 20);",
			want: []string{"0001_t.up.sql:1:YD114"}},
		{name: "a metering mode on creation", sql: "CREATE TOPIC events WITH (metering_mode = 'request_units');",
			want: []string{"0001_t.up.sql:1:YD114"}},
		{name: "an unknown codec", sql: "CREATE TOPIC events WITH (supported_codecs = 'raw,kafka_batch');",
			want: []string{"0001_t.up.sql:1:YD114"}},
		{name: "an unknown consumer codec", sql: "ALTER TOPIC events ADD CONSUMER c WITH (supported_codecs = 'snappy');",
			want: []string{"0001_t.up.sql:1:YD114"}},
		{name: "a maximum without auto-partitioning", sql: "CREATE TOPIC events WITH (max_active_partitions = 5);",
			want: []string{"0001_t.up.sql:1:YD114"}},
		{name: "a maximum with auto-partitioning disabled",
			sql:  "CREATE TOPIC events WITH (auto_partitioning_strategy = 'disabled', max_active_partitions = 5);",
			want: []string{"0001_t.up.sql:1:YD114"}},
		{name: "a shared consumer on creation", sql: "CREATE TOPIC events (CONSUMER c WITH (type = 'shared'));",
			want: []string{"0001_t.up.sql:1:YD114"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, map[string]string{"0001_t.up.sql": test.sql + "\n"}, "")), qt.DeepEquals, test.want)
		})
	}
}

// Each row is the remedy for a row above, or a topic statement YDB keeps as
// written.
func TestYDBRules_LeaveTopicStatementsYDBKeeps(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "a topic with every setting YDB keeps", sql: "CREATE TOPIC events (CONSUMER c WITH (important = TRUE, " +
			"supported_codecs = 'RAW,gzip')) WITH (min_active_partitions = 2, auto_partitioning_strategy = 'scale_up', " +
			"max_active_partitions = 4, retention_period = Interval('PT36H'), supported_codecs = 'raw,gzip,lzop,zstd,custom');"},
		{name: "a setting set to a value", sql: "ALTER TOPIC events SET (retention_period = Interval('P1D'));"},
		{name: "an availability period reset", sql: "ALTER TOPIC events ALTER CONSUMER c RESET (availability_period);"},
		{name: "a maximum set on an existing topic", sql: "ALTER TOPIC events SET (max_active_partitions = 8);"},
		{name: "a consumer dropped", sql: "ALTER TOPIC events DROP CONSUMER c;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, map[string]string{"0001_t.up.sql": test.sql + "\n"}, "")), qt.HasLen, 0)
		})
	}
}

func TestYDBRules_SayWhatATopicStatementKeeps(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "reset", sql: "ALTER TOPIC `app/events` RESET (retention_period, supported_codecs);",
			want: "RESET (retention_period, supported_codecs) of topic app/events changes nothing on YDB 26.2, which keeps " +
				"every value as it was, and is refused on 25.1; SET each setting to the value it is to hold instead"},
		{name: "consumer reset", sql: "ALTER TOPIC events ALTER CONSUMER c RESET (read_from, availability_period);",
			want: "RESET (read_from) of consumer c of topic events changes nothing on YDB 26.2, which keeps every value as it " +
				"was, and is refused on 25.1; SET each setting to the value it is to hold instead"},
		{name: "kept as nothing", sql: "CREATE TOPIC events WITH (supported_codecs = 'raw,foo');",
			want: `supported_codecs naming codec "foo" of topic events: YDB accepts the statement and keeps nothing of it, ` +
				"so the topic does not hold what the statement says"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, err := lint.ResolveTarget("ydb", "")
			c.Assert(err, qt.IsNil)

			findings, err := lint.LintFS(fixture(map[string]string{"0001_t.up.sql": test.sql + "\n"}),
				lint.Options{Dialect: "ydb", Target: target})

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Equals, test.want)
		})
	}
}

// DS107 reports DROP TOPIC on YDB, as it reports DROP USER and DROP GROUP:
// the topic goes with every message it holds and every consumer's position in
// it.
func TestYDBRules_DS107ReportsADroppedTopic(t *testing.T) {
	c := qt.New(t)
	sites := ydbLint(c, map[string]string{"0001_t.up.sql": "DROP TOPIC `shop/events`;\n"}, "")
	c.Assert(sites, qt.Contains, "0001_t.up.sql:1:DS107")
}
