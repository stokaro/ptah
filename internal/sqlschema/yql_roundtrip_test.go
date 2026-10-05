package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/sqlschema"
)

func TestParseRender(t *testing.T) {
	for _, text := range []string{
		`CREATE COORDINATION NODE locks;`,
		`CREATE COORDINATION NODE locks WITH (self_check_period = Interval('PT1S'), session_grace_period = Interval('PT2S'), read_consistency_mode = 'strict', attach_consistency_mode = 'relaxed', rate_limiter_counters_mode = 'detailed');`,
		`CREATE VIEW v WITH (security_invoker = TRUE) AS SELECT 1 AS id; CREATE TOPIC events;`,
		`CREATE VIEW v WITH (security_invoker = TRUE) AS DO BEGIN $f = ($x) -> { RETURN $x + 1; }; SELECT $f(1) AS id; END DO;`,
		`CREATE TOPIC events (CONSUMER worker WITH (important = true, read_from = Timestamp("2026-01-01T00:00:00Z"), supported_codecs = "raw,gzip")) WITH (min_active_partitions = 2, retention_period = Interval("P1D"), supported_codecs = "raw,gzip");`,
		`CREATE TOPIC events (CONSUMER reader WITH (availability_period = Interval("PT1H"))) WITH (min_active_partitions = 2, max_active_partitions = 4, auto_partitioning_strategy = "scale_up", auto_partitioning_up_utilization_percent = 80, auto_partitioning_down_utilization_percent = 20, auto_partitioning_stabilization_window = Interval("PT1M"), partition_write_speed_bytes_per_second = 1024, partition_write_burst_bytes = 2048);`,
		`CREATE TABLE t (id Uint64 NOT NULL, ts Timestamp, body Utf8 FAMILY payload, PRIMARY KEY (id), FAMILY payload (COMPRESSION = "lz4"), FAMILY empty ()) WITH (TTL = Interval("PT1H") ON ts);`,
		`CREATE TABLE t (id Uint64 NOT NULL, ts Uint64, PRIMARY KEY (id)) WITH (TTL = Interval("PT1H") ON ts AS SECONDS);`,
		`CREATE TABLE t (ts Timestamp NOT NULL, id Uint64, PRIMARY KEY (ts)) PARTITION BY HASH (ts) WITH (STORE = COLUMN, TTL = Interval("PT1H") DELETE ON ts);`,
		`CREATE TABLE t (id Utf8 NOT NULL, PRIMARY KEY (id)) WITH (PARTITION_AT_KEYS = ('a b'u));`,
		`CREATE TABLE t (id Uint64 NOT NULL, part Utf8 NOT NULL, PRIMARY KEY (id, part)) WITH (PARTITION_AT_KEYS = ((10, 'a\n\'b'u), (20)));`,
		"--!syntax_v1\nCREATE TABLE t (id Int64 NOT NULL, value Utf8 DEFAULT 'active'u, PRIMARY KEY (id), INDEX i GLOBAL SYNC ON (value));",
		"CREATE TABLE t (id Uint64 NOT NULL, v String, PRIMARY KEY (id), INDEX i GLOBAL SYNC USING vector_kmeans_tree ON (v) WITH (distance = 'cosine', vector_type = 'float', vector_dimension = 3, levels = 1, clusters = 2));",
		"CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX i GLOBAL USING fulltext_plain ON (v) WITH (tokenizer = 'standard', use_filter_lowercase = true));",
		"CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX i LOCAL USING bloom_filter ON (v)) PARTITION BY HASH (id) WITH (STORE = COLUMN);",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(text), "ydb")
			c.Assert(err, qt.IsNil)
			rendered, err := renderer.GetOrderedCreateStatements(&database, "ydb")
			c.Assert(err, qt.IsNil)
			again, _, err := sqlschema.Read([]byte(strings.Join(rendered, "\n")), "ydb")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", rendered))
			second, err := renderer.GetOrderedCreateStatements(&again, "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(second, qt.DeepEquals, rendered)
		})
	}
}
