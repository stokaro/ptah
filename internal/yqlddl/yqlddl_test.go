package yqlddl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/yqlddl"
)

func TestRead_CreateTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want yqlddl.Statement
	}{
		{
			name: "columns, key, inline indexes and a TTL",
			sql: "CREATE TABLE `dir/d1` (\n  id Int64 NOT NULL,\n  k Utf8,\n  c Utf8,\n  amount Decimal(22, 9) DEFAULT 0,\n" +
				"  ts Timestamp NOT NULL DEFAULT Timestamp('2026-01-01T00:00:00Z'),\n  PRIMARY KEY (id),\n" +
				"  INDEX d1_k GLOBAL SYNC ON (k) COVER (c, `amount`),\n  INDEX `d1_u` GLOBAL UNIQUE SYNC ON (c)\n" +
				") WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, TTL = Interval(\"P1D\") ON ts);",
			want: yqlddl.Statement{
				Kind: yqlddl.CreateTable,
				Name: "dir/d1",
				Columns: []yqlddl.Column{
					{Name: "id", Type: "Int64", NotNull: true},
					{Name: "k", Type: "Utf8"},
					{Name: "c", Type: "Utf8"},
					{Name: "amount", Type: "Decimal", Default: true},
					{Name: "ts", Type: "Timestamp", NotNull: true, Default: true},
				},
				PrimaryKey: true,
				Indexes: []yqlddl.Index{
					{Name: "d1_k", Columns: []string{"k"}, Cover: []string{"c", "amount"}},
					{Name: "d1_u", Unique: true, Columns: []string{"c"}},
				},
				TTLColumn: "ts",
				Settings: []yqlddl.Setting{
					{Name: "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT", Value: "4"},
					{Name: "TTL", Value: "INTERVAL", Column: "ts"},
				},
			},
		},
		{
			name: "no key, IF NOT EXISTS, a family and a TTL with tiers",
			sql: "create table if not exists t (id Uint64, ts Timestamp, family cold (data = \"rot\")) " +
				"partition by hash (id) with (store = column, ttl = Interval(\"PT1H\") to external data source s, " +
				"Interval(\"PT2H\") delete on ts)",
			want: yqlddl.Statement{
				Kind:      yqlddl.CreateTable,
				Name:      "t",
				IfExists:  true,
				Columns:   []yqlddl.Column{{Name: "id", Type: "Uint64"}, {Name: "ts", Type: "Timestamp"}},
				Families:  []string{"cold"},
				TTLColumn: "ts",
				Settings: []yqlddl.Setting{
					{Name: "STORE", Value: "COLUMN"},
					{Name: "TTL", Value: "INTERVAL", Column: "ts"},
				},
			},
		},
		{
			name: "columns in families, the family before NOT NULL as YDB 25.1 takes it",
			sql: "CREATE TABLE t (id Uint64 NOT NULL, a Utf8 FAMILY `cold`, c Int32 FAMILY cold NOT NULL DEFAULT 7, " +
				"PRIMARY KEY (id), FAMILY `cold` (COMPRESSION = 'lz4'), FAMILY default (DATA = 'hdd'))",
			want: yqlddl.Statement{
				Kind: yqlddl.CreateTable,
				Name: "t",
				Columns: []yqlddl.Column{
					{Name: "id", Type: "Uint64", NotNull: true},
					{Name: "a", Type: "Utf8", Family: "cold"},
					{Name: "c", Type: "Int32", NotNull: true, Default: true, Family: "cold"},
				},
				PrimaryKey: true,
				Families:   []string{"cold", "default"},
			},
		},
		{
			name: "split points, single and composite",
			sql: "CREATE TABLE t (a Uint64 NOT NULL, b Uint64 NOT NULL, PRIMARY KEY (a, b)) " +
				"WITH (PARTITION_AT_KEYS = ((10, 1), (20, 2), (30, 3)), UNIFORM_PARTITIONS = 4)",
			want: yqlddl.Statement{
				Kind:       yqlddl.CreateTable,
				Name:       "t",
				Columns:    []yqlddl.Column{{Name: "a", Type: "Uint64", NotNull: true}, {Name: "b", Type: "Uint64", NotNull: true}},
				PrimaryKey: true,
				Settings: []yqlddl.Setting{
					{Name: "PARTITION_AT_KEYS", Items: 3},
					{Name: "UNIFORM_PARTITIONS", Value: "4"},
				},
			},
		},
		{
			name: "a name written as a named expression is not known",
			sql:  "CREATE TABLE $t (id Int64 NOT NULL, PRIMARY KEY (id))",
			want: yqlddl.Statement{
				Kind:       yqlddl.CreateTable,
				Columns:    []yqlddl.Column{{Name: "id", Type: "Int64", NotNull: true}},
				PrimaryKey: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_AlterSequence reads the path an ALTER SEQUENCE names and the
// restart it makes: YDB takes START [WITH], INCREMENT [BY] and RESTART [[WITH]
// n], in any order.
func TestRead_AlterSequence(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want yqlddl.Statement
	}{
		{
			name: "settings without a restart",
			sql:  "ALTER SEQUENCE `/local/app/t/_serial_column_id` START WITH 100 INCREMENT BY 5;",
			want: yqlddl.Statement{Kind: yqlddl.AlterSequence, Name: "/local/app/t/_serial_column_id"},
		},
		{
			name: "a restart at a value",
			sql:  "alter sequence if exists `/local/t/_serial_column_id` increment 2 restart with 500",
			want: yqlddl.Statement{Kind: yqlddl.AlterSequence, Name: "/local/t/_serial_column_id", IfExists: true,
				Restart: true, RestartWith: "500"},
		},
		{
			name: "a restart at the start",
			sql:  "ALTER SEQUENCE `/local/t/_serial_column_id` RESTART START 7",
			want: yqlddl.Statement{Kind: yqlddl.AlterSequence, Name: "/local/t/_serial_column_id", Restart: true},
		},
		{
			name: "a restart written without WITH",
			sql:  "ALTER SEQUENCE `/local/t/_serial_column_id` RESTART 42",
			want: yqlddl.Statement{Kind: yqlddl.AlterSequence, Name: "/local/t/_serial_column_id", Restart: true,
				RestartWith: "42"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestRead_AlterTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []yqlddl.Action
	}{
		{
			name: "columns",
			sql:  "ALTER TABLE t ADD COLUMN a Int64 NOT NULL, ADD b Utf8 DEFAULT 'x'u, DROP COLUMN c, DROP d",
			want: []yqlddl.Action{
				{Kind: yqlddl.AddColumn, Column: yqlddl.Column{Name: "a", Type: "Int64", NotNull: true}},
				{Kind: yqlddl.AddColumn, Column: yqlddl.Column{Name: "b", Type: "Utf8", Default: true}},
				{Kind: yqlddl.DropColumn, Column: yqlddl.Column{Name: "c"}},
				{Kind: yqlddl.DropColumn, Column: yqlddl.Column{Name: "d"}},
			},
		},
		{
			name: "indexes",
			sql: "ALTER TABLE t ADD INDEX `t_u` GLOBAL UNIQUE SYNC ON (v) COVER (w), DROP INDEX t_old, " +
				"RENAME INDEX t_a TO t_b",
			want: []yqlddl.Action{
				{Kind: yqlddl.AddIndex, Index: yqlddl.Index{Name: "t_u", Unique: true, Columns: []string{"v"}, Cover: []string{"w"}}},
				{Kind: yqlddl.DropIndex, Index: yqlddl.Index{Name: "t_old"}},
				{Kind: yqlddl.RenameIndex, Index: yqlddl.Index{Name: "t_a"}, NewName: "t_b"},
			},
		},
		{
			name: "a vector index with a prefix, a cover and its settings",
			sql: "ALTER TABLE t ADD INDEX t_e GLOBAL SYNC USING vector_kmeans_tree ON (g, e) COVER (b) " +
				"WITH (similarity = inner_product, vector_type = Uint8, vector_dimension = 3, levels = 1, clusters = 2)",
			want: []yqlddl.Action{{Kind: yqlddl.AddIndex, Index: yqlddl.Index{Name: "t_e", Method: "vector_kmeans_tree",
				VectorType: "uint8", Columns: []string{"g", "e"}, Cover: []string{"b"}}}},
		},
		{
			name: "settings",
			sql: "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, TTL = Interval(\"P1D\") ON `ts` AS SECONDS), " +
				"SET AUTO_PARTITIONING_BY_LOAD DISABLED, RESET (TTL, KEY_BLOOM_FILTER)",
			want: []yqlddl.Action{
				{Kind: yqlddl.SetSettings, Settings: []yqlddl.Setting{
					{Name: "AUTO_PARTITIONING_BY_SIZE", Value: "ENABLED"},
					{Name: "TTL", Value: "INTERVAL", Column: "ts"},
				}},
				{Kind: yqlddl.SetSettings, Settings: []yqlddl.Setting{{Name: "AUTO_PARTITIONING_BY_LOAD", Value: "DISABLED"}}},
				{Kind: yqlddl.ResetSettings, Settings: []yqlddl.Setting{{Name: "TTL"}, {Name: "KEY_BLOOM_FILTER"}}},
			},
		},
		{
			name: "renames, changefeeds and actions read as other",
			sql: "ALTER TABLE t RENAME TO `dir/u`, ALTER COLUMN v DROP NOT NULL, " +
				"ADD CHANGEFEED cf WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON'), DROP CHANGEFEED old, DROP FAMILY f",
			want: []yqlddl.Action{
				{Kind: yqlddl.RenameTable, NewName: "dir/u"},
				{},
				{Kind: yqlddl.AddChangefeed, Changefeed: "cf"},
				{Kind: yqlddl.DropChangefeed, Changefeed: "old"},
				{},
			},
		},
		{
			name: "column families",
			sql: "ALTER TABLE t ADD FAMILY `cold` (DATA = \"ssd\"), ALTER FAMILY default SET COMPRESSION 'lz4', " +
				"ALTER COLUMN `v` SET FAMILY cold, ADD COLUMN w Int32 FAMILY `cold` NOT NULL DEFAULT 1",
			want: []yqlddl.Action{
				{Kind: yqlddl.AddFamily, Family: "cold"},
				{Kind: yqlddl.AlterFamily, Family: "default"},
				{Kind: yqlddl.SetColumnFamily, Column: yqlddl.Column{Name: "v"}, Family: "cold"},
				{Kind: yqlddl.AddColumn, Column: yqlddl.Column{Name: "w", Type: "Int32", NotNull: true, Default: true, Family: "cold"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := yqlddl.Read(test.sql)
			c.Assert(stmt.Kind, qt.Equals, yqlddl.AlterTable)
			c.Assert(stmt.Name, qt.Equals, "t")
			c.Assert(stmt.Actions, qt.DeepEquals, test.want)
		})
	}
}

func TestRead_DropsAndViews(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want yqlddl.Statement
	}{
		{name: "drop table", sql: "-- retire it\nDROP TABLE `dir/t`;", want: yqlddl.Statement{Kind: yqlddl.DropTable, Name: "dir/t"}},
		{name: "drop table if exists", sql: "DROP TABLE IF EXISTS t", want: yqlddl.Statement{Kind: yqlddl.DropTable, Name: "t", IfExists: true}},
		{name: "drop view", sql: "DROP VIEW IF EXISTS v", want: yqlddl.Statement{Kind: yqlddl.DropView, Name: "v", IfExists: true}},
		{
			name: "create view",
			sql: "CREATE VIEW `dir/v` WITH (security_invoker = TRUE) AS SELECT a.id FROM base AS a " +
				"JOIN `dir/other` AS o ON a.id = o.id WHERE a.id IN (SELECT id FROM third)",
			want: yqlddl.Statement{Kind: yqlddl.CreateView, Name: "dir/v", Reads: []string{"base", "dir/other", "third"}},
		},
		{name: "a statement this package does not read", sql: "UPSERT INTO t (id) VALUES (1)", want: yqlddl.Statement{}},
		{name: "drop the pool default", sql: "drop resource pool default;",
			want: yqlddl.Statement{Kind: yqlddl.DropResourcePool, Name: "default"}},
		{name: "drop a pool named as YQL quotes it", sql: "DROP RESOURCE POOL `Default`",
			want: yqlddl.Statement{Kind: yqlddl.DropResourcePool, Name: "Default"}},
		{name: "a classifier is no pool", sql: "DROP RESOURCE POOL CLASSIFIER default", want: yqlddl.Statement{}},
		{name: "drop a backup collection", sql: "DROP BACKUP COLLECTION `nightly`",
			want: yqlddl.Statement{Kind: yqlddl.DropBackupCollection, Name: "nightly"}},
		{name: "analyze", sql: "ANALYZE `dir/t` (v)", want: yqlddl.Statement{Kind: yqlddl.Analyze, Name: "dir/t"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestRead_CreateTopic(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want yqlddl.Statement
	}{
		{name: "a name alone", sql: "CREATE TOPIC `dir/events`;", want: yqlddl.Statement{Kind: yqlddl.CreateTopic, Name: "dir/events"}},
		{
			name: "consumers and settings",
			sql: "CREATE TOPIC IF NOT EXISTS events (CONSUMER billing WITH (important = TRUE, supported_codecs = 'raw,gzip'), " +
				"CONSUMER `audit`) WITH (min_active_partitions = 2, retention_period = Interval('PT2H'), metering_mode = \"it\\'s\")",
			want: yqlddl.Statement{
				Kind: yqlddl.CreateTopic, Name: "events", IfExists: true,
				Consumers: []yqlddl.Consumer{
					{Name: "billing", Settings: []yqlddl.Setting{
						{Name: "IMPORTANT", Value: "TRUE"},
						{Name: "SUPPORTED_CODECS", Text: "raw,gzip"},
					}},
					{Name: "audit"},
				},
				Settings: []yqlddl.Setting{
					{Name: "MIN_ACTIVE_PARTITIONS", Value: "2"},
					{Name: "RETENTION_PERIOD", Value: "INTERVAL"},
					{Name: "METERING_MODE", Text: "it's"},
				},
			},
		},
		{name: "drop topic", sql: "DROP TOPIC IF EXISTS `dir/events`", want: yqlddl.Statement{Kind: yqlddl.DropTopic, Name: "dir/events", IfExists: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_ReplicationsAndTransfers reads the settings of an async replication
// and a transfer: a CREATE's as its settings, an ALTER's as one SET action,
// and a DROP's CASCADE. A transfer's lambda, with semicolons, brackets and a
// WITH of its own inside, is not read as a clause.
func TestRead_ReplicationsAndTransfers(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want yqlddl.Statement
	}{
		{
			name: "create replication",
			sql: "CREATE ASYNC REPLICATION `dr/mirror` FOR a AS ra, `b` AS `rb` WITH (" +
				"CONNECTION_STRING = 'grpc://p:2136/?database=/prod', PASSWORD = 'x', CONSISTENCY_LEVEL = 'GLOBAL')",
			want: yqlddl.Statement{Kind: yqlddl.CreateAsyncReplication, Name: "dr/mirror", Settings: []yqlddl.Setting{
				{Name: "CONNECTION_STRING", Text: "grpc://p:2136/?database=/prod"},
				{Name: "PASSWORD", Text: "x"},
				{Name: "CONSISTENCY_LEVEL", Text: "GLOBAL"},
			}},
		},
		{
			name: "alter replication",
			sql:  "ALTER ASYNC REPLICATION mirror SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE')",
			want: yqlddl.Statement{Kind: yqlddl.AlterAsyncReplication, Name: "mirror", Actions: []yqlddl.Action{{
				Kind: yqlddl.SetSettings, Settings: []yqlddl.Setting{
					{Name: "STATE", Text: "DONE"},
					{Name: "FAILOVER_MODE", Text: "FORCE"},
				},
			}}},
		},
		{
			name: "drop replication with cascade",
			sql:  "DROP ASYNC REPLICATION mirror CASCADE",
			want: yqlddl.Statement{Kind: yqlddl.DropAsyncReplication, Name: "mirror", Cascade: true},
		},
		{
			name: "drop replication",
			sql:  "DROP ASYNC REPLICATION `dr/mirror`",
			want: yqlddl.Statement{Kind: yqlddl.DropAsyncReplication, Name: "dr/mirror"},
		},
		{
			name: "create transfer",
			sql: "CREATE TRANSFER ingest FROM `orders/feed` TO log USING ($m) -> { $with = (1); " +
				"return [<| a: $with, b: ListMap([1], ($x) -> { return $x; }) |>]; } WITH (TOKEN = 't', BATCH_SIZE_BYTES = 10)",
			want: yqlddl.Statement{Kind: yqlddl.CreateTransfer, Name: "ingest", Settings: []yqlddl.Setting{
				{Name: "TOKEN", Text: "t"},
				{Name: "BATCH_SIZE_BYTES", Value: "10"},
			}},
		},
		{
			name: "alter transfer",
			sql:  "ALTER TRANSFER ingest SET USING ($m) -> { return [<| a: 1 |>]; }, SET (FLUSH_INTERVAL = Interval('PT10S'))",
			want: yqlddl.Statement{Kind: yqlddl.AlterTransfer, Name: "ingest", Actions: []yqlddl.Action{{
				Kind: yqlddl.SetSettings, Settings: []yqlddl.Setting{{Name: "FLUSH_INTERVAL", Value: "INTERVAL"}},
			}}},
		},
		{
			name: "alter transfer lambda alone",
			sql:  "ALTER TRANSFER ingest SET USING ($m) -> { return []; }",
			want: yqlddl.Statement{Kind: yqlddl.AlterTransfer, Name: "ingest"},
		},
		{
			name: "drop transfer",
			sql:  "DROP TRANSFER `shop/ingest`",
			want: yqlddl.Statement{Kind: yqlddl.DropTransfer, Name: "shop/ingest"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestRead_AlterTopic(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []yqlddl.Action
	}{
		{
			name: "topic settings",
			sql:  "ALTER TOPIC events SET (retention_period = Interval('P1D'), supported_codecs = 'raw'u), RESET (partition_count_limit)",
			want: []yqlddl.Action{
				{Kind: yqlddl.SetSettings, Settings: []yqlddl.Setting{
					{Name: "RETENTION_PERIOD", Value: "INTERVAL"},
					{Name: "SUPPORTED_CODECS", Text: "raw"},
				}},
				{Kind: yqlddl.ResetSettings, Settings: []yqlddl.Setting{{Name: "PARTITION_COUNT_LIMIT"}}},
			},
		},
		{
			name: "consumers",
			sql: "ALTER TOPIC events ADD CONSUMER `fresh` WITH (important = TRUE), DROP CONSUMER gone, " +
				"ALTER CONSUMER kept SET (read_from = Timestamp('2026-01-01T00:00:00Z')), ALTER CONSUMER kept RESET (availability_period), " +
				"ALTER CONSUMER odd RENAME TO other",
			want: []yqlddl.Action{
				{Kind: yqlddl.AddConsumer, Consumer: "fresh", Settings: []yqlddl.Setting{{Name: "IMPORTANT", Value: "TRUE"}}},
				{Kind: yqlddl.DropConsumer, Consumer: "gone"},
				{Kind: yqlddl.SetConsumerSettings, Consumer: "kept", Settings: []yqlddl.Setting{{Name: "READ_FROM", Value: "TIMESTAMP"}}},
				{Kind: yqlddl.ResetConsumerSettings, Consumer: "kept", Settings: []yqlddl.Setting{{Name: "AVAILABILITY_PERIOD"}}},
				{Consumer: "odd"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := yqlddl.Read(test.sql)
			c.Assert(stmt.Kind, qt.Equals, yqlddl.AlterTopic)
			c.Assert(stmt.Name, qt.Equals, "events")
			c.Assert(stmt.Actions, qt.DeepEquals, test.want)
		})
	}
}

// tokens lexes a statement the way the package does, comments and whitespace
// left out.
func tokens(statement string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	var out []lexer.Token
	for token := lexr.NextToken(); token.Type != lexer.TokenEOF; token = lexr.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			out = append(out, token)
		}
	}
	return out
}

func TestTablesRead(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "from and join", sql: "SELECT * FROM a JOIN `dir/b` ON a.id = b.id", want: []string{"a", "dir/b"}},
		{name: "a subquery's own from", sql: "SELECT * FROM (SELECT id FROM inner_t) AS x", want: []string{"inner_t"}},
		{name: "a named expression and a call read no table", sql: "SELECT * FROM $rows AS r JOIN AS_TABLE($list) AS l ON r.id = l.id", want: nil},
		{name: "no from", sql: "SELECT 1", want: nil},
		{name: "a name inside a string is not read", sql: "SELECT 'FROM t' AS label", want: nil},
		{name: "the statement ends at its semicolon", sql: "SELECT 1; SELECT * FROM later", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.TablesRead(tokens(test.sql)), qt.DeepEquals, test.want)
		})
	}
}

func TestIndex_Uses(t *testing.T) {
	index := yqlddl.Index{Name: "i", Columns: []string{"k"}, Cover: []string{"c"}}
	tests := []struct {
		column string
		want   bool
	}{
		{column: "k", want: true},
		{column: "c", want: true},
		{column: "K", want: false},
		{column: "other", want: false},
	}
	for _, test := range tests {
		t.Run(test.column, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(index.Uses(test.column), qt.Equals, test.want)
		})
	}
}

func TestStatement_Requirements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []yqlddl.Requirement
	}{
		{
			name: "a unique index and a column default, in action order",
			sql:  "ALTER TABLE t ADD COLUMN a Int64 NOT NULL DEFAULT 0, ADD INDEX t_v GLOBAL UNIQUE SYNC ON (v)",
			want: []yqlddl.Requirement{
				{Capability: capability.AddColumnWithDefault, Action: 0},
				{Capability: capability.UniqueIndexOnExistingTable, Action: 1},
			},
		},
		{name: "a plain index and a nullable column", sql: "ALTER TABLE t ADD INDEX t_v GLOBAL ON (v), ADD COLUMN b Utf8", want: nil},
		{name: "a unique index declared with its table", sql: "CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX t_v GLOBAL UNIQUE ON (v))", want: nil},
		{
			name: "vector indexes declared with their table, one over bit vectors",
			sql: "CREATE TABLE t (id Uint64 NOT NULL, e String, PRIMARY KEY (id), INDEX t_g GLOBAL ON (id), " +
				"INDEX t_e GLOBAL USING vector_kmeans_tree ON (e) WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2), " +
				"INDEX t_b GLOBAL USING Vector_KMeans_Tree ON (e) WITH (similarity=cosine, vector_type='Bit', vector_dimension=8, levels=1, clusters=2))",
			want: []yqlddl.Requirement{
				{Capability: capability.VectorIndexes, Action: 1, Inline: true},
				{Capability: capability.VectorIndexes, Action: 2, Inline: true},
				{Capability: capability.VectorBitType, Action: 2, Inline: true},
			},
		},
		{
			name: "a vector index added, covering a column",
			sql: "ALTER TABLE t ADD COLUMN b Utf8, ADD INDEX t_e GLOBAL USING vector_kmeans_tree ON (g, e) COVER (b) " +
				"WITH (distance=cosine, vector_type=\"bit\", vector_dimension=8, levels=1, clusters=2)",
			want: []yqlddl.Requirement{
				{Capability: capability.VectorIndexes, Action: 1},
				{Capability: capability.VectorBitType, Action: 1},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql).Requirements(), qt.DeepEquals, test.want)
		})
	}
}

func TestWrittenTable_HappyPath(t *testing.T) {
	tests := []struct {
		statement string
		want      string
	}{
		{statement: "UPSERT INTO `docs/d` (id, emb) VALUES (1, \"x\")", want: "docs/d"},
		{statement: "insert into d (id) values (1);", want: "d"},
		{statement: "INSERT OR REVERT INTO d SELECT * FROM e", want: "d"},
		{statement: "REPLACE INTO d (id) VALUES (1)", want: "d"},
		{statement: "UPDATE d SET n = 1 WHERE id = 2", want: "d"},
		{statement: "BATCH UPDATE d SET n = 1", want: "d"},
		{statement: "DELETE FROM d WHERE id = 1", want: "d"},
		{statement: "-- a comment\nBATCH DELETE FROM `d` WHERE id = 1", want: "d"},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			got, ok := yqlddl.WrittenTable(test.statement)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestWrittenTable_FailurePath(t *testing.T) {
	for _, statement := range []string{
		"SELECT * FROM d",
		"ALTER TABLE d ADD COLUMN n Int64",
		"UPSERT INTO $target (id) VALUES (1)",
		"DELETE d",
		"",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			got, ok := yqlddl.WrittenTable(statement)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
