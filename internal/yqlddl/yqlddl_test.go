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
				TTLColumn: "ts",
				Settings: []yqlddl.Setting{
					{Name: "STORE", Value: "COLUMN"},
					{Name: "TTL", Value: "INTERVAL", Column: "ts"},
				},
			},
		},
		{
			name: "split points, single and composite",
			sql: "CREATE TABLE t (a Uint64 NOT NULL, b Uint64 NOT NULL, PRIMARY KEY (a, b)) " +
				"WITH (PARTITION_AT_KEYS = ((10, 1), (20, 2), (30, 3)), UNIFORM_PARTITIONS = 4)",
			want: yqlddl.Statement{
				Kind:       yqlddl.CreateTable,
				Name:       "t",
				Columns:    []yqlddl.Column{{Name: "a", NotNull: true}, {Name: "b", NotNull: true}},
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
			name: "renames and actions read as other",
			sql: "ALTER TABLE t RENAME TO `dir/u`, ALTER COLUMN v DROP NOT NULL, ADD FAMILY f (DATA = \"ssd\"), " +
				"ADD CHANGEFEED cf WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON'), DROP CHANGEFEED old, DROP FAMILY f",
			want: []yqlddl.Action{
				{Kind: yqlddl.RenameTable, NewName: "dir/u"},
				{}, {}, {}, {}, {},
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql), qt.DeepEquals, test.want)
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlddl.Read(test.sql).Requirements(), qt.DeepEquals, test.want)
		})
	}
}
