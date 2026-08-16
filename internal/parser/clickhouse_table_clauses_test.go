package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
)

// Every MergeTree table carries clauses after its column list -- ORDER BY is
// required by the engine -- and the parser refused all of them with
// `unsupported table option`. The ClickHouse renderer has written them since
// the dialect was added, so Ptah emitted DDL it could not read back
// (stokaro/ptah#1571).

func parseOneTable(c *qt.C, sql string) *ast.CreateTableNode {
	c.Helper()

	statements, err := parser.NewParser(sql).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	return table
}

func TestParserReadsMergeTreeTableClauses(t *testing.T) {
	tests := []struct {
		name   string
		clause string
		option string
		want   string
	}{
		{
			name:   "ORDER BY a single column",
			clause: "ORDER BY a",
			option: "ORDER_BY",
			want:   "a",
		},
		{
			// The parentheses are part of the value: ClickHouse reads
			// `ORDER BY (a, b)` and `ORDER BY a, b` the same way, but the
			// renderer writes back what it was given, and a value rebuilt from
			// tokens is where a difference would come from.
			name:   "ORDER BY a parenthesised tuple",
			clause: "ORDER BY (a, b)",
			option: "ORDER_BY",
			want:   "(a, b)",
		},
		{
			name:   "ORDER BY tuple(), the empty sorting key",
			clause: "ORDER BY tuple()",
			option: "ORDER_BY",
			want:   "tuple()",
		},
		{
			name:   "PRIMARY KEY as a table clause",
			clause: "ORDER BY (a, b) PRIMARY KEY a",
			option: "PRIMARY_KEY",
			want:   "a",
		},
		{
			name:   "SAMPLE BY",
			clause: "ORDER BY a SAMPLE BY a",
			option: "SAMPLE_BY",
			want:   "a",
		},
		{
			name:   "SETTINGS",
			clause: "ORDER BY a SETTINGS index_granularity = 8192",
			option: "SETTINGS",
			want:   "index_granularity = 8192",
		},
		{
			name:   "TTL, whose expression contains an identifier this parser also uses as a keyword",
			clause: "ORDER BY a TTL d + INTERVAL 1 DAY",
			option: "TTL",
			want:   "d + INTERVAL 1 DAY",
		},
		{
			name:   "PARTITION BY an expression is ClickHouse, not a PostgreSQL strategy",
			clause: "PARTITION BY toYYYYMM(d) ORDER BY a",
			option: "PARTITION_BY",
			want:   "toYYYYMM(d)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			table := parseOneTable(c,
				"CREATE TABLE t (a Int32 NOT NULL, b Int32 NOT NULL, d Date NOT NULL) ENGINE = MergeTree "+
					tt.clause+";")

			c.Assert(table.Options[tt.option], qt.Equals, tt.want)
		})
	}
}

func TestParserKeepsPostgreSQLPartitioningSeparate(t *testing.T) {
	tests := []struct {
		name     string
		clause   string
		wantType string
	}{
		{name: "RANGE", clause: "PARTITION BY RANGE (d)", wantType: "RANGE"},
		{name: "LIST", clause: "PARTITION BY LIST (d)", wantType: "LIST"},
		{name: "HASH", clause: "PARTITION BY HASH (d)", wantType: "HASH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// The two dialects spell PARTITION BY identically and mean
			// different things. The strategy keyword decides, so a PostgreSQL
			// partitioned table must still produce a partition spec and no
			// ClickHouse option.
			table := parseOneTable(c, "CREATE TABLE t (d date NOT NULL) "+tt.clause+";")

			c.Assert(table.Partition, qt.IsNotNil)
			c.Assert(table.Partition.Type, qt.Equals, tt.wantType)
			c.Assert(table.Options["PARTITION_BY"], qt.Equals, "")
		})
	}
}

func TestParserRefusesTwoClickHousePartitionClauses(t *testing.T) {
	c := qt.New(t)

	// The PostgreSQL form already refused a duplicate; the expression form
	// reaches a different branch and would otherwise overwrite in silence.
	_, err := parser.NewParser(
		"CREATE TABLE t (d Date NOT NULL) ENGINE = MergeTree PARTITION BY toYYYYMM(d) " +
			"PARTITION BY toYYYY(d) ORDER BY d;").Parse()

	c.Assert(err, qt.ErrorMatches, `.*duplicate PARTITION BY clause.*`)
}

func TestParserRefusesAClauseWithNoExpression(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "ORDER BY nothing",
			sql:  "CREATE TABLE t (a Int32 NOT NULL) ENGINE = MergeTree ORDER BY;",
		},
		{
			name: "SETTINGS nothing",
			sql:  "CREATE TABLE t (a Int32 NOT NULL) ENGINE = MergeTree ORDER BY a SETTINGS;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// An accepted-but-empty clause is the failure mode a parse-only
			// check cannot see: the table would render without the clause it
			// was written with.
			_, err := parser.NewParser(tt.sql).Parse()

			c.Assert(err, qt.ErrorMatches, `.*expected an expression after.*`)
		})
	}
}

// renderClickHouse parses sqlText as ClickHouse and renders it back.
func renderClickHouse(c *qt.C, sqlText string) string {
	c.Helper()

	statements, err := parser.NewParser(sqlText, parser.WithDialect(platform.ClickHouse)).Parse()
	c.Assert(err, qt.IsNil)
	database := toschema.ToDatabase(statements, platform.ClickHouse)
	goschema.Finalize(&database)

	rendered, err := renderer.GetOrderedCreateStatements(&database, platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.HasLen, 1)
	return rendered[0]
}

func TestMergeTreeClausesSurviveARoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		table string
	}{
		{
			name: "every clause at once",
			table: "CREATE TABLE t (a Int32 NOT NULL, b Int32 NOT NULL, d Date NOT NULL) " +
				"ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY (a, b) SAMPLE BY a " +
				"TTL d + INTERVAL 1 DAY SETTINGS index_granularity = 8192;",
		},
		{
			name: "the sorting key alone",
			table: "CREATE TABLE t (a Int32 NOT NULL) " +
				"ENGINE = MergeTree ORDER BY a;",
		},
		{
			name: "an empty sorting key",
			table: "CREATE TABLE t (a Int32 NOT NULL) " +
				"ENGINE = MergeTree ORDER BY tuple();",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Rendering twice is what a parse-only assertion cannot do: a
			// parser that accepts a clause and drops it parses cleanly, and
			// the loss shows only when the table is written back out.
			first := renderClickHouse(c, tt.table)
			second := renderClickHouse(c, first)

			c.Assert(second, qt.Equals, first)
			c.Assert(first, qt.Contains, "ENGINE = MergeTree")
		})
	}
}

func TestARoundTripKeepsEachClauseItWasGiven(t *testing.T) {
	tests := []struct {
		name   string
		clause string
	}{
		{name: "PARTITION BY", clause: "PARTITION BY toYYYYMM(d)"},
		{name: "ORDER BY", clause: "ORDER BY (a, b)"},
		{name: "SAMPLE BY", clause: "SAMPLE BY a"},
		{name: "TTL", clause: "TTL d + INTERVAL 1 DAY"},
		{name: "SETTINGS", clause: "SETTINGS index_granularity = 8192"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// The all-at-once row above would still pass if two clauses were
			// dropped together, because it only compares one render to the
			// next. Each clause is named here so a single lost one fails by
			// its own name.
			rendered := renderClickHouse(c,
				"CREATE TABLE t (a Int32 NOT NULL, b Int32 NOT NULL, d Date NOT NULL) "+
					"ENGINE = MergeTree ORDER BY (a, b) "+tt.clause+";")

			c.Assert(rendered, qt.Contains, tt.clause)
		})
	}
}

func TestClickHouseColumnsAreNotNullUnlessTheTypeSaysOtherwise(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		column       string
		wantNullable bool
	}{
		{
			name:         "a bare type is NOT NULL on ClickHouse",
			dialect:      platform.ClickHouse,
			column:       "a Int32",
			wantNullable: false,
		},
		{
			name:         "Nullable(T) is nullable",
			dialect:      platform.ClickHouse,
			column:       "a Nullable(Int32)",
			wantNullable: true,
		},
		{
			name:         "the marker nests",
			dialect:      platform.ClickHouse,
			column:       "a LowCardinality(Nullable(String))",
			wantNullable: true,
		},
		{
			name:         "an explicit NULL still wins",
			dialect:      platform.ClickHouse,
			column:       "a Int32 NULL",
			wantNullable: true,
		},
		{
			// The convention is ClickHouse's alone. Applying it anywhere else
			// would silently make every unannotated column NOT NULL.
			name:         "a bare type is nullable on PostgreSQL",
			dialect:      platform.Postgres,
			column:       "a integer",
			wantNullable: true,
		},
		{
			name:         "a bare type is nullable with no dialect at all",
			dialect:      "",
			column:       "a integer",
			wantNullable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(
				"CREATE TABLE t ("+tt.column+");",
				parser.WithDialect(tt.dialect),
			).Parse()
			c.Assert(err, qt.IsNil)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 1)

			c.Assert(table.Columns[0].Nullable, qt.Equals, tt.wantNullable)
		})
	}
}
