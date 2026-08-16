package parser_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
)

// renderClickHouseSchema parses sqlText as ClickHouse and renders every
// statement back. It differs from renderClickHouse only in accepting more than
// one: a table plus the indexes declared on it.
func renderClickHouseSchema(c *qt.C, sqlText string) string {
	c.Helper()

	statements, err := parser.NewParser(sqlText, parser.WithDialect(platform.ClickHouse)).Parse()
	c.Assert(err, qt.IsNil)
	database := toschema.ToDatabase(statements, platform.ClickHouse)
	goschema.Finalize(&database)

	rendered, err := renderer.GetOrderedCreateStatements(&database, platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	return strings.Join(rendered, "\n") + "\n"
}

// The ClickHouse renderer writes a data-skipping index as
// `ALTER TABLE t ADD INDEX name expression TYPE type GRANULARITY n`
// (VisitIndex, core/renderer/internal/dialects/clickhouse). The parser read
// that as an ADD COLUMN, taking the indexed expression for a type and TYPE for
// a column attribute, so Ptah refused a statement Ptah wrote
// (stokaro/ptah#1574).

func parseOneAlter(c *qt.C, sql string) *ast.AlterTableNode {
	c.Helper()

	statements, err := parser.NewParser(sql, parser.WithDialect(platform.ClickHouse)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alter.Operations, qt.HasLen, 1)
	return alter
}

func TestParserReadsAnAddedSkippingIndex(t *testing.T) {
	tests := []struct {
		name            string
		statement       string
		wantExpression  string
		wantType        string
		wantGranularity int
	}{
		{
			name:            "a column expression",
			statement:       "ALTER TABLE t ADD INDEX idx_b b TYPE minmax GRANULARITY 1;",
			wantExpression:  "b",
			wantType:        "minmax",
			wantGranularity: 1,
		},
		{
			// The type carries its own parameters, and the parentheses are
			// part of it. A type rebuilt from tokens loses them.
			name:            "a parameterised type",
			statement:       "ALTER TABLE t ADD INDEX idx_b b TYPE set(100) GRANULARITY 4;",
			wantExpression:  "b",
			wantType:        "set(100)",
			wantGranularity: 4,
		},
		{
			name:            "a type with several parameters",
			statement:       "ALTER TABLE t ADD INDEX idx_b b TYPE tokenbf_v1(256, 2, 0) GRANULARITY 2;",
			wantExpression:  "b",
			wantType:        "tokenbf_v1(256, 2, 0)",
			wantGranularity: 2,
		},
		{
			// The indexed expression is arbitrary, and a tuple is not a column
			// list: splitting it would name columns that do not exist.
			name:            "a tuple expression",
			statement:       "ALTER TABLE t ADD INDEX idx_ab (a, b) TYPE minmax GRANULARITY 1;",
			wantExpression:  "(a, b)",
			wantType:        "minmax",
			wantGranularity: 1,
		},
		{
			name:            "a function-call expression",
			statement:       "ALTER TABLE t ADD INDEX idx_low lower(b) TYPE bloom_filter(0.01) GRANULARITY 3;",
			wantExpression:  "lower(b)",
			wantType:        "bloom_filter(0.01)",
			wantGranularity: 3,
		},
		{
			// GRANULARITY is optional; the renderer supplies ClickHouse's
			// documented default for a zero.
			name:            "no granularity",
			statement:       "ALTER TABLE t ADD INDEX idx_b b TYPE minmax;",
			wantExpression:  "b",
			wantType:        "minmax",
			wantGranularity: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			alter := parseOneAlter(c, tt.statement)
			operation, ok := alter.Operations[0].(*ast.AddSkippingIndexOperation)
			c.Assert(ok, qt.IsTrue)

			c.Assert(operation.Expression, qt.Equals, tt.wantExpression)
			c.Assert(operation.IndexType, qt.Equals, tt.wantType)
			c.Assert(operation.Granularity, qt.Equals, tt.wantGranularity)
		})
	}
}

func TestParserRefusesAMalformedSkippingIndex(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "no expression",
			statement: "ALTER TABLE t ADD INDEX idx_b TYPE minmax GRANULARITY 1;",
			wantErr:   `.*expected an indexed expression after ADD INDEX idx_b.*`,
		},
		{
			name:      "no TYPE",
			statement: "ALTER TABLE t ADD INDEX idx_b b GRANULARITY 1;",
			wantErr:   `.*expected TYPE after the expression of index idx_b.*`,
		},
		{
			name:      "a granularity that is not a number",
			statement: "ALTER TABLE t ADD INDEX idx_b b TYPE minmax GRANULARITY wide;",
			wantErr:   `.*GRANULARITY on index idx_b must be a number.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(tt.statement, parser.WithDialect(platform.ClickHouse)).Parse()

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}

func TestParserStillReadsAnOrdinaryAddColumn(t *testing.T) {
	c := qt.New(t)

	// The branch is entered on the INDEX keyword alone, so an ADD COLUMN --
	// including one whose column happens to be named index-something -- must
	// still take the column path.
	alter := parseOneAlter(c, "ALTER TABLE t ADD COLUMN index_hint Int32;")
	operation, ok := alter.Operations[0].(*ast.AddColumnOperation)

	c.Assert(ok, qt.IsTrue)
	c.Assert(operation.Column.Name, qt.Equals, "index_hint")
}

func TestASkippingIndexSurvivesARoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		index string
	}{
		{name: "minmax", index: "ADD INDEX `idx_b` b TYPE minmax GRANULARITY 1;"},
		{name: "set", index: "ADD INDEX `idx_b` b TYPE set(100) GRANULARITY 4;"},
		{name: "bloom_filter on an expression", index: "ADD INDEX `idx_low` lower(b) TYPE bloom_filter(0.01) GRANULARITY 3;"},
		{name: "a tuple", index: "ADD INDEX `idx_ab` (a, b) TYPE minmax GRANULARITY 1;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Rendering twice is the assertion a parse-only check cannot make:
			// the index parsed cleanly and was then dropped on the way to
			// goschema, which showed only when the schema was written back.
			source := "CREATE TABLE t (a Int32 NOT NULL, b Int32 NOT NULL) ENGINE = MergeTree ORDER BY a;\n" +
				"ALTER TABLE `t` " + tt.index + "\n"
			first := renderClickHouseSchema(c, source)
			second := renderClickHouseSchema(c, first)

			c.Assert(second, qt.Equals, first)
			c.Assert(first, qt.Contains, tt.index)
		})
	}
}
