package parser_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// tableChecks describes where a table's CHECKs landed: the one its first column
// carries, then every table-level CHECK in the order the body recorded them,
// each as `<name>: <expression>`.
func tableChecks(table *ast.CreateTableNode) []string {
	checks := []string{fmt.Sprintf("column %s: %s", table.Columns[0].CheckName, table.Columns[0].Check)}
	for _, element := range table.Elements {
		checks = append(checks, fmt.Sprintf("table %s: %s", element.Constraint.Name, element.Constraint.Expression))
	}
	return checks
}

// TestParse_EveryCheckOnAColumnIsKept_HappyPath reads a column carrying more
// than one CHECK. PostgreSQL 18.6 creates one constraint for each: `CREATE
// TABLE h2 (a int CHECK (a > 0) CHECK (a < 10))` holds `h2_a_check` and
// `h2_a_check1`. The model keeps one CHECK on a column, so the first stays
// there and each later one is read as a table-level CHECK, in order and under
// the name written in front of it. Kept on the column, the second would
// replace the first.
func TestParse_EveryCheckOnAColumnIsKept_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "two unnamed",
			sql:  "CREATE TABLE h2 (a int CHECK (a > 0) CHECK (a < 10));",
			want: []string{"column : a > 0", "table : a < 10"},
		},
		{
			name: "named, then unnamed",
			sql:  "CREATE TABLE h3 (a int CONSTRAINT h3_named CHECK (a > 0) CHECK (a < 10));",
			want: []string{"column h3_named: a > 0", "table : a < 10"},
		},
		{
			name: "unnamed, then named",
			sql:  "CREATE TABLE h4 (a int CHECK (a > 0) CONSTRAINT h4_named CHECK (a < 10));",
			want: []string{"column : a > 0", "table h4_named: a < 10"},
		},
		{
			name: "three, before a table-level CHECK",
			sql:  "CREATE TABLE h5 (a int CHECK (a > 0) CHECK (a < 10) CHECK (a <> 5), CHECK (a <> 7));",
			want: []string{"column : a > 0", "table : a < 10", "table : a <> 5", "table : a <> 7"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(tableChecks(statements.Statements[0].(*ast.CreateTableNode)), qt.DeepEquals, test.want)
		})
	}
}

// TestParse_EveryCheckOnAColumnIsKept_FailurePath refuses a second CHECK on a
// column ALTER TABLE adds or modifies. The statement has no table body to read
// it into, and keeping one of the two would drop the other in silence.
func TestParse_EveryCheckOnAColumnIsKept_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "ADD COLUMN",
			dialect: "postgres",
			sql:     "ALTER TABLE t ADD COLUMN a int CHECK (a > 0) CHECK (a < 10);",
			wantErr: `(?s).*column a carries a second CHECK at position \d+: a column added or modified by ` +
				`ALTER TABLE keeps one CHECK; add the next with ALTER TABLE \.\.\. ADD CHECK.*`,
		},
		{
			name:    "MODIFY COLUMN",
			dialect: "mysql",
			sql:     "ALTER TABLE t MODIFY COLUMN a int CHECK (a > 0) CHECK (a < 10);",
			wantErr: `(?s).*column a carries a second CHECK at position \d+: a column added or modified by ` +
				`ALTER TABLE keeps one CHECK; add the next with ALTER TABLE \.\.\. ADD CHECK.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
