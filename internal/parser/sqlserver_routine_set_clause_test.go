package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/parser"
)

// tsqlBodyStatements parses a T-SQL procedure and returns its body statements
// as "kind|sql" pairs, which is the whole of what the split decides.
func tsqlBodyStatements(c *qt.C, body string) []string {
	c.Helper()

	list, err := parser.NewParser("CREATE PROCEDURE p AS "+body, parser.WithDialect("sqlserver")).Parse()
	c.Assert(err, qt.IsNil)

	var pairs []string
	for _, statement := range list.Statements {
		node, ok := statement.(*ast.SQLServerRoutineNode)
		c.Assert(ok, qt.IsTrue)
		for _, inner := range node.Body.Statements {
			pairs = append(pairs, string(inner.Kind)+"|"+inner.SQL)
		}
	}
	return pairs
}

// TestParse_TSQLSetIsAClauseWhereItIsOne is the defect this file is about.
//
// SET is two different things in T-SQL: `SET @total = 0` is a statement, and
// the SET in `UPDATE t SET c = 1` is a clause. Every one was read as a
// statement, so an UPDATE came apart into `UPDATE t` and `SET c = 1` and
// neither half was an update -- invisible to anything reading a routine body
// for what it writes (stokaro/ptah#2451).
func TestParse_TSQLSetIsAClauseWhereItIsOne(t *testing.T) {
	rows := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "an update keeps its set clause",
			body: "UPDATE t SET c = 1;",
			want: []string{"raw|UPDATE t SET c = 1"},
		},
		{
			name: "a merge keeps one set clause per branch",
			body: "MERGE t USING s ON t.id = s.id WHEN MATCHED AND s.x = 1 THEN UPDATE SET c = 1 WHEN MATCHED THEN UPDATE SET c = 2;",
			want: []string{"raw|MERGE t USING s ON t.id = s.id WHEN MATCHED AND s.x = 1 THEN UPDATE SET c = 1 WHEN MATCHED THEN UPDATE SET c = 2"},
		},
		{
			name: "a subquery in the set clause stays inside it",
			body: "UPDATE t SET c = (SELECT max(v) FROM u) WHERE id = 1;",
			want: []string{"raw|UPDATE t SET c = (SELECT max(v) FROM u) WHERE id = 1"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got := tsqlBodyStatements(c, row.body)

			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}

// TestParse_TSQLSetIsStillAStatementWhereItIsOne is the control.
//
// A repair that swallowed every SET would pass the test above and lose the
// assignment statement, which is what the kind exists to name. These are the
// spellings that must keep splitting.
func TestParse_TSQLSetIsStillAStatementWhereItIsOne(t *testing.T) {
	rows := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "a bare assignment",
			body: "SET @total = 0;",
			want: []string{"assignment|SET @total = 0"},
		},
		{
			name: "assignments around an update",
			body: "BEGIN DECLARE @x INT; SET @x = 1; UPDATE t SET c = @x; SET @x = 2; END",
			want: []string{
				"declaration|DECLARE @x INT;",
				"assignment|SET @x = 1;",
				"raw|UPDATE t SET c = @x;",
				"assignment|SET @x = 2;",
			},
		},
		{
			name: "a second set after an update with no separator",
			body: "UPDATE t SET c = 1 SET @x = 2",
			want: []string{"raw|UPDATE t SET c = 1", "assignment|SET @x = 2"},
		},
		{
			// T-SQL lets a statement end without a semicolon, and this is how
			// most bodies are written. Only UPDATE and MERGE take a SET clause,
			// so a SET after anything else opens a statement whatever precedes
			// it.
			name: "a declaration and an assignment with no separator",
			body: "BEGIN DECLARE @x INT SET @x = 1 END",
			want: []string{"declaration|DECLARE @x INT", "assignment|SET @x = 1"},
		},
		{
			name: "a select and an assignment with no separator",
			body: "BEGIN SELECT 1 SET @x = 2 END",
			want: []string{"select|SELECT 1", "assignment|SET @x = 2"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got := tsqlBodyStatements(c, row.body)

			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}
