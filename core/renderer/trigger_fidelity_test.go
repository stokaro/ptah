package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
)

// statementTrigger is a FOR EACH STATEMENT trigger whose body is spelled the
// natural way for a SQL statement: ending in a semicolon.
func statementTrigger() *ast.CreateTriggerNode {
	return ast.NewCreateTrigger("stmt_trg", "t1").
		SetTiming("AFTER").
		SetEvent("INSERT").
		SetForEach("STATEMENT").
		SetBody("INSERT INTO audit VALUES (1);")
}

// rowTrigger is the same trigger at row level, so a dialect that refuses
// statement level still renders something for the semicolon assertions.
func rowTrigger() *ast.CreateTriggerNode {
	return ast.NewCreateTrigger("row_trg", "t1").
		SetTiming("AFTER").
		SetEvent("INSERT").
		SetForEach("ROW").
		SetBody("INSERT INTO audit VALUES (1);")
}

func beforeTrigger() *ast.CreateTriggerNode {
	return ast.NewCreateTrigger("bef_trg", "t1").
		SetTiming("BEFORE").
		SetEvent("INSERT").
		SetForEach("ROW").
		SetBody("INSERT INTO audit VALUES (1)")
}

// TestRenderTrigger_LevelAndTimingAreRefusedNotDowngraded pins that a dialect
// which cannot express the trigger the author declared says so.
//
// MySQL and MariaDB hard-coded "FOR EACH ROW" into the format string, so a
// FOR EACH STATEMENT trigger rendered as a row-level trigger -- a body that runs
// once per statement instead became one that runs per affected row, with no
// diagnostic. SQL Server rewrote timing BEFORE to AFTER unconditionally, moving
// the body from ahead of the write to behind it. SQLite already refused the same
// input, and that is the answer the other two now give (stokaro/ptah#931 item 4).
func TestRenderTrigger_LevelAndTimingAreRefusedNotDowngraded(t *testing.T) {

	tests := []struct {
		name    string
		dialect string
		node    *ast.CreateTriggerNode
		want    string
	}{
		{name: "mysql statement level", dialect: "mysql", node: statementTrigger(), want: ".*FOR EACH STATEMENT triggers are not supported.*"},
		{name: "mariadb statement level", dialect: "mariadb", node: statementTrigger(), want: ".*FOR EACH STATEMENT triggers are not supported.*"},
		{name: "sqlite statement level", dialect: "sqlite", node: statementTrigger(), want: ".*FOR EACH STATEMENT triggers are not supported.*"},
		{name: "sqlserver before timing", dialect: "sqlserver", node: beforeTrigger(), want: ".*BEFORE triggers are not supported.*"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRenderTrigger_PostgreSQLKeepsWhatTheAuthorDeclared is the non-interference
// control for the refusals above: PostgreSQL can express both, so both must
// still render unchanged. Without this row, deleting trigger rendering entirely
// would pass the test above.
func TestRenderTrigger_PostgreSQLKeepsWhatTheAuthorDeclared(t *testing.T) {

	tests := []struct {
		name string
		node *ast.CreateTriggerNode
		want string
	}{
		{name: "statement level survives", node: statementTrigger(), want: "FOR EACH STATEMENT"},
		{name: "before timing survives", node: beforeTrigger(), want: "BEFORE INSERT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQL("postgres", test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderTrigger_BodyEndingInSemicolonGetsExactlyOne pins that a body already
// terminated by ";" is not terminated a second time.
//
// The MySQL/MariaDB and SQL Server renderers appended ";" unconditionally, so
// the natural spelling of a body produced `... VALUES (1);;`
// (stokaro/ptah#931 item 6).
func TestRenderTrigger_BodyEndingInSemicolonGetsExactlyOne(t *testing.T) {

	for _, dialect := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQL(dialect, rowTrigger())

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "INSERT INTO audit VALUES (1);")
			c.Assert(sql, qt.Not(qt.Contains), ";;")
		})
	}
}

// TestRenderTrigger_BodyWithoutSemicolonStillGetsOne is the inverse control: the
// terminator is added when the body lacks it, so the fix above is a conditional
// and not a deletion.
func TestRenderTrigger_BodyWithoutSemicolonStillGetsOne(t *testing.T) {

	node := ast.NewCreateTrigger("row_trg", "t1").
		SetTiming("AFTER").
		SetEvent("INSERT").
		SetForEach("ROW").
		SetBody("INSERT INTO audit VALUES (1)")

	for _, dialect := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQL(dialect, node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "INSERT INTO audit VALUES (1);")
		})
	}
}
