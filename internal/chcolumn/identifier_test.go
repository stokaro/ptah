package chcolumn_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/sqlident"
)

func TestUnquoteIdentifier_HappyPath(t *testing.T) {
	for _, test := range []struct {
		identifier, want string
	}{
		{identifier: "id", want: "id"},
		{identifier: "`id`", want: "id"},
		{identifier: "`sl\\\\x`", want: `sl\x`},
		{identifier: "`a\\`b`", want: "a`b"},
		{identifier: "`a``b`", want: "a`b"},
		{identifier: "`new\\nline`", want: "new\nline"},
		{identifier: "`tab\\tx\\rcr\\bb\\ff\\0z`", want: "tab\tx\rcr\bb\ff\x00z"},
		{identifier: "`\\x41\\'q`", want: "A'q"},
		{identifier: `"double ""quoted"""`, want: `double "quoted"`},
		{identifier: "`é`", want: "é"},
	} {
		t.Run(test.identifier, func(t *testing.T) {
			c := qt.New(t)
			got, ok := chcolumn.UnquoteIdentifier(test.identifier)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestUnquoteIdentifier_FailurePath(t *testing.T) {
	for _, identifier := range []string{"", "`", "`open", "`ends\\`", "`lone`quote`", "`\\x4`", "`\\xZZ`"} {
		t.Run(identifier, func(t *testing.T) {
			c := qt.New(t)
			got, ok := chcolumn.UnquoteIdentifier(identifier)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// A name sqlident quotes for ClickHouse reads back as itself. The renderer
// writes every name with sqlident.Quote and the reader, the rebuild and the
// key comparison read names with UnquoteIdentifier, so the two ends have to
// agree on every byte; a writer that stopped doubling the backslash turns
// `sl\x` into a malformed escape here.
func TestUnquoteIdentifier_ReadsWhatSQLIdentWrites(t *testing.T) {
	for _, name := range []string{"t", "we'ird", `sl\x`, "a`b", "new\nline", `\`, "``", `a\tb`, "c -- d", `\` + "`"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := chcolumn.UnquoteIdentifier(sqlident.Quote(platform.ClickHouse, name))
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, name)
		})
	}
}

// TestComparableIdentifier pins the spelling a key comparison reads a name in.
// The server prints a quoted word bare and quotes the rest in its own way
// (measured on 26.10: a key over id, not and a`b, each written in backticks,
// reads back as id, not, `a\`b`), so both spellings of one name have to compare
// the same, and a word the server reads as a literal keeps its quotes.
func TestComparableIdentifier(t *testing.T) {
	for _, test := range []struct {
		identifier, want string
	}{
		{identifier: "id", want: "id"},
		{identifier: "`id`", want: "id"},
		{identifier: `"id"`, want: "id"},
		{identifier: "`not`", want: "not"},
		{identifier: "`a``b`", want: "`a``b`"},
		{identifier: "`a\\`b`", want: "`a``b`"},
		{identifier: "`c -- d`", want: "`c -- d`"},
		{identifier: "`a\\\\b`", want: "`a\\\\b`"},
		{identifier: "`null`", want: "`null`"},
		{identifier: "`True`", want: "`True`"},
		{identifier: "`1a`", want: "`1a`"},
		{identifier: "'text'", want: "'text'"},
		{identifier: "`open", want: "`open"},
	} {
		t.Run(test.identifier, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chcolumn.ComparableIdentifier(test.identifier), qt.Equals, test.want)
		})
	}
}

func TestCheckName(t *testing.T) {
	for _, test := range []struct {
		name, declared, table, column, want string
	}{
		{name: "declared", declared: "positive", table: "t", column: "x", want: "positive"},
		{name: "column check", table: "t", column: "x", want: "t_x_check"},
		{name: "table check", table: "t", want: "t_check"},
		{name: "qualified table", table: "db.t", column: "x", want: "t_x_check"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chcolumn.CheckName(test.declared, test.table, test.column), qt.Equals, test.want)
		})
	}
}
