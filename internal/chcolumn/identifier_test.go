package chcolumn_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/chcolumn"
)

// The quoted forms are what ClickHouse 24.10 and 26.10 print in SHOW CREATE
// TABLE for these names, measured, so a name the reader unquotes from the
// server's text quotes back to the same text.
func TestQuoteIdentifier(t *testing.T) {
	for _, test := range []struct {
		name, want string
	}{
		{name: "t", want: "`t`"},
		{name: "we'ird", want: "`we'ird`"},
		{name: `sl\x`, want: "`sl\\\\x`"},
		{name: "a`b", want: "`a\\`b`"},
		{name: "p q", want: "`p q`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chcolumn.QuoteIdentifier(test.name), qt.Equals, test.want)
		})
	}
}

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

// A name quoted by QuoteIdentifier reads back as itself, which is what lets a
// rebuild find a column by the name the reader took out of the server's text.
func TestUnquoteIdentifier_ReadsWhatQuoteIdentifierWrites(t *testing.T) {
	for _, name := range []string{"t", "we'ird", `sl\x`, "a`b", "new\nline", `\`, "``"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := chcolumn.UnquoteIdentifier(chcolumn.QuoteIdentifier(name))
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, name)
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
