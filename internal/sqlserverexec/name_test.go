package sqlserverexec_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlserverexec"
)

// TestSplitName_HappyPath reads each spelling of a name the server reads from
// a DBCC CHECKIDENT string; each one reseeded the table it names on SQL Server
// 2025.
func TestSplitName_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "a table alone", text: "items", want: []string{"items"}},
		{name: "a schema and a table", text: "dbo.items", want: []string{"dbo", "items"}},
		{name: "a database, a schema and a table", text: "o6688.dbo.ident", want: []string{"o6688", "dbo", "ident"}},
		{name: "the schema left out", text: "o6688..ident", want: []string{"o6688", "", "ident"}},
		{name: "bracketed parts", text: "[o6688].[dbo].[ident]", want: []string{"o6688", "dbo", "ident"}},
		{name: "quoted parts", text: `"dbo"."items"`, want: []string{"dbo", "items"}},
		{name: "a doubled closing bracket", text: "[a]]b].[c.d]", want: []string{"a]b", "c.d"}},
		{name: "a doubled quote", text: `"a""b".c`, want: []string{`a"b`, "c"}},
		{name: "the database left out", text: ".dbo.items", want: []string{"", "dbo", "items"}},
		{name: "a name with @ inside", text: "x@.dbo.kept", want: []string{"x@", "dbo", "kept"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parts, ok := sqlserverexec.SplitName(test.text)

			c.Assert(ok, qt.IsTrue)
			c.Assert(parts, qt.DeepEquals, test.want)
		})
	}
}

// TestSplitName_FailurePath holds text the splitter cannot read as a name, so
// a caller refuses it rather than guess the database it names.
func TestSplitName_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "an empty last part", text: "dbo."},
		{name: "a space beside a dot", text: "dbo . items"},
		{name: "a leading space", text: " dbo.items"},
		{name: "an unclosed bracket", text: "[dbo.items"},
		{name: "text after a bracketed part", text: "[dbo]x.items"},
		{name: "a variable", text: "@t"},
		{name: "a part that starts with a digit", text: "1x.items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parts, ok := sqlserverexec.SplitName(test.text)

			c.Assert(ok, qt.IsFalse)
			c.Assert(parts, qt.IsNil)
		})
	}
}
