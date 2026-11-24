package tableref_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/tableref"
)

// A reader that trims what an author typed removes white space beside a bare
// part, so that space is reported; beside a quote it is the spelling of the
// value, and is not.
func TestBareSpace(t *testing.T) {
	tests := []struct {
		name, value string
		want        bool
	}{
		{name: "a bare name", value: "t", want: false},
		{name: "a leading space", value: " t", want: true},
		{name: "a trailing space", value: "t ", want: true},
		{name: "a trailing tab", value: "t\t", want: true},
		{name: "a quoted name", value: `" t"`, want: false},
		{name: "space around a quoted name", value: ` " t" `, want: false},
		{name: "a leading space before a bare schema", value: ` a."t"`, want: true},
		{name: "a trailing space after a bare name", value: `"a".t `, want: true},
		{name: "a closing quote the reader does not read", value: "t` ", want: false},
		{name: "white space alone", value: "  ", want: false},
		{name: "empty", value: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tableref.BareSpace(test.value, "\"`[", "\"`]"), qt.Equals, test.want)
		})
	}
}

// Only the quotes a reader names make a part quoted: a reader that reads
// double quotes alone reports the space beside a bracket.
func TestBareSpace_ReadsOnlyTheQuotesItIsGiven(t *testing.T) {
	c := qt.New(t)
	c.Assert(tableref.BareSpace(" [t]", `"`, `"`), qt.IsTrue)
	c.Assert(tableref.BareSpace(" [t]", "\"`[", "\"`]"), qt.IsFalse)
}

func TestSpacedBareName(t *testing.T) {
	c := qt.New(t)

	err := tableref.SpacedBareName("table name", " t")

	c.Assert(err, qt.ErrorIs, tableref.ErrSpacedBareName)
	c.Assert(err, qt.ErrorMatches, `table name " t" begins or ends with white space, which a bare name loses: `+
		`write it as one quoted identifier, " t", or remove the space`)
}
