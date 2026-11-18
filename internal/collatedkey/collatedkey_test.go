package collatedkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/collatedkey"
)

// TestRead_HappyPath reads the spellings SQLite accepts for one collated key.
// Each is the same key to sqlite3 3.51.0, which reports column code and
// collation NOCASE in pragma_index_xinfo for all of them, and the community
// binary v1.3.0 reports a database holding any of them synced with a document
// that writes `code COLLATE NOCASE`.
func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		element string
		want    collatedkey.Key
	}{
		{name: "bare", element: "code COLLATE NOCASE", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "double-quoted", element: `"code" COLLATE NOCASE`, want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "bracketed", element: "[code] COLLATE NOCASE", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "backticked", element: "`code` COLLATE NOCASE", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "a comment and spacing", element: "code  COLLATE /*x*/ NOCASE\n", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "ascending", element: "code COLLATE NOCASE ASC", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "descending", element: `"code" COLLATE NOCASE DESC`, want: collatedkey.Key{Text: "code COLLATE NOCASE", Desc: true}},
		{name: "a name with a space", element: "[my code] COLLATE NOCASE", want: collatedkey.Key{Text: `"my code" COLLATE NOCASE`}},
		{name: "a quoted name with a quote", element: `"a""b" COLLATE NOCASE`, want: collatedkey.Key{Text: `"a""b" COLLATE NOCASE`}},
		{name: "a keyword", element: `"order" COLLATE NOCASE`, want: collatedkey.Key{Text: `"order" COLLATE NOCASE`}},
		{name: "a quoted collation", element: `code COLLATE "NOCASE"`, want: collatedkey.Key{Text: `code COLLATE "NOCASE"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := collatedkey.Read("sqlite", tt.element)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestRead_FailurePath leaves alone every element that is not one column under
// one COLLATE, and every element of a dialect where quoting a plain name can
// change the column it names: PostgreSQL reads `Code` as code and `"Code"` as
// Code.
func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		element string
	}{
		{name: "a column", dialect: "sqlite", element: "code"},
		{name: "a column that descends", dialect: "sqlite", element: "code DESC"},
		{name: "an expression", dialect: "sqlite", element: "lower(code) COLLATE NOCASE"},
		{name: "no collation", dialect: "sqlite", element: "code COLLATE"},
		{name: "a word after the direction", dialect: "sqlite", element: "code COLLATE NOCASE DESC x"},
		{name: "a word for a direction", dialect: "sqlite", element: "code COLLATE NOCASE x"},
		{name: "a string literal", dialect: "sqlite", element: "'code' COLLATE NOCASE"},
		{name: "a number", dialect: "sqlite", element: "1 COLLATE NOCASE"},
		{name: "a string literal for the collation", dialect: "sqlite", element: "code COLLATE 'NOCASE'"},
		{name: "a folding dialect", dialect: "postgres", element: `"code" COLLATE "C"`},
		{name: "an unknown dialect", dialect: "nosuch", element: "code COLLATE NOCASE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := collatedkey.Read(tt.dialect, tt.element)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, collatedkey.Key{})
		})
	}
}
