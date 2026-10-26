package routinesetting_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routinesetting"
)

// TestSplit_SpellsACatalogValueInSQL pins the reader's spelling of each
// proconfig entry, which is what pg_get_functiondef prints, measured on
// PostgreSQL 15.17 and 18.6: a list setting's elements each as a literal, an
// element the catalog quoted as a name read without its quotes, and any other
// value as one literal. Written back unquoted, `SET work_mem = 8MB` is refused
// with "trailing junk after numeric literal".
func TestSplit_SpellsACatalogValueInSQL(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		want    []string
	}{
		{name: "a memory size", catalog: "work_mem=8MB", want: []string{"work_mem='8MB'"}},
		{name: "a time", catalog: "statement_timeout=5s", want: []string{"statement_timeout='5s'"}},
		{name: "a number", catalog: "work_mem=8192", want: []string{"work_mem='8192'"}},
		{name: "a value with a quote and a space", catalog: "application_name=it's me", want: []string{"application_name='it''s me'"}},
		{name: "a list input that is one literal", catalog: "DateStyle=ISO, MDY", want: []string{"DateStyle='ISO, MDY'"}},
		{name: "a search path", catalog: "search_path=public, pg_catalog", want: []string{"search_path='public', 'pg_catalog'"}},
		{name: "a search path with quoted names", catalog: `search_path=a, "B", "$user", "with space", "say ""hi"""`,
			want: []string{`search_path='a', 'B', '$user', 'with space', 'say "hi"'`}},
		{name: "an empty search path", catalog: `search_path=""`, want: []string{"search_path=''"}},
		{name: "two entries", catalog: "work_mem=8MB\nsearch_path=pg_catalog", want: []string{"work_mem='8MB'", "search_path='pg_catalog'"}},
		{name: "nothing", catalog: "", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := routinesetting.Split(test.catalog)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestComparable_ReadsADeclarationAndACatalogRowAlike pins which spellings
// compare equal: each pair is a declaration and what the reader spells the
// catalog's row as. A literal and a quoted name compare by their contents, a
// bare word in lower case as the server folds it, and a name without case.
func TestComparable_ReadsADeclarationAndACatalogRowAlike(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		read     string
	}{
		{name: "a quoted memory size", declared: "work_mem='8MB'", read: "work_mem='8MB'"},
		{name: "a bare number", declared: "work_mem=8192", read: "work_mem='8192'"},
		{name: "a quoted time", declared: "statement_timeout='5s'", read: "statement_timeout='5s'"},
		{name: "a bare search path", declared: "search_path=public,pg_catalog", read: "search_path='public', 'pg_catalog'"},
		{name: "a search path of literals", declared: "search_path='a', 'B'", read: "search_path='a', 'B'"},
		{name: "a search path with a quoted name", declared: `search_path="My Schema",public`, read: "search_path='My Schema', 'public'"},
		{name: "a bare word the server folds", declared: "enable_seqscan=OFF", read: "enable_seqscan='off'"},
		{name: "a name in another case", declared: "datestyle=ISO,MDY", read: "DateStyle='iso, mdy'"},
		{name: "a value with a space", declared: "application_name='it''s me'", read: "application_name='it''s me'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared := routinesetting.Comparable(test.declared)
			read := routinesetting.Comparable(test.read)

			c.Assert(declared, qt.Equals, read)
		})
	}
}

// TestComparable_TellsTwoValuesApart pins the pairs that stay different: a
// changed value, a name a bare word folds to lower case, and a value a
// literal keeps in its case.
func TestComparable_TellsTwoValuesApart(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
	}{
		{name: "two sizes", left: "work_mem='8MB'", right: "work_mem='16MB'"},
		{name: "a bare word and a literal in upper case", left: "search_path=B", right: "search_path='B'"},
		{name: "two search paths", left: "search_path='a', 'b'", right: "search_path='b', 'a'"},
		{name: "two settings", left: "work_mem='8MB'", right: "maintenance_work_mem='8MB'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			left := routinesetting.Comparable(test.left)
			right := routinesetting.Comparable(test.right)

			c.Assert(left, qt.Not(qt.Equals), right)
		})
	}
}

// TestNormalize_KeepsTheSpaceInsideAQuotedPart pins the fold a declaration is
// kept in: the white space between a value's parts goes, and the white space
// inside a literal or a quoted name is the value's own.
func TestNormalize_KeepsTheSpaceInsideAQuotedPart(t *testing.T) {
	tests := []struct {
		name    string
		setting string
		want    string
	}{
		{name: "a list", setting: " search_path = pg_catalog, pg_temp ", want: "search_path=pg_catalog,pg_temp"},
		{name: "a literal with a space", setting: "application_name = 'it''s me'", want: "application_name='it''s me'"},
		{name: "a quoted name with a space", setting: `search_path = "My Schema", public`, want: `search_path="My Schema",public`},
		{name: "from current", setting: "search_path FROM CURRENT", want: "search_path FROM CURRENT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := routinesetting.Normalize(test.setting)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
