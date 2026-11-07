package routinesetting_test

import (
	"slices"
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
		{name: "a quoted name", declared: `"TimeZone"='UTC'`, read: "TimeZone='UTC'"},
		{name: "a dotted name with a quoted part", declared: `myapp."Mode"='on'`, read: "myapp.mode='on'"},
		{name: "a search path of one schema with a comma", declared: "search_path='a, b'", read: "search_path='a, b'"},
		{name: "a list input that is one value", declared: "DateStyle='ISO, MDY'", read: "DateStyle='ISO, MDY'"},
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
		{name: "one schema with a comma and two schemas", left: "search_path='a, b'", right: "search_path=a, b"},
		{name: "two schemas and one", left: "search_path='a', 'b'", right: "search_path='a, b'"},
		{name: "text after a literal", left: "application_name='a'b", right: "application_name='a'"},
		{name: "two time zones", left: `"TimeZone"='UTC'`, right: "TimeZone='Europe/Prague'"},
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

// TestComparableAll_ComparesTheSettingsAsASet pins that a list of settings
// compares by what the server stores: the order of the names is not part of
// it, since every setting applies before the body and the Atlas HCL reader
// sorts its `set`, and a name set twice stores its last value.
func TestComparableAll_ComparesTheSettingsAsASet(t *testing.T) {
	tests := []struct {
		name  string
		left  []string
		right []string
		equal bool
	}{
		{name: "another order", left: []string{"work_mem='8MB'", "search_path=public"},
			right: []string{"search_path='public'", "work_mem='8MB'"}, equal: true},
		{name: "a name set twice", left: []string{"work_mem='4MB'", "search_path=public", "work_mem='8MB'"},
			right: []string{"search_path='public'", "work_mem='8MB'"}, equal: true},
		{name: "a name set twice keeps the last value", left: []string{"work_mem='8MB'", "work_mem='4MB'"},
			right: []string{"work_mem='8MB'"}, equal: false},
		{name: "another value", left: []string{"work_mem='8MB'", "search_path=public"},
			right: []string{"search_path='public'", "work_mem='16MB'"}, equal: false},
		{name: "one setting fewer", left: []string{"work_mem='8MB'", "search_path=public"},
			right: []string{"work_mem='8MB'"}, equal: false},
		{name: "nothing and empty settings", left: nil, right: []string{" ", ""}, equal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			left := routinesetting.ComparableAll(test.left)
			right := routinesetting.ComparableAll(test.right)

			c.Assert(slices.Equal(left, right), qt.Equals, test.equal, qt.Commentf("%q and %q", left, right))
		})
	}
}

// TestToCatalog_IsTheSpellingFromCatalogReads pins the spelling a setting is
// written in where a document holds it as the server keeps it, as the Atlas
// HCL `set` does. Each entry is a proconfig value PostgreSQL 18.6 stores for a
// SET clause; spelled in SQL by [routinesetting.FromCatalog] and back, it is the
// entry the server stored.
func TestToCatalog_IsTheSpellingFromCatalogReads(t *testing.T) {
	entries := []string{
		`search_path=""`,
		`search_path="$user", public`,
		`search_path=myschema, public`,
		`search_path="MySchema", public`,
		`search_path=public, "Weird ""q"""`,
		`search_path="a, b"`,
		`search_path=public, pg_temp`,
		`temp_tablespaces=""`,
		`DateStyle=iso, mdy`,
		`DateStyle=ISO, MDY`,
		`application_name=it's, me`,
		`application_name=a\b`,
		`application_name= a `,
		`work_mem=8192`,
		`work_mem=8MB`,
		`enable_seqscan=off`,
		`random_page_cost=1.10`,
		`extra_float_digits=-1`,
		`TimeZone=Europe/Prague`,
		`default_text_search_config=pg_catalog.english`,
	}
	for _, entry := range entries {
		t.Run(entry, func(t *testing.T) {
			c := qt.New(t)

			got := routinesetting.ToCatalog(routinesetting.FromCatalog(entry))

			c.Assert(got, qt.Equals, entry)
		})
	}
}

// TestToCatalog_SpellsADeclarationAsTheServerStoresIt pins the catalog
// spelling of declarations: quotes come off a literal and a quoted name, a
// bare word folds to lower case, and a setting without a value is kept.
func TestToCatalog_SpellsADeclarationAsTheServerStoresIt(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "a literal", declared: "work_mem='8MB'", want: "work_mem=8MB"},
		{name: "a quoted name", declared: `"TimeZone"='UTC'`, want: "TimeZone=UTC"},
		{name: "a bare search path", declared: "search_path=MySchema,pg_catalog", want: "search_path=myschema, pg_catalog"},
		{name: "a search path of literals", declared: "search_path='$user', 'Public'", want: `search_path="$user", "Public"`},
		{name: "a list input that is one value", declared: "DateStyle=ISO,MDY", want: "DateStyle=iso, mdy"},
		{name: "from current", declared: "search_path FROM CURRENT", want: "search_path FROM CURRENT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := routinesetting.ToCatalog(test.declared)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestParseCatalog_HappyPath pins how an author's text in the catalog's spelling
// becomes SQL. The bare elements fold, as the server folds them when it reads a
// list back: on PostgreSQL 18, `set_config('search_path', 'MySchema, public',
// false)` searches myschema and `"MySchema", public` searches MySchema.
func TestParseCatalog_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		entry string
		want  string
	}{
		{name: "a unit", entry: "work_mem=8MB", want: "work_mem='8MB'"},
		{name: "a time zone with a slash", entry: "TimeZone=Europe/Prague", want: "TimeZone='Europe/Prague'"},
		{name: "a quote in a value that is not a list", entry: "application_name=it's", want: "application_name='it''s'"},
		{name: "a value that looks like a literal", entry: "application_name='x'", want: "application_name='''x'''"},
		{name: "white space around a value", entry: "application_name= a ", want: "application_name=' a '"},
		{name: "white space around a list", entry: "search_path= a , b ", want: "search_path='a', 'b'"},
		{name: "a list input that is one value", entry: "DateStyle=ISO, MDY", want: "DateStyle='ISO, MDY'"},
		{name: "two schemas", entry: "search_path=a, b", want: "search_path='a', 'b'"},
		{name: "one schema with a comma", entry: `search_path="a, b"`, want: "search_path='a, b'"},
		{name: "a bare name folds", entry: "search_path=MySchema, public", want: "search_path='myschema', 'public'"},
		{name: "a quoted name keeps its case", entry: `search_path="MySchema", public`, want: "search_path='MySchema', 'public'"},
		{name: "a name that is not an identifier", entry: `search_path="$user", public`, want: "search_path='$user', 'public'"},
		{name: "a doubled quote inside a name", entry: `search_path="a""b"`, want: `search_path='a"b'`},
		{name: "a single quote inside a quoted name", entry: `search_path="it's"`, want: "search_path='it''s'"},
		{name: "no space around the comma", entry: "search_path=a,b", want: "search_path='a', 'b'"},
		{name: "an empty list value", entry: "search_path=", want: "search_path=''"},
		{name: "the catalog's empty name", entry: `search_path=""`, want: "search_path=''"},
		{name: "a list name in another case", entry: "SEARCH_PATH=a, b", want: "SEARCH_PATH='a', 'b'"},
		{name: "a quoted list name", entry: `"search_path"=a, b`, want: `"search_path"='a', 'b'`},
		{name: "no value", entry: "search_path FROM CURRENT", want: "search_path FROM CURRENT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := routinesetting.ParseCatalog(test.entry)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestParseCatalog_FailurePath pins the lists an author can write that the
// server would not keep. PostgreSQL 18 refuses the first five through
// set_config with "List syntax is invalid". It accepts the single quotes as part
// of a name and then searches nothing, so reading them as SQL's quotes or
// passing them on would both pin a routine to something the author did not
// write.
func TestParseCatalog_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		entry string
	}{
		{name: "text after a quoted name", entry: `search_path="a"b`},
		{name: "two words", entry: "search_path=a b"},
		{name: "an empty element", entry: "search_path=a,,b"},
		{name: "a trailing comma", entry: "search_path=a,"},
		{name: "an unclosed quote", entry: `search_path="a`},
		{name: "a literal in SQL's spelling", entry: "search_path='public'"},
		{name: "a literal that holds a comma", entry: "search_path='a, b'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := routinesetting.ParseCatalog(test.entry)

			c.Assert(err, qt.ErrorMatches, `.* is not a list as pg_proc.proconfig keeps it: .*`)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestFromCatalog_KeepsAListTheServerCouldNotHaveKept pins that reading a
// catalog row never loses text: what the strict reader refuses stays one literal
// and so compares as written.
func TestFromCatalog_KeepsAListTheServerCouldNotHaveKept(t *testing.T) {
	c := qt.New(t)

	got := routinesetting.FromCatalog(`search_path="a"b`)

	c.Assert(got, qt.Equals, `search_path='"a"b'`)
}
