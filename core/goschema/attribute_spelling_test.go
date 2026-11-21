package goschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
)

const (
	spellingGrant = booleanTable + "//ptah:schema:role name=\"r\"\ntype R struct{}\n\n" +
		"//ptah:schema:grant role=\"r\" on_table=\"t\" ATTR\ntype G struct{}\n"
	spellingIndex = "//ptah:schema:table name=\"t\"\ntype T struct {\n" +
		"\t//ptah:schema:field name=\"a\" type=\"INTEGER\"\n" +
		"\t//ptah:schema:index name=\"t_a_idx\" ATTR\n\tA int\n}\n"
)

// spellingCase is one attribute written under its alias and under the name
// the alias stands for, in a source where ATTR stands for the spelling.
type spellingCase struct {
	name   string
	source string
	alias  string
	target string
}

var spellingCases = []spellingCase{
	{name: "createdb", source: booleanRole, alias: "create_db", target: "createdb"},
	{name: "createrole", source: booleanRole, alias: "create_role", target: "createrole"},
	{name: "with_option", source: booleanGrant, alias: "grant_option", target: "with_option"},
	{name: "privilege", source: spellingGrant, alias: `privileges="SELECT"`, target: `privilege="SELECT"`},
	{name: "fields", source: spellingIndex, alias: `columns="a"`, target: `fields="a"`},
	{name: "condition", source: spellingIndex, alias: `fields="a" where="a > 0"`, target: `fields="a" condition="a > 0"`},
}

// parseSpelling parses source with spelling written where ATTR stands.
func parseSpelling(c *qt.C, source, spelling string) (schemamodel.Database, error) {
	c.Helper()
	return goschema.ParseSource(builtintest.Annotations(), "spelling.go",
		"package p\n\n"+strings.ReplaceAll(source, "ATTR", spelling))
}

// An alias written alone declares what the name it stands for declares.
func TestParseSource_AttributeAlias_HappyPath(t *testing.T) {
	for _, test := range spellingCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			alias, aliasErr := parseSpelling(c, test.source, test.alias)
			target, targetErr := parseSpelling(c, test.source, test.target)

			c.Assert(aliasErr, qt.IsNil)
			c.Assert(targetErr, qt.IsNil)
			c.Assert(alias, qt.DeepEquals, target)
		})
	}
}

// An attribute written under two of its spellings is refused by name, the
// spellings sorted. A reader takes one spelling or either of them, so the
// other would be dropped or would override it: read either way,
// createdb="false" create_db declares CREATEDB.
func TestParseSource_TwoSpellingsOfOneAttribute_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		spelling string
		first    string
		second   string
	}{
		{name: "createdb", source: booleanRole, spelling: `createdb="false" create_db`, first: "create_db", second: "createdb"},
		{name: "createrole", source: booleanRole, spelling: `create_role createrole="false"`, first: "create_role", second: "createrole"},
		{name: "with_option", source: booleanGrant, spelling: `with_option="false" grant_option`, first: "grant_option", second: "with_option"},
		{name: "privilege", source: spellingGrant, spelling: `privilege="SELECT" privileges="INSERT"`, first: "privilege", second: "privileges"},
		{name: "fields", source: spellingIndex, spelling: `fields="a" columns="a"`, first: "columns", second: "fields"},
		{name: "condition", source: spellingIndex, spelling: `fields="a" condition="a > 0" where="a > 1"`, first: "condition", second: "where"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := parseSpelling(c, test.source, test.spelling)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches,
				`(?s).*"`+test.first+`" and "`+test.second+`" on //ptah:schema:\w+ at \S+ spell one attribute; write one of them.*`)
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Attribute, qt.Equals, test.second)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}
