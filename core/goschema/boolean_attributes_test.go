package goschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/builtintest"
)

// booleanAttributeCase is one boolean attribute of one directive, with a
// source that declares the directive and writes the attribute where ATTR
// stands.
type booleanAttributeCase struct {
	directive string
	attribute string
	source    string
}

const (
	booleanTable = "//ptah:schema:table name=\"t\"\ntype T struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary\n\tID int\n}\n\n"
	booleanRole  = "//ptah:schema:role name=\"r\" ATTR\ntype R struct{}\n"
	booleanGrant = booleanTable + "//ptah:schema:role name=\"r\"\ntype R struct{}\n\n" +
		"//ptah:schema:grant role=\"r\" privilege=\"SELECT\" on_table=\"t\" ATTR\ntype G struct{}\n"
	booleanField = "//ptah:schema:table name=\"t\"\ntype T struct {\n" +
		"\t//ptah:schema:field name=\"a\" type=\"INTEGER\" ATTR\n\tA int\n}\n"
	booleanIndex = "//ptah:schema:table name=\"t\"\ntype T struct {\n" +
		"\t//ptah:schema:field name=\"a\" type=\"INTEGER\"\n" +
		"\t//ptah:schema:index name=\"t_a_idx\" fields=\"a\" ATTR\n\tA int\n}\n"
	booleanFunction = "//ptah:schema:function name=\"f\" params=\"a integer\" returns=\"integer\" " +
		"language=\"sql\" body=\"SELECT a\" ATTR\ntype F struct{}\n"
	booleanHypertable = "//ptah:schema:table name=\"t\"\ntype T struct {\n" +
		"\t//ptah:schema:field name=\"ts\" type=\"TIMESTAMPTZ\" not_null\n\tTS string\n}\n\n" +
		"//ptah:schema:hypertable table=\"t\" column=\"ts\" ATTR\ntype H struct{}\n"
)

// booleanAttributeCases holds every boolean attribute the built-in catalog
// declares. TestBooleanAttributeCases_CoverTheCatalog holds the list to the
// catalog, so an attribute added there without a row here fails.
var booleanAttributeCases = []booleanAttributeCase{
	{"ptah:schema:field", "not_null", booleanField},
	{"ptah:schema:field", "primary", booleanField},
	{"ptah:schema:field", "auto_increment", strings.ReplaceAll(booleanField, "ATTR", "primary ATTR")},
	{"ptah:schema:field", "unique", booleanField},
	{"ptah:schema:field", "stored", "//ptah:schema:table name=\"t\"\ntype T struct {\n" +
		"\t//ptah:schema:field name=\"a\" type=\"INTEGER\"\n\tA int\n" +
		"\t//ptah:schema:field name=\"b\" type=\"INTEGER\" generated=\"a + 1\" ATTR\n\tB int\n}\n"},
	{"ptah:embedded", "nullable", "type Addr struct {\n\t//ptah:schema:field name=\"city\" type=\"TEXT\"\n\tCity string\n}\n\n" +
		"//ptah:schema:table name=\"t\"\ntype T struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary\n\tID int\n" +
		"\t//ptah:embedded mode=\"json\" name=\"addr\" ATTR\n\tAddr Addr\n}\n"},
	{"ptah:schema:index", "unique", booleanIndex},
	{"ptah:schema:index", "invisible", booleanIndex},
	{"ptah:schema:index", "nulls_distinct", strings.ReplaceAll(booleanIndex, "ATTR", "unique ATTR")},
	{"ptah:schema:table", "primary_key_without_overlaps", "//ptah:schema:table name=\"t\" primary_key=\"id,valid_at\" ATTR\n" +
		"type T struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" not_null\n\tID int\n" +
		"\t//ptah:schema:field name=\"valid_at\" type=\"DATERANGE\" not_null\n\tValidAt string\n}\n"},
	{"ptah:schema:constraint", "nulls_distinct", "//ptah:schema:constraint name=\"u\" type=\"UNIQUE\" columns=\"a\" ATTR\ntype C struct{}\n"},
	{"ptah:schema:constraint", "without_overlaps", "//ptah:schema:constraint name=\"u\" type=\"UNIQUE\" columns=\"id,valid_at\" ATTR\ntype C struct{}\n"},
	{"ptah:schema:constraint", "period", "//ptah:schema:constraint name=\"f\" type=\"FOREIGN KEY\" columns=\"a,b\" " +
		"foreign_table=\"p\" foreign_columns=\"a,b\" ATTR\ntype C struct{}\n"},
	{"ptah:schema:extension", "if_not_exists", "//ptah:schema:extension name=\"pgcrypto\" ATTR\ntype E struct{}\n"},
	{"ptah:schema:sequence", "cycle", "//ptah:schema:sequence name=\"s\" ATTR\ntype S struct{}\n"},
	{"ptah:schema:sequence", "if_not_exists", "//ptah:schema:sequence name=\"s\" ATTR\ntype S struct{}\n"},
	{"ptah:schema:domain", "not_null", "//ptah:schema:domain name=\"d\" type=\"TEXT\" ATTR\ntype D struct{}\n"},
	{"ptah:schema:rls:enable", "force", booleanTable + "//ptah:schema:rls:enable table=\"t\" ATTR\ntype P struct{}\n"},
	{"ptah:schema:role", "login", booleanRole},
	{"ptah:schema:role", "superuser", booleanRole},
	{"ptah:schema:role", "createdb", booleanRole},
	{"ptah:schema:role", "create_db", booleanRole},
	{"ptah:schema:role", "createrole", booleanRole},
	{"ptah:schema:role", "create_role", booleanRole},
	{"ptah:schema:role", "inherit", booleanRole},
	{"ptah:schema:role", "replication", booleanRole},
	{"ptah:schema:role", "group", booleanRole},
	{"ptah:schema:grant", "on_database", "//ptah:schema:role name=\"r\"\ntype R struct{}\n\n" +
		"//ptah:schema:grant role=\"r\" privilege=\"SELECT\" ATTR\ntype G struct{}\n"},
	{"ptah:schema:grant", "with_option", booleanGrant},
	{"ptah:schema:grant", "grant_option", booleanGrant},
	{"ptah:schema:revoke", "on_database", "//ptah:schema:revoke role=\"r\" privilege=\"SELECT\" ATTR\ntype V struct{}\n"},
	{"ptah:schema:function", "strict", booleanFunction},
	{"ptah:schema:function", "leakproof", booleanFunction},
	{"ptah:schema:exclude", "deferrable", booleanTable + "//ptah:schema:exclude name=\"t_ex\" table=\"t\" using=\"gist\" " +
		"elements=\"id WITH =\" ATTR\ntype X struct{}\n"},
	{"ptah:schema:hypertable", "if_not_exists", booleanHypertable},
	{"ptah:schema:continuousaggregate", "materialized_only", strings.ReplaceAll(booleanHypertable, "ATTR", "") +
		"\n//ptah:schema:continuousaggregate name=\"agg\" body=\"SELECT 1\" ATTR\ntype A struct{}\n"},
}

// parseBooleanCase parses the case's source with spelling written for ATTR.
func parseBooleanCase(c *qt.C, test booleanAttributeCase, spelling string) (schemamodel.Database, error) {
	c.Helper()
	source := "package p\n\n" + strings.ReplaceAll(test.source, "ATTR", spelling)
	return goschema.ParseSource(builtintest.Annotations(), "boolean.go", source)
}

// Every boolean attribute is declared a bare boolean, and the list of cases
// is the catalog's list: an attribute declared "boolean" and not bare would
// be refused when written bare, which is the convention its siblings follow.
func TestBooleanAttributeCases_CoverTheCatalog(t *testing.T) {
	c := qt.New(t)
	catalog, err := annotationmeta.NewCatalog(builtintest.Annotations())
	c.Assert(err, qt.IsNil)

	declared, bare := catalogBooleanAttributes(catalog)
	cases := make([]string, 0, len(booleanAttributeCases))
	for _, test := range booleanAttributeCases {
		cases = append(cases, test.directive+" "+test.attribute)
	}
	slices.Sort(cases)

	c.Assert(bare, qt.DeepEquals, declared)
	c.Assert(cases, qt.DeepEquals, declared)
	c.Assert(len(cases) >= 36, qt.IsTrue, qt.Commentf("%d boolean attributes", len(cases)))
}

// catalogBooleanAttributes lists, sorted and as "<directive> <attribute>",
// every boolean attribute the catalog declares and the ones it declares bare.
func catalogBooleanAttributes(catalog annotationmeta.Catalog) (declared, bare []string) {
	for _, directive := range catalog.Directives() {
		for _, attribute := range directive.Attributes {
			if attribute.Value == "boolean" {
				declared = append(declared, directive.Name+" "+attribute.Name)
			}
			if attribute.Boolean {
				bare = append(bare, directive.Name+" "+attribute.Name)
			}
		}
	}
	slices.Sort(declared)
	slices.Sort(bare)
	return declared, bare
}

// A bare boolean attribute reads as true: the schema it declares is the one
// its attribute="true" spelling declares, and not the one attribute="false"
// declares. The second comparison proves the reader reads the attribute at
// all; without it, a reader that ignored the attribute would make the bare
// and the spelled-out forms agree.
func TestParseSource_BareBooleanAttribute_HappyPath(t *testing.T) {
	for _, test := range booleanAttributeCases {
		t.Run(test.directive+" "+test.attribute, func(t *testing.T) {
			c := qt.New(t)

			bare, bareErr := parseBooleanCase(c, test, test.attribute)
			spelled, spelledErr := parseBooleanCase(c, test, test.attribute+`="true"`)
			negated, negatedErr := parseBooleanCase(c, test, test.attribute+`="false"`)

			c.Assert(bareErr, qt.IsNil)
			c.Assert(spelledErr, qt.IsNil)
			c.Assert(negatedErr, qt.IsNil)
			c.Assert(bare, qt.DeepEquals, spelled)
			c.Assert(bare, qt.Not(qt.DeepEquals), negated)
		})
	}
}

// A boolean attribute written with a value other than exactly true or false
// is refused by name. Read as false, TRUE, yes, 1 and an empty value would
// each declare the opposite of what their author wrote; read through
// strconv.ParseBool, TRUE and 1 would mean true on some attributes and false
// on others.
func TestParseSource_BooleanAttributeValue_FailurePath(t *testing.T) {
	values := []string{"TRUE", "", "yes", "1", "True", " true", "no", "0"}
	for i, test := range booleanAttributeCases {
		value := values[i%len(values)]
		t.Run(test.directive+" "+test.attribute+" "+value, func(t *testing.T) {
			c := qt.New(t)

			_, err := parseBooleanCase(c, test, test.attribute+`="`+value+`"`)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, `(?s).*invalid "`+test.attribute+`" value "`+value+`" on //`+test.directive+
				` at .*: not a boolean; write true or false.*`)
		})
	}
}

// A bare word naming an attribute that takes a value is refused by name, with
// the file and line of its comment: an owner's attribute, a platform override
// and an attribute named like a word of a directive name included. Dropped,
// the bare schema of a table would put it in the default schema without a
// word.
func TestParseSource_BareValueAttribute_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		directive string
		attribute string
		line      string
	}{
		{name: "the schema of a table", source: "//ptah:schema:table name=\"t\" schema\ntype T struct{}\n",
			directive: "ptah:schema:table", attribute: "schema", line: "3"},
		{name: "the privileges of a grant", source: booleanTable + "//ptah:schema:role name=\"r\"\ntype R struct{}\n\n" +
			"//ptah:schema:grant role=\"r\" privileges on_table=\"t\"\ntype G struct{}\n",
			directive: "ptah:schema:grant", attribute: "privileges", line: "12"},
		{name: "the table of a row-level security switch", source: "//ptah:schema:rls:enable table force\ntype P struct{}\n",
			directive: "ptah:schema:rls:enable", attribute: "table", line: "3"},
		{name: "a platform override", source: strings.ReplaceAll(booleanField, "ATTR", "platform.mysql.type"),
			directive: "ptah:schema:field", attribute: "platform.mysql.type", line: "5"},
		{name: "an attribute an owner adds to a directive", source: strings.ReplaceAll(booleanFunction, "ATTR", "parallel"),
			directive: "ptah:schema:function", attribute: "parallel", line: "3"},
		{name: "an attribute of an owner's directive", source: strings.ReplaceAll(strings.ReplaceAll(booleanHypertable,
			`column="ts"`, `column`), "ATTR", ""),
			directive: "ptah:schema:hypertable", attribute: "column", line: "9"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(builtintest.Annotations(), "bare.go", "package p\n\n"+test.source)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+strings.ReplaceAll(test.attribute, ".", `\.`)+` on //`+test.directive+
				` takes a value and is written bare; write `+strings.ReplaceAll(test.attribute, ".", `\.`)+
				`="\.\.\." at bare\.go:`+test.line+`.*`)
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Attribute, qt.Equals, test.attribute)
			c.Assert(parseErr.Directive, qt.Equals, test.directive)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}
