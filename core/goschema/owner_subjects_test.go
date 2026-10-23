package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// partsFile holds every declaration of a file until the file is read, then
// declares a gauge facet for the part each one describes. A declaration that
// writes ghost names a column or index the file does not declare.
type partsFile struct{ declarations *[]annotation.Declaration }

func (f partsFile) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	*f.declarations = append(*f.declarations, declaration)
	return nil, nil
}

func (f partsFile) Finish(annotation.Tables) ([]annotation.Contribution, error) {
	contributions := make([]annotation.Contribution, 0, len(*f.declarations))
	for _, declaration := range *f.declarations {
		subject := declaration.Subject
		if declaration.Attributes["ghost"] != "" {
			subject.Name = "ghost"
		}
		contributions = append(contributions, annotation.Contribution{Subject: subject, Label: "a gauge", Source: declaration,
			Facet: &widget{Level: declaration.Attributes["level"]}})
	}
	return contributions, nil
}

// partsOwner declares a column directive and an index directive of its own, and
// reads the level and ghost attributes of the frontend's table, field and index
// directives. declarations receives what the frontend hands the owner.
func partsOwner(c *qt.C, declarations *[]annotation.Declaration) annotation.Set {
	c.Helper()
	attributes := []annotation.Attribute{{Name: "level", Value: "string"}, {Name: "ghost", Value: "string"}}
	set, err := annotation.NewSet(annotation.Extension{
		Owner: "example.org/widget",
		Directives: []annotation.Directive{
			{Name: "ptah:schema:gauge-column", Scopes: []annotation.Scope{annotation.ScopeField}, Describes: annotation.SubjectColumn, Attributes: attributes},
			{Name: "ptah:schema:gauge-index", Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField}, Describes: annotation.SubjectIndex, Attributes: attributes},
		},
		Attributes: []annotation.DirectiveAttributes{
			{Directive: "ptah:schema:table", Attributes: attributes[:1], PerFile: true},
			{Directive: "ptah:schema:field", Attributes: attributes, PerFile: true},
			{Directive: "ptah:schema:index", Attributes: attributes, PerFile: true},
		},
		Kinds:    []schemaext.Kind{widgetKind},
		File:     func() annotation.FileDecoder { return partsFile{declarations: declarations} },
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) { return widgetCoverage() },
	})
	c.Assert(err, qt.IsNil)
	return set
}

func levelOf(c *qt.C, facets schemaext.Facets) string {
	c.Helper()
	gauge, found, err := schemaext.FacetAs[*widget](facets, widgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return gauge.Level
}

const partsSource = `package models

//ptah:schema:table name="items" level=table
type Item struct {
	//ptah:schema:field name="id" type="INTEGER" level=attribute
	ID int
	//ptah:schema:field name="body" type="TEXT"
	//ptah:schema:gauge-column level=directive
	Body string
	//ptah:schema:field name="spare" type="TEXT"
	Spare string

	//ptah:schema:index name="by_body" fields="body"
	//ptah:schema:gauge-index level=directive
	_ int
	//ptah:schema:index name="by_id" fields="id" level=attribute
	_ int
	//ptah:schema:index name="plain" fields="spare"
	_ int
}

type Extra struct {
	//ptah:schema:index name="elsewhere" fields="id" table="items" level=attribute
	_ int
}
`

func TestParseSource_AnOwnerDeclaresFacetsOfColumnsAndIndexes(t *testing.T) {
	c := qt.New(t)
	var declarations []annotation.Declaration

	db, err := goschema.ParseSource(partsOwner(c, &declarations), "items.go", partsSource)

	c.Assert(err, qt.IsNil)
	c.Assert(levelOf(c, db.Tables[0].Facets), qt.Equals, "table")
	c.Assert(columnLevels(c, db.Fields), qt.DeepEquals, map[string]string{"id": "attribute", "body": "directive", "spare": ""})
	c.Assert(indexLevels(c, db.Indexes), qt.DeepEquals, map[string]string{"by_body": "directive", "by_id": "attribute", "plain": "", "elsewhere": "attribute"})
}

// columnLevels maps each column to its gauge level, or to "" where it has none.
func columnLevels(c *qt.C, fields []schemamodel.Field) map[string]string {
	c.Helper()
	levels := make(map[string]string)
	for _, field := range fields {
		levels[field.Name] = ""
		if len(field.Facets.DeclaredKinds()) > 0 {
			levels[field.Name] = levelOf(c, field.Facets)
		}
	}
	return levels
}

// indexLevels maps each index to its gauge level, or to "" where it has none.
func indexLevels(c *qt.C, indexes []schemamodel.Index) map[string]string {
	c.Helper()
	levels := make(map[string]string)
	for _, index := range indexes {
		levels[index.Name] = ""
		if len(index.Facets.DeclaredKinds()) > 0 {
			levels[index.Name] = levelOf(c, index.Facets)
		}
	}
	return levels
}

func TestParseSource_AnOwnerIsHandedTheColumnOrIndexItDescribes(t *testing.T) {
	c := qt.New(t)
	var declarations []annotation.Declaration

	_, err := goschema.ParseSource(partsOwner(c, &declarations), "items.go", partsSource)

	c.Assert(err, qt.IsNil)
	var got []string
	for _, declaration := range declarations {
		got = append(got, declaration.Directive+" "+string(declaration.Subject.Kind)+" "+declaration.Subject.Struct+" "+declaration.Subject.Name+" "+declaration.Subject.Table)
	}
	c.Assert(got, qt.DeepEquals, []string{
		"ptah:schema:table table Item  ",
		"ptah:schema:field column Item id ",
		"ptah:schema:gauge-column column Item body ",
		"ptah:schema:gauge-index index Item by_body ",
		"ptah:schema:index index Item by_id ",
		"ptah:schema:index index Extra elsewhere items",
	})
	c.Assert(declarations[1].Attributes, qt.DeepEquals, map[string]string{"level": "attribute"}, qt.Commentf("an owner reads its attributes and not the frontend's"))
	c.Assert(declarations[2].Line, qt.Equals, 8)
	c.Assert(declarations[2].Start.Line, qt.Equals, 8)
}

func TestParseSource_AnOwnerFacetOfAColumnOrIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{"a column directive without a field directive",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:gauge-column level=a\n\tBody string\n}\n",
			`a column directive is written beside the ptah:schema:field directive of its column on //ptah:schema:gauge-column at Item`},
		{"a column that has no name",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:field type=\"TEXT\"\n\t//ptah:schema:gauge-column level=a\n\tBody string\n}\n",
			`the ptah:schema:field directive it describes needs a name attribute on //ptah:schema:gauge-column at Item`},
		{"an index directive without an index directive",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:gauge-index level=a\n\t_ int\n}\n",
			`an index directive is written beside the ptah:schema:index directive of its index on //ptah:schema:gauge-index at Item`},
		{"an index directive among two index directives",
			"package models\n\n//ptah:schema:table name=\"items\"\n//ptah:schema:index name=\"a\" fields=\"id\"\n//ptah:schema:index name=\"b\" fields=\"id\"\n//ptah:schema:gauge-index level=a\ntype Item struct{}\n",
			`this comment has 2 ptah:schema:index directives, and an index directive describes exactly one on //ptah:schema:gauge-index at Item`},
		{"an index that has no name",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:index fields=\"id\"\n\t//ptah:schema:gauge-index level=a\n\t_ int\n}\n",
			`the ptah:schema:index directive it describes needs a name attribute on //ptah:schema:gauge-index at Item`},
		{"a facet of a column the file does not declare",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" level=a ghost=yes\n\tID int\n}\n",
			`column "ghost" of struct Item is not declared in this file, and a gauge is declared beside it on //ptah:schema:field at Item`},
		{"a facet of an index the file does not declare",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:index name=\"a\" fields=\"id\" level=a ghost=yes\n\t_ int\n}\n",
			`index "ghost" of struct Item is not declared in this file, and a gauge is declared beside it on //ptah:schema:index at Item`},
		{"a column that declares a gauge twice",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" level=a\n\t//ptah:schema:gauge-column level=b\n\tID int\n}\n",
			`column "id" declares a gauge twice on //ptah:schema:gauge-column at Item`},
		{"an attribute outside the owner's",
			"package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" typo=1\n\tID int\n}\n",
			`unknown annotation attribute "typo" on //ptah:schema:field at Item.ID`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var declarations []annotation.Declaration

			db, err := goschema.ParseSource(partsOwner(c, &declarations), "items.go", test.source)

			c.Assert(err, qt.ErrorMatches, test.want)
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}
