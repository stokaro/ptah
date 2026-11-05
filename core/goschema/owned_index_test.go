package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
)

// treeFile holds every tree index of a file and answers, once the file is
// read, with one widget of each table: the names of its tree indexes.
type treeFile struct{ declarations []annotation.Declaration }

func (f *treeFile) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	f.declarations = append(f.declarations, declaration)
	return nil, nil
}

func (f *treeFile) Finish(annotation.Tables) ([]annotation.Contribution, error) {
	if len(f.declarations) == 0 {
		return nil, nil
	}
	level := ""
	for _, declaration := range f.declarations {
		level += declaration.Attributes["name"] + ":" + declaration.Attributes["fields"] + ";"
	}
	return []annotation.Contribution{{Label: "a tree index set", Source: f.declarations[0], Facet: &widget{Level: level}}}, nil
}

// treeOwner reads the index declarations of type "tree" in place of the
// frontend. files receives the file decoder of each file.
func treeOwner(c *qt.C, files *[]*treeFile) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(annotation.Extension{
		Owner: "example.org/widget",
		Kinds: []schemaext.Kind{widgetKind},
		File: func() annotation.FileDecoder {
			file := &treeFile{}
			*files = append(*files, file)
			return file
		},
		ReadsIndex: func(indexType string) bool { return indexType == "tree" },
		Coverage:   annotation.Unlimited(widgetCoverage),
	})
	c.Assert(err, qt.IsNil)
	return set
}

// An index of a type an owner reads builds no common index: the owner reads
// the whole directive, every attribute included, and what it declares
// attaches to the table. An index of another type stays the frontend's.
func TestParseSource_AnOwnerReadsTheIndexTypesItClaims(t *testing.T) {
	c := qt.New(t)
	var files []*treeFile
	source := "package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n" +
		"\t//ptah:schema:index name=\"by_id\" fields=\"id\" type=\"tree\" comment=\"ids\"\n" +
		"\t//ptah:schema:index name=\"plain\" fields=\"id\" type=\"btree\"\n\tID int\n}\n"

	db, err := goschema.ParseSource(treeOwner(c, &files), "items.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "plain")
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].declarations, qt.HasLen, 1)
	declaration := files[0].declarations[0]
	c.Assert(declaration.Directive, qt.Equals, "ptah:schema:index")
	c.Assert(declaration.Attributes, qt.DeepEquals, map[string]string{"name": "by_id", "fields": "id", "type": "tree", "comment": "ids"})
	c.Assert(declaration.Subject, qt.Equals, annotation.Subject{Kind: annotation.SubjectIndex, Struct: "Item", Name: "by_id"})
	tree, found, err := schemaext.FacetAs[*widget](db.Tables[0].Facets, widgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(tree.Level, qt.Equals, "by_id:id;")
}

// An index the owner reads names itself: the owner reads it by its name.
func TestParseSource_AnOwnedIndexNeedsAName(t *testing.T) {
	c := qt.New(t)
	var files []*treeFile
	source := "package models\n\n//ptah:schema:table name=\"items\"\ntype Item struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\t//ptah:schema:index fields=\"id\" type=\"tree\"\n\tID int\n}\n"

	db, err := goschema.ParseSource(treeOwner(c, &files), "items.go", source)

	c.Assert(err, qt.ErrorMatches, `.*//ptah:schema:index at Item of type "tree" needs a name attribute: its owner reads the index by its name`)
	c.Assert(db.Tables, qt.HasLen, 0)
	c.Assert(files, qt.HasLen, 0)
}
