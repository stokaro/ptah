package yamlschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// entryDocumentOwner is a document owner of the column key shade and the
// index key shade, which reads the index's type with it. It gives each
// fragment's column or index the shade the key names, and records what it
// received.
func entryDocumentOwner(received *[]yamlext.Fragment) yamlext.Extension {
	return yamlext.Extension{Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		EntrySections: []yamlext.EntrySection{
			{Entry: yamlext.EntryColumn, Key: "shade"},
			{Entry: yamlext.EntryIndex, Key: "shade", Reads: []string{"type"}},
		},
		Document: func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
			*received = document.Fragments
			var result yamlext.DocumentResult
			for _, fragment := range document.Fragments {
				contribution := yamlext.Contribution{Facet: &shade{Name: fragment.Node.Value}, Table: fragment.Table, Label: "shade"}
				switch entry, _, _ := fragment.Entry(); entry {
				case yamlext.EntryColumn:
					contribution.Column = fragment.Name
				case yamlext.EntryIndex:
					contribution.Index = fragment.Name
				}
				result.Contributions = append(result.Contributions, contribution)
			}
			return result, nil
		}}
}

// TestOwnerEntryKeys_HappyPath hands a document owner its keys of column and
// index entries, a top-level index's among them, with the name each entry
// declares, the type the index key reads with its own and their positions,
// and attaches what it returns to the column or the index.
func TestOwnerEntryKeys_HappyPath(t *testing.T) {
	c := qt.New(t)
	var received []yamlext.Fragment
	owners, err := yamlext.NewSet(entryDocumentOwner(&received))
	c.Assert(err, qt.IsNil)
	source := []byte(`tables:
  items:
    columns:
      id: {type: INTEGER, name: item_id, shade: teal}
    fields:
      body: {type: TEXT, shade: red}
    indexes:
      by_body: {fields: [body], type: btree, shade: blue}
indexes:
  by_id: {table: items, name: items_by_id, fields: [item_id], shade: green}
`)

	db, err := yamlschema.Parse(t.Context(), owners, source)

	c.Assert(err, qt.IsNil)
	paths := make([][]string, 0, len(received))
	names := make([]string, 0, len(received))
	for _, fragment := range received {
		paths, names = append(paths, fragment.Path), append(names, fragment.Name)
	}
	c.Assert(paths, qt.DeepEquals, [][]string{
		{"tables", "items", "columns", "id", "shade"},
		{"tables", "items", "fields", "body", "shade"},
		{"tables", "items", "indexes", "by_body", "shade"},
		{"indexes", "by_id", "shade"},
	})
	c.Assert(names, qt.DeepEquals, []string{"item_id", "body", "by_body", "items_by_id"})
	c.Assert(received[2].Inherited["type"].Value, qt.Equals, "btree")
	c.Assert(received[2].Inherited["type"].Range.Start.Line, qt.Equals, 8)
	c.Assert(received[3].Inherited, qt.IsNil)
	c.Assert(received[3].Table, qt.Equals, 0)
	c.Assert(received[0].Node.Range.Start.Line, qt.Equals, 4)
	shades := make([]string, 0, 4)
	for _, facets := range []schemaext.Facets{db.Fields[0].Facets, db.Fields[1].Facets, db.Indexes[0].Facets, db.Indexes[1].Facets} {
		painted, found, err := schemaext.FacetAs[*shade](facets, shadeKind)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		shades = append(shades, painted.Name)
	}
	c.Assert(shades, qt.DeepEquals, []string{"teal", "red", "blue", "green"})
	c.Assert(db.Tables[0].Facets.Len(), qt.Equals, 0)
}

// TestOwnerEntryKeys_FailurePath refuses an owner that reads a key the
// frontend does not have with its key, a facet for a column the table does
// not declare, and a top-level index key whose table the document leaves to
// the database.
func TestOwnerEntryKeys_FailurePath(t *testing.T) {
	var received []yamlext.Fragment
	unread := entryDocumentOwner(&received)
	unread.EntrySections[1].Reads = []string{"tint"}
	stray := entryDocumentOwner(&received)
	document := stray.Document
	stray.Document = func(ctx context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
		result, err := document(ctx, input)
		result.Contributions[0].Column = "ghost"
		return result, err
	}
	cases := []struct {
		name    string
		owner   yamlext.Extension
		source  string
		wantErr string
	}{
		{name: "a read the frontend does not have", owner: unread, source: "tables: {}\n",
			wantErr: `parse YAML schema: the owner of key "shade" of indexes reads "tint" with it, which is not a key of the frontend's own`},
		{name: "a column the table does not declare", owner: stray, source: "tables:\n  items:\n    columns:\n      id: {type: INTEGER, shade: teal}\n",
			wantErr: `owner document: invalid feature value: shade names column "ghost", which table "items" does not declare`},
		{name: "a top-level index of a table the document leaves to the database", owner: entryDocumentOwner(&received),
			source:  "tables: {}\nindexes:\n  by_id: {table: elsewhere, fields: [id], shade: green}\n",
			wantErr: `parse YAML schema: index "by_id" names table "elsewhere", which the document does not declare; an owner's key of an index needs its table`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners, err := yamlext.NewSet(test.owner)
			c.Assert(err, qt.IsNil)

			db, err := yamlschema.Parse(t.Context(), owners, []byte(test.source))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
