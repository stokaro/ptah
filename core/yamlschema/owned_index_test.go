package yamlschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// treeDocumentOwner reads the index entries of type "tree" in place of the
// frontend, and the shade key of index entries. It gives each tree index's
// table the shade the index names, and records what it received.
func treeDocumentOwner(received *[]yamlext.Fragment) yamlext.Extension {
	return yamlext.Extension{Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		EntrySections: []yamlext.EntrySection{{Entry: yamlext.EntryIndex, Key: "shade"}},
		ReadsIndex:    func(indexType string) bool { return indexType == "tree" },
		Document: func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
			*received = document.Fragments
			var result yamlext.DocumentResult
			for _, fragment := range document.Fragments {
				if fragment.Index != nil && fragment.Index.Owned["shade"] != nil {
					result.Contributions = append(result.Contributions, yamlext.Contribution{Facet: &shade{Name: fragment.Index.Owned["shade"].Value},
						Table: fragment.Table, Label: "shade"})
				}
			}
			return result, nil
		}}
}

// TestOwnedIndexEntries_HappyPath hands an owner a tree index entry of a
// table and a top-level one whole, with what the frontend read of each and
// the owner's keys, and builds no common index of them; an index of another
// type stays the frontend's.
func TestOwnedIndexEntries_HappyPath(t *testing.T) {
	c := qt.New(t)
	var received []yamlext.Fragment
	owners, err := yamlext.NewSet(treeDocumentOwner(&received))
	c.Assert(err, qt.IsNil)
	source := []byte(`tables:
  items:
    columns:
      id: {type: INTEGER}
      body: {type: TEXT}
    indexes:
      by_body: {fields: [body], include: [id], type: tree, comment: bodies, shade: blue}
      plain: {fields: [id]}
indexes:
  by_id: {table: items, name: items_by_id, fields: [id], type: tree, unique: true, ops: int_ops}
`)

	db, err := yamlschema.Parse(t.Context(), owners, source)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "plain")
	c.Assert(received, qt.HasLen, 2)
	c.Assert(received[0].Path, qt.DeepEquals, []string{"tables", "items", "indexes", "by_body"})
	c.Assert(received[0].Name, qt.Equals, "by_body")
	c.Assert(received[0].Table, qt.Equals, 0)
	c.Assert(received[0].Index.Owned["shade"].Value, qt.Equals, "blue")
	received[0].Index.Owned = nil
	c.Assert(received[0].Index, qt.DeepEquals, &yamlext.Index{Type: "tree", Columns: []string{"body"}, Include: []string{"id"}, Comment: "bodies"})
	c.Assert(received[1].Path, qt.DeepEquals, []string{"indexes", "by_id"})
	c.Assert(received[1].Name, qt.Equals, "items_by_id")
	c.Assert(received[1].Index, qt.DeepEquals, &yamlext.Index{Type: "tree", Columns: []string{"id"}, Unique: true, Ops: "int_ops", Owned: make(map[string]*yamlext.Node)})
	tree, found, err := schemaext.FacetAs[*shade](db.Tables[0].Facets, shadeKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(tree.Name, qt.Equals, "blue")
}

// TestOwnedIndexEntries_FailurePath refuses a tree index entry that writes a
// key the owner would never receive, one no owner reads, and one without
// fields.
func TestOwnedIndexEntries_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		entry string
		want  string
	}{
		{"a condition", `{fields: [id], type: tree, where: id > 0}`,
			`index "by_id" is a "tree" index, which example.org/paint reads in place of the frontend, and example.org/paint receives no where of it`},
		{"a platform group", `{fields: [id], type: tree, platform: {postgres: {fillfactor: "70"}}}`,
			`index "by_id" is a "tree" index, which example.org/paint reads in place of the frontend, and example.org/paint receives no platform of it`},
		{"a key no owner reads", `{fields: [id], type: tree, depth: 2}`, `parse YAML schema: line \d+: unknown key "depth" of index "by_id"`},
		{"no fields", `{type: tree}`, `index "by_id" requires fields`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var received []yamlext.Fragment
			owners, err := yamlext.NewSet(treeDocumentOwner(&received))
			c.Assert(err, qt.IsNil)

			db, err := yamlschema.Parse(t.Context(), owners, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER}\n    indexes:\n      by_id: "+test.entry+"\n"))

			c.Assert(err, qt.ErrorMatches, `.*`+test.want)
			c.Assert(db, qt.IsNil)
			c.Assert(received, qt.HasLen, 0)
		})
	}
}

// An owner reads index entries through its document decoder alone.
func TestOwnedIndexEntriesNeedADocumentOwner(t *testing.T) {
	c := qt.New(t)

	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		ReadsIndex: func(string) bool { return true }, Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }})

	c.Assert(err, qt.ErrorMatches, `YAML owner example.org/paint reads index entries without a document decoder`)
	c.Assert(owners.Selected(), qt.IsFalse)
}

// TestOwnedIndexEntries_RefusesAnotherOwnersKey refuses a key another owner
// reads in process on an entry of a type a document owner reads: no index is
// built for the first owner to read the key from.
func TestOwnedIndexEntries_RefusesAnotherOwnersKey(t *testing.T) {
	c := qt.New(t)
	var received []yamlext.Fragment
	gloss := yamlext.Extension{Owner: "example.org/gloss", Kinds: []schemaext.Kind{"example.org/gloss/level"},
		EntryAttributes: []yamlext.EntryAttributes{{Entry: yamlext.EntryIndex, Attributes: []string{"gloss"},
			Decode: func(map[string]string) (schemaext.Facets, error) { return schemaext.Facets{}, nil }}},
		Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }}
	owners, err := yamlext.NewSet(treeDocumentOwner(&received), gloss)
	c.Assert(err, qt.IsNil)

	db, err := yamlschema.Parse(t.Context(), owners, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER}\n"+
		"    indexes:\n      by_id: {fields: [id], type: tree, gloss: high}\n"))

	c.Assert(err, qt.ErrorMatches, `.*index "by_id" is a "tree" index, which example.org/paint reads in place of the frontend, and key "gloss" is another owner's`)
	c.Assert(db, qt.IsNil)
	c.Assert(received, qt.HasLen, 0)
}
