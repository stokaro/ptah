package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// coatEntry is the value of the paint owner's coats key of a table.
type coatEntry struct {
	Shade string `yaml:"shade"`
}

func decodeCoats(decode func(any) error, _ yamlext.Table) ([]yamlext.Contribution, error) {
	var coats []coatEntry
	if err := decode(&coats); err != nil {
		return nil, err
	}
	return []yamlext.Contribution{{Facet: &shade{Name: coats[0].Shade}, Label: "coats"}}, nil
}

// entryPaintOwner adds a gloss scalar to indexes, read beside the index's
// type into its WITH options, and a coats key to tables.
func entryPaintOwner() yamlext.Set {
	return must.Must(yamlext.NewSet(yamlext.Extension{
		Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil },
		EntryAttributes: []yamlext.EntryAttributes{{Entry: yamlext.EntryIndex, Attributes: []string{"gloss"}, Reads: []string{"type"},
			Parameters: func(attributes map[string]string) (map[string]string, error) {
				return map[string]string{"gloss": attributes["gloss"] + "/" + attributes["type"]}, nil
			}}},
		EntrySections: []yamlext.EntrySection{{Key: "coats", Decode: decodeCoats}},
	}))
}

const coatedDocument = `tables:
  rooms:
    coats:
      - shade: teal
    columns:
      id: { type: INTEGER, primary: true }
    indexes:
      idx_rooms_id:
        fields: [id]
        type: btree
        gloss: matte
`

// TestParse_AnOwnersEntryKeysJoinTheSchema drives the frontend's half of the
// entry keys: a table's coats reach the owner and its facet the table, and an
// index's gloss, read beside its type, becomes the index's options.
func TestParse_AnOwnersEntryKeysJoinTheSchema(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(entryPaintOwner(), []byte(coatedDocument))

	c.Assert(err, qt.IsNil)
	coat, found, err := schemaext.FacetAs[*shade](db.Tables[0].Facets, shadeKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(coat.Name, qt.Equals, "teal")
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].StorageParams, qt.DeepEquals, map[string]string{"gloss": "matte/btree"})
}

// TestParse_AnOwnersEntryKeys_FailurePath pins what the frontend refuses about
// an owner's entry keys: a key of the owner's value the owner does not
// declare, at the line the author wrote; and the owner's keys when no selected
// owner reads them, as any unknown key of an entry.
func TestParse_AnOwnersEntryKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		owners   yamlext.Set
		document string
		wantErr  string
	}{
		{name: "a key of the owner's value it does not declare", owners: entryPaintOwner(),
			document: "tables:\n  rooms:\n    coats:\n      - hue: teal\n",
			wantErr:  `(?s)parse YAML schema: .*line 4: field hue not found.*`},
		{name: "a table key no selected owner reads", owners: yamlext.None(), document: coatedDocument,
			wantErr: `parse YAML schema: line 4: unknown key "coats" of table "rooms"`},
		{name: "an index key no selected owner reads", owners: yamlext.None(),
			document: "tables:\n  rooms:\n    indexes:\n      idx:\n        fields: [id]\n        gloss: matte\n",
			wantErr:  `parse YAML schema: line \d+: unknown key "gloss" of index "idx"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(test.owners, []byte(test.document))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
