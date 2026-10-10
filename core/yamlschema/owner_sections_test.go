package yamlschema_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// shade is a synthetic owner model: a shade a table is painted.
type shade struct{ Name string }

const shadeKind schemaext.Kind = "example.org/paint/shade"

func (*shade) Kind() schemaext.Kind { return shadeKind }
func (s *shade) Clone() schemaext.Value {
	cloned := *s
	return &cloned
}
func (s *shade) Equal(other schemaext.Value) bool {
	o, ok := other.(*shade)
	return ok && *o == *s
}

// shadeEntry is one entry of the paint owner's shades key.
type shadeEntry struct {
	Table string `yaml:"table"`
	Name  string `yaml:"name"`
}

// decodeShades reads the shades key as one facet of each table it names.
func decodeShades(decode func(any) error, tables yamlext.Tables) ([]yamlext.Contribution, error) {
	var entries map[string]shadeEntry
	if err := decode(&entries); err != nil {
		return nil, err
	}
	var contributions []yamlext.Contribution
	for key, entry := range entries {
		index, err := tables.Find("", entry.Table)
		if err != nil {
			return nil, err
		}
		if index < 0 {
			return nil, fmt.Errorf("shades.%s names table %q, which the document does not declare", key, entry.Table)
		}
		contributions = append(contributions, yamlext.Contribution{Facet: &shade{Name: entry.Name}, Table: index, Label: "a shade"})
	}
	return contributions, nil
}

// paintOwner reads the shades key under key.
func paintOwner(key string) yamlext.Set {
	return must.Must(yamlext.NewSet(yamlext.Extension{
		Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil },
		Sections: []yamlext.Section{{Key: key, Decode: decodeShades}},
	}))
}

const paintedDocument = `tables:
  rooms:
    columns:
      id: { type: INTEGER, primary: true }
shades:
  walls:
    table: rooms
    name: teal
`

// TestParse_AnOwnerSectionJoinsTheSchema drives the frontend's half of an
// owner section: the owner reads its key, finds the table by the document's
// rule, and the facet reaches that table.
func TestParse_AnOwnerSectionJoinsTheSchema(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(paintOwner("shades"), []byte(paintedDocument))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	painted, found, err := schemaext.FacetAs[*shade](db.Tables[0].Facets, shadeKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(painted.Name, qt.Equals, "teal")
}

// TestParse_AnOwnerSection_FailurePath pins what the frontend refuses about an
// owner section: a key inside it the owner does not declare, at the line the
// author wrote; the key when no selected owner reads it, as any unknown key;
// and an owner key that takes the name of one of the frontend's own.
func TestParse_AnOwnerSection_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		owners   yamlext.Set
		document string
		wantErr  string
	}{
		{name: "a key the owner does not declare", owners: paintOwner("shades"),
			document: "shades:\n  walls:\n    table: rooms\n    hue: teal\n",
			wantErr:  `(?s).*line 4: field hue not found in type yamlschema_test.shadeEntry.*`},
		{name: "a key no selected owner reads", owners: yamlext.None(), document: paintedDocument,
			wantErr: `parse YAML schema: line 5: unknown key "shades"`},
		{name: "an owner key of the frontend's own", owners: paintOwner("tables"), document: paintedDocument,
			wantErr: `parse YAML schema: YAML key "tables" of example.org/paint takes the name of one of the frontend's own`},
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
