package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

func materializedPaintOwner(attribute string) yamlext.Set {
	return must.Must(yamlext.NewSet(yamlext.Extension{
		Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil },
		EntryAttributes: []yamlext.EntryAttributes{{Entry: yamlext.EntryMaterializedView, Attributes: []string{attribute},
			Decode: func(values map[string]string) (schemaext.Facets, error) {
				return schemaext.NewFacets(&shade{Name: values[attribute]})
			}}},
	}))
}

func TestParse_MaterializedViewOwnerAttributes(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse(materializedPaintOwner("gloss"), []byte(`matviews:
  first: { body: "SELECT 1", gloss: teal }
  second: { body: "SELECT 2" }
`))
	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 2)
	c.Assert(db.MaterializedViews[0].Name, qt.Equals, "first")
	value, found, err := schemaext.FacetAs[*shade](db.MaterializedViews[0].Facets, shadeKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Name, qt.Equals, "teal")
	c.Assert(db.MaterializedViews[1].Facets.IsZero(), qt.IsTrue)
}

func TestParse_MaterializedViewOwnerAttributes_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name       string
		owners     yamlext.Set
		text, want string
	}{
		{"unselected owner", yamlext.None(), "matviews:\n  mv: { body: 'SELECT 1', gloss: teal }", `parse YAML schema: line 2: unknown key "gloss" of materialized view "mv"`},
		{"frontend key collision", materializedPaintOwner("body"), "matviews:\n  mv: { body: 'SELECT 1' }", `parse YAML schema: key "body" an owner adds to matviews takes the name of one of the frontend's own`},
		{"non-scalar value", materializedPaintOwner("gloss"), "matviews:\n  mv: { body: 'SELECT 1', gloss: [teal] }", `(?s)materialized view "mv": line 2: key "gloss": .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(test.owners, []byte(test.text))
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
