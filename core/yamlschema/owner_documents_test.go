package yamlschema_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

func documentOwner(decode func(context.Context, yamlext.Document) (yamlext.DocumentResult, error)) yamlext.Extension {
	return yamlext.Extension{Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		Sections:      []yamlext.Section{{Key: "shades"}, {Key: "palettes"}},
		EntrySections: []yamlext.EntrySection{{Key: "paint"}}, Document: decode}
}

// TestOwnerDocumentRetainsExactInputAndForwardBindings crosses the public
// frontend and checks information that scalar coercion or per-key dispatch loses.
func TestOwnerDocumentRetainsExactInputAndForwardBindings(t *testing.T) {
	c := qt.New(t)
	source := []byte(`shades:
  walls: {table: future, palette: later, count: 9007199254740993, unset: null}
# A declaration may refer to a later owner section and table.
palettes:
  later: &colors [teal, "9007199254740993"]
tables:
  z:
    name: future
    schema: public
    columns:
      id: {type: INTEGER}
    paint: *colors
`)
	calls := 0
	owners, err := yamlext.NewSet(documentOwner(func(ctx context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(document.Source, qt.DeepEquals, source)
		c.Assert(document.Fragments, qt.HasLen, 3)
		c.Assert(document.Tables, qt.DeepEquals, yamlext.Tables{{Key: "z", Name: "future", Schema: "public", Struct: "z"}})
		c.Assert(document.Fragments[0].Path, qt.DeepEquals, []string{"shades"})
		c.Assert(document.Fragments[0].Table, qt.Equals, -1)
		value := document.Fragments[0].Node.Content[1]
		c.Assert(value.Content[5].Tag, qt.Equals, "!!int")
		c.Assert(value.Content[5].Value, qt.Equals, "9007199254740993")
		c.Assert(value.Content[7].Tag, qt.Equals, "!!null")
		c.Assert(value.Line, qt.Equals, 2)
		colors := document.Fragments[1].Node.Content[1]
		c.Assert(colors.Content[1].Tag, qt.Equals, "!!str")
		alias := document.Fragments[2].Node
		c.Assert(alias.Kind, qt.Equals, yamlext.AliasNode)
		c.Assert(alias.Alias, qt.Equals, colors)
		c.Assert(document.Fragments[2].Path, qt.DeepEquals, []string{"tables", "z", "paint"})
		c.Assert(document.Fragments[2].Table, qt.Equals, 0)
		table, findErr := document.Tables.Find("", "public.future")
		c.Assert(findErr, qt.IsNil)
		return yamlext.DocumentResult{Contributions: []yamlext.Contribution{{Facet: &shade{Name: colors.Content[0].Value}, Table: table}}}, nil
	}))
	c.Assert(err, qt.IsNil)
	db, err := yamlschema.Parse(t.Context(), owners, source)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	painted, found, err := schemaext.FacetAs[*shade](db.Tables[0].Facets, shadeKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(painted.Name, qt.Equals, "teal")
	c.Assert(db.FeatureCoverage.Lookup(shadeKind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
}

func TestOwnerDocumentEmptyInputNeedsExplicitCoverage(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Complete} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			owners, err := yamlext.NewSet(documentOwner(func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
				calls++
				c.Assert(document.Fragments, qt.HasLen, 0)
				claim, claimErr := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{
					Model:     schemaext.CodecIdentity{Owner: "example.org/paint", Kind: shadeKind, Representation: schemaext.Desired, Version: 1, Definition: "test-definition"},
					Knowledge: schemaext.Knowledge{State: state, Reason: "explicit test knowledge"},
				}}, nil)
				return yamlext.DocumentResult{Coverage: claim}, claimErr
			}))
			c.Assert(err, qt.IsNil)
			db, err := yamlschema.Parse(t.Context(), owners, []byte("tables: {}\n"))
			c.Assert(err, qt.IsNil)
			c.Assert(calls, qt.Equals, 1)
			c.Assert(db.FeatureCoverage.Lookup(shadeKind, objectidentity.ID{}).State, qt.Equals, state)
		})
	}
}

func TestOwnerDocumentRefusesMalformedInputBeforeDispatch(t *testing.T) {
	cases := []struct{ name, input string }{
		{"duplicate owner key", "shades: {x: {value: 1, value: 2}}"},
		{"cyclic alias", "shades: &loop [*loop]"},
		{"unknown top key", "misspelled: {}"},
		{"unknown table key", "tables: {a: {misspelled: {}}}"},
		{"multiple documents", "shades: {}\n---\npalettes: {}"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			owners, err := yamlext.NewSet(documentOwner(func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
				calls++
				return yamlext.DocumentResult{}, nil
			}))
			c.Assert(err, qt.IsNil)
			db, err := yamlschema.Parse(t.Context(), owners, []byte(test.input))
			c.Assert(err, qt.IsNotNil)
			c.Assert(db, qt.IsNil)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestOwnerDocumentFailureDiscardsContributions(t *testing.T) {
	refused := errors.New("owner refused document")
	cases := []struct {
		name string
		fail func(context.CancelFunc) error
		want error
	}{
		{"owner failure", func(context.CancelFunc) error { return refused }, refused},
		{"canceled successful callback", func(cancel context.CancelFunc) error { cancel(); return nil }, context.Canceled},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			owners, err := yamlext.NewSet(documentOwner(func(got context.Context, _ yamlext.Document) (yamlext.DocumentResult, error) {
				c.Assert(got, qt.Equals, ctx)
				return yamlext.DocumentResult{Contributions: []yamlext.Contribution{{Facet: &shade{Name: "teal"}}}}, test.fail(cancel)
			}))
			c.Assert(err, qt.IsNil)
			db, err := yamlschema.Parse(ctx, owners, []byte("tables: {a: {columns: {id: {type: INTEGER}}}}"))
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
