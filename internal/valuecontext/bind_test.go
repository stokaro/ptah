package valuecontext_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/valuecontext"
)

const objectKind schemaext.Kind = "example.org/context/object"
const facetKind schemaext.Kind = "example.org/context/facet"

type value struct {
	ID    schemaext.Kind
	Bound string
}

func (v *value) Kind() schemaext.Kind   { return v.ID }
func (v *value) Clone() schemaext.Value { result := *v; return &result }
func (v *value) Equal(other schemaext.Value) bool {
	right, ok := other.(*value)
	return ok && right != nil && *v == *right
}

type binder struct {
	fail      bool
	addresses []string
}

func (b *binder) ResolveDatabaseContext(_ context.Context, r schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error) {
	b.addresses = append(b.addresses, r.Address)
	return schemaext.DatabaseContext{Database: r.Address, SourceDatabase: r.Address + "/source"}, nil
}

func (b *binder) BindValueContexts(_ context.Context, r schemaext.ValueContextRequest) ([]schemaext.Value, error) {
	for _, input := range r.Values {
		input.(*value).Bound = r.Context.Database
	}
	if b.fail {
		return nil, errors.New("binding refused")
	}
	return r.Values, nil
}

// owner resolves addresses and binds values for the test runtime.
type owner interface {
	schemaext.DatabaseContextResolver
	schemaext.ValueContextService
}

func runtime(c *qt.C, b owner) *engine.Runtime {
	c.Helper()
	var codecs []schemaext.Codec
	for _, kind := range []schemaext.Kind{objectKind, facetKind} {
		encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload.(*value).Bound) }
		codecs = append(codecs, schemaext.Codec{Prototype: &value{ID: kind}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(`{"type":"string"}`),
			Clone: func(payload schemaext.Payload) (schemaext.Payload, error) { return payload.(*value).Clone(), nil }, Encode: encode, Canonical: encode,
			Decode: func(data json.RawMessage) (schemaext.Payload, error) {
				bound, err := schemaext.DecodeJSON[string](data)
				return &value{ID: kind, Bound: bound}, err
			},
		})
	}
	r, err := engine.New(engine.Provider{ID: "example.org/context", Targets: []engine.Target{{Name: "selected", Aliases: []string{"alias"}, DatabaseContext: b}}, Codecs: codecs,
		ContextBindings: []engine.ContextBinding{{Target: "selected", Kinds: []schemaext.Kind{objectKind, facetKind}, Service: b}},
	})
	c.Assert(err, qt.IsNil)
	return r
}

func source(c *qt.C) *schemamodel.Database {
	c.Helper()
	ref := func(name string) objectidentity.ID {
		return objectidentity.ID{Kind: objectidentity.Kind(objectKind), Schema: objectidentity.Part{Source: "app", Normalized: "app"}, Name: objectidentity.Part{Source: name, Normalized: name}}
	}
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ref("kept"), Value: &value{ID: objectKind}, Targets: []string{"alias"}}, schemaext.Object{Ref: ref("other"), Value: &value{ID: objectKind}, Targets: []string{"different"}})
	c.Assert(err, qt.IsNil)
	facets, err := schemaext.NewFacets(&value{ID: facetKind})
	c.Assert(err, qt.IsNil)
	facets, err = facets.WithTargetScope(facetKind, "alias")
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{FeatureObjects: objects, Tables: []schemamodel.Table{{Name: "items", Facets: facets}}}
}

func TestSourceBindingPreservesIdentityScopesAndIndependentTargetContexts(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	b := &binder{}
	r := runtime(c, b)
	first, second := source(c), source(c)
	before, err := first.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(valuecontext.BindSource(c.Context(), r, "alias", "/first", first), qt.IsNil)
	c.Assert(valuecontext.BindSource(c.Context(), r, "selected", "/second", second), qt.IsNil)
	for _, test := range []struct {
		name     string
		database *schemamodel.Database
		root     string
	}{{name: "first", database: first, root: "/first"}, {name: "second", database: second, root: "/second"}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects, err := test.database.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects[0].Ref, qt.DeepEquals, before[0].Ref)
			c.Assert(objects[0].Targets, qt.DeepEquals, before[0].Targets)
			c.Assert(objects[0].Value.(*value).Bound, qt.Equals, test.root)
			c.Assert(objects[1].Value.(*value).Bound, qt.Equals, "")
			facet, found, err := test.database.Tables[0].Facets.Get(facetKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(facet.(*value).Bound, qt.Equals, test.root)
			c.Assert(test.database.Tables[0].Facets.TargetScope(facetKind), qt.DeepEquals, []string{"alias"})
		})
	}
	c.Assert(before[0].Value.(*value).Bound, qt.Equals, "")
	c.Assert(b.addresses, qt.DeepEquals, []string{"/first", "/second"})
}

func TestSourceBindingFailureLeavesObjectsAndFacetsUnchanged(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	b := &binder{fail: true}
	database := source(c)
	err := valuecontext.BindSource(c.Context(), runtime(c, b), "selected", "/db", database)
	c.Assert(err, qt.ErrorMatches, "binding refused")
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects[0].Value.(*value).Bound, qt.Equals, "")
	facet, _, err := database.Tables[0].Facets.Get(facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(facet.(*value).Bound, qt.Equals, "")
}

func TestSourceBindingReturnsIndependentFacetCarriers(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	b := &binder{}
	original := source(c)
	original.Views = []schemamodel.View{{Name: "report", Facets: original.Tables[0].Facets}}
	result, err := valuecontext.Source(c.Context(), runtime(c, b), "selected", "/db", original)
	c.Assert(err, qt.IsNil)
	for _, pair := range []struct {
		name          string
		before, after schemaext.Facets
	}{
		{"table", original.Tables[0].Facets, result.Tables[0].Facets},
		{"view", original.Views[0].Facets, result.Views[0].Facets},
	} {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			before, _, err := pair.before.Get(facetKind)
			c.Assert(err, qt.IsNil)
			after, _, err := pair.after.Get(facetKind)
			c.Assert(err, qt.IsNil)
			c.Assert(before.(*value).Bound, qt.Equals, "")
			c.Assert(after.(*value).Bound, qt.Equals, "/db")
		})
	}
}

// mover resolves each address to its context in contexts and binds a value by
// recording the database it is bound to and the one it moved from.
type mover struct {
	contexts map[string]schemaext.DatabaseContext
	fail     bool
	calls    int
}

func (m *mover) ResolveDatabaseContext(_ context.Context, r schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error) {
	return m.contexts[r.Address], nil
}

func (m *mover) BindValueContexts(_ context.Context, r schemaext.ValueContextRequest) ([]schemaext.Value, error) {
	m.calls++
	for _, input := range r.Values {
		input.(*value).Bound = r.From.Database + ">" + r.Context.Database
	}
	if m.fail {
		return nil, errors.New("move refused")
	}
	return r.Values, nil
}

func realms() map[string]schemaext.DatabaseContext {
	return map[string]schemaext.DatabaseContext{
		"db":    {Database: "/db", SourceDatabase: "/db"},
		"realm": {Database: "/db/realm", SourceDatabase: "/db"},
		"other": {Database: "/other", SourceDatabase: "/other"},
	}
}

// A move binds the selected target's values, objects and facets alike, from
// the context the first address names to the one the second names.
func TestMove_HappyPath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	m := &mover{contexts: realms()}
	database := source(c)

	moved, err := valuecontext.Move(c.Context(), runtime(c, m), "selected", "db", "realm", database)

	c.Assert(err, qt.IsNil)
	c.Assert(moved, qt.IsTrue)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects[0].Value.(*value).Bound, qt.Equals, "/db>/db/realm")
	c.Assert(objects[1].Value.(*value).Bound, qt.Equals, "")
	facet, _, err := database.Tables[0].Facets.Get(facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(facet.(*value).Bound, qt.Equals, "/db>/db/realm")
}

// Nothing moves, and the source is left as it is, where there is nothing to
// move between: a missing address, one context, or two roots a moved
// reference would be read against differently.
func TestMoveLeavesTheSource_HappyPath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ name, from, to string }{
		{name: "the same context", from: "realm", to: "realm"},
		{name: "another root", from: "db", to: "other"},
		{name: "no target address", from: "", to: "realm"},
		{name: "no dev address", from: "db", to: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			m := &mover{contexts: realms()}
			database := source(c)

			moved, err := valuecontext.Move(c.Context(), runtime(c, m), "selected", test.from, test.to, database)

			c.Assert(err, qt.IsNil)
			c.Assert(moved, qt.IsFalse)
			c.Assert(m.calls, qt.Equals, 0)
			objects, err := database.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects[0].Value.(*value).Bound, qt.Equals, "")
		})
	}
}

// A refused move leaves the objects and the facets as they were.
func TestMove_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	m := &mover{contexts: realms(), fail: true}
	database := source(c)

	moved, err := valuecontext.Move(c.Context(), runtime(c, m), "selected", "db", "realm", database)

	c.Assert(err, qt.ErrorMatches, "move refused")
	c.Assert(moved, qt.IsFalse)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects[0].Value.(*value).Bound, qt.Equals, "")
	facet, _, err := database.Tables[0].Facets.Get(facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(facet.(*value).Bound, qt.Equals, "")
}
