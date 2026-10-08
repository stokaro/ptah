package schemaext_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestRegistry_RoundTripCollections(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(otherKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	value := &widget{ID: widgetKind, Names: []string{"kept"}}
	facets, err := schemaext.NewFacets(value, &widget{ID: otherKind})
	c.Assert(err, qt.IsNil)
	encoded, err := registry.EncodeFacets(context.Background(), schemaext.Desired, facets)
	c.Assert(err, qt.IsNil)
	c.Assert(encoded[0].Kind, qt.Equals, otherKind)
	decoded, err := registry.DecodeFacets(context.Background(), schemaext.Desired, encoded)
	c.Assert(err, qt.IsNil)
	values, err := decoded.Values()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []schemaext.Value{&widget{ID: otherKind}, value})
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: widgetRef("table", "name"), Value: value})
	c.Assert(err, qt.IsNil)
	wireObjects, err := registry.EncodeObjects(context.Background(), schemaext.Desired, objects)
	c.Assert(err, qt.IsNil)
	readObjects, err := registry.DecodeObjects(context.Background(), schemaext.Desired, wireObjects)
	c.Assert(err, qt.IsNil)
	all, err := readObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(all, qt.DeepEquals, []schemaext.Object{{Ref: widgetRef("table", "name"), Value: value}})
	_, err = registry.DecodeObjects(context.Background(), schemaext.Desired, append(wireObjects, wireObjects...))
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	_, err = registry.DecodeFacets(context.Background(), schemaext.Observed, encoded)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
