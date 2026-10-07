package schemaext_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestFacets_PreserveSnapshotsAndOrder(t *testing.T) {
	c := qt.New(t)
	input := &widget{ID: widgetKind, Names: []string{"kept"}}
	facets, err := schemaext.NewFacets(input, &widget{ID: otherKind})
	c.Assert(err, qt.IsNil)
	input.Names[0] = "changed"
	value, found, err := schemaext.FacetAs[*widget](facets, widgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Names, qt.DeepEquals, []string{"kept"})
	value.Names[0] = "changed again"
	values, err := facets.Values()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []schemaext.Value{&widget{ID: otherKind}, &widget{ID: widgetKind, Names: []string{"kept"}}})
	c.Assert(facets.Kinds(), qt.DeepEquals, []schemaext.Kind{otherKind, widgetKind})
	replaced, err := facets.Replace(&widget{ID: widgetKind, Names: []string{"new"}})
	c.Assert(err, qt.IsNil)
	original, _, err := schemaext.FacetAs[*widget](facets, widgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(original.Names, qt.DeepEquals, []string{"kept"})
	c.Assert(replaced.Without(widgetKind).Len(), qt.Equals, 1)
	c.Assert(replaced.Len(), qt.Equals, 2)
}

func TestFacets_RefuseDuplicateAndIncorrectType(t *testing.T) {
	c := qt.New(t)
	value := &widget{ID: widgetKind}
	_, err := schemaext.NewFacets(value, value)
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	facets, err := schemaext.NewFacets(value)
	c.Assert(err, qt.IsNil)
	_, err = facets.With(value)
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	_, err = facets.Replace(&widget{ID: otherKind})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	_, found, err := schemaext.FacetAs[*otherWidget](facets, widgetKind)
	c.Assert(found, qt.IsTrue)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	missing, found, err := facets.Get(otherKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
	c.Assert(missing, qt.IsNil)
}

func TestCollections_RefuseNilValues(t *testing.T) {
	for _, value := range []schemaext.Value{nil, (*widget)(nil), &widget{ID: "invalid"}} {
		c := qt.New(t)
		_, err := schemaext.NewFacets(value)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		_, err = schemaext.NewObjects(schemaext.Object{Ref: widgetRef("table", "name"), Value: value})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	}
}

func TestObjects_PreserveComponentBoundariesAndSnapshots(t *testing.T) {
	c := qt.New(t)
	input := &widget{ID: widgetKind, Names: []string{"kept"}}
	first := widgetRef("a.b", "c")
	second := widgetRef("a", "b.c")
	c.Assert(first.String(), qt.Equals, second.String())
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: first, Value: input}, schemaext.Object{Ref: second, Value: input})
	c.Assert(err, qt.IsNil)
	c.Assert(objects.Len(), qt.Equals, 2)
	input.Names[0] = "changed"
	all, err := objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(all[0].Ref, qt.Equals, second)
	c.Assert(all[1].Ref, qt.Equals, first)
	c.Assert(all[0].Value, qt.DeepEquals, &widget{ID: widgetKind, Names: []string{"kept"}})
	all[0].Value.(*widget).Names[0] = "changed again"
	got, found, err := objects.Get(second)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(got.Value, qt.DeepEquals, &widget{ID: widgetKind, Names: []string{"kept"}})
	c.Assert(objects.Without(first).Len(), qt.Equals, 1)
	c.Assert(objects.Len(), qt.Equals, 2)
}

func TestObjects_RejectNormalizedCollisions(t *testing.T) {
	c := qt.New(t)
	first := widgetRef("table", "Name")
	first.Name.Normalized = "name"
	second := widgetRef("table", "name")
	value := &widget{ID: widgetKind}
	_, err := schemaext.NewObjects(schemaext.Object{Ref: first, Value: value}, schemaext.Object{Ref: second, Value: value})
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	first.Kind = "example.org/wrong"
	_, err = schemaext.NewObjects(schemaext.Object{Ref: first, Value: value})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

func TestCollections_RequireExplicitJSONCodec(t *testing.T) {
	for _, value := range []any{schemaext.Facets{}, schemaext.Object{}, schemaext.Objects{}, schemaext.Coverage{}} {
		_, err := json.Marshal(value)
		qt.New(t).Assert(err, qt.ErrorIs, schemaext.ErrExplicitCodec)
	}
	for _, value := range []any{&schemaext.Facets{}, &schemaext.Object{}, &schemaext.Objects{}, &schemaext.Coverage{}} {
		err := json.Unmarshal([]byte(`{}`), value)
		qt.New(t).Assert(err, qt.ErrorIs, schemaext.ErrExplicitCodec)
	}
}

func TestEqualValues_UsesTheOwnersLocalRepresentation(t *testing.T) {
	left := &widget{ID: widgetKind, Names: []string{"b", "a"}, Order: []int{2, 1}}
	tests := []struct {
		name  string
		right schemaext.Value
		want  bool
	}{
		{name: "set order", right: &widget{ID: widgetKind, Names: []string{"a", "b"}, Order: []int{2, 1}}, want: true},
		{name: "sequence order", right: &widget{ID: widgetKind, Names: []string{"a", "b"}, Order: []int{1, 2}}},
		{name: "different kind", right: &widget{ID: otherKind, Names: []string{"b", "a"}, Order: []int{2, 1}}},
		{name: "different concrete type", right: &otherWidget{ID: widgetKind}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			equal, err := schemaext.EqualValues(left, test.right)
			c.Assert(err, qt.IsNil)
			c.Assert(equal, qt.Equals, test.want)
			c.Assert(left.Names, qt.DeepEquals, []string{"b", "a"})
		})
	}
	c := qt.New(t)
	_, err := schemaext.EqualValues(left, (*widget)(nil))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
