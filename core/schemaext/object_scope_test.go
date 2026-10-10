package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// scopedWidgets holds an unrestricted widget, one bound to custom through its
// alias, and one bound to another target.
func scopedWidgets(c *qt.C) schemaext.Objects {
	c.Helper()
	return must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: widgetRef("t", "open"), Value: &widget{ID: widgetKind}},
		schemaext.Object{Ref: widgetRef("t", "local"), Value: &widget{ID: widgetKind}, Targets: []string{" ALTERNATE "}},
		schemaext.Object{Ref: widgetRef("t", "foreign"), Value: &widget{ID: widgetKind}, Targets: []string{"foreign"}},
	))
}

// TestObjectTargetScope_ForTarget pins the projection: an unrestricted object
// and one whose binding names the target, here through an alias, remain with
// their bindings, and an object bound elsewhere is absent.
func TestObjectTargetScope_ForTarget(t *testing.T) {
	c := qt.New(t)
	objects := scopedWidgets(c)
	local := must.Must(schemaext.NewTargetSelection("custom", "alternate"))

	selected, err := objects.ForTarget(local)

	c.Assert(err, qt.IsNil)
	c.Assert(selected.Refs(), qt.DeepEquals, []objectidentity.ID{widgetRef("t", "local"), widgetRef("t", "open")})
	kept, _, err := selected.Get(widgetRef("t", "local"))
	c.Assert(err, qt.IsNil)
	c.Assert(kept.Targets, qt.DeepEquals, []string{"alternate"})
	c.Assert(objects.Len(), qt.Equals, 3, qt.Commentf("projection leaves its input alone"))
}

func TestObjectTargetScope_ForTarget_FailurePath(t *testing.T) {
	c := qt.New(t)

	selected, err := scopedWidgets(c).ForTarget(schemaext.TargetSelection{})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(selected.IsZero(), qt.IsTrue)
}

// TestObjectTargetScope_NormalizesTheBinding pins that a collection folds,
// trims and sorts the names and keeps no alias of the caller's slice, and
// that an empty binding is unrestricted.
func TestObjectTargetScope_NormalizesTheBinding(t *testing.T) {
	c := qt.New(t)
	targets := []string{" Postgres", "cockroachdb"}
	objects := must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: widgetRef("t", "w"), Value: &widget{ID: widgetKind}, Targets: targets},
		schemaext.Object{Ref: widgetRef("t", "empty"), Value: &widget{ID: widgetKind}, Targets: make([]string, 0)},
	))
	targets[0] = "changed"

	bound, _, err := objects.Get(widgetRef("t", "w"))
	c.Assert(err, qt.IsNil)
	c.Assert(bound.Targets, qt.DeepEquals, []string{"cockroachdb", "postgres"})
	bound.Targets[0] = "changed"
	again, _, err := objects.Get(widgetRef("t", "w"))
	c.Assert(err, qt.IsNil)
	c.Assert(again.Targets, qt.DeepEquals, []string{"cockroachdb", "postgres"})
	empty, _, err := objects.Get(widgetRef("t", "empty"))
	c.Assert(err, qt.IsNil)
	c.Assert(empty.Targets, qt.IsNil)
	c.Assert(objects.HasTargetScopes(), qt.IsTrue)
	c.Assert(objects.Select(func(ref objectidentity.ID) bool { return ref.Name.Source == "empty" }).HasTargetScopes(), qt.IsFalse)
}

func TestObjectTargetScope_RefusesMalformedNames(t *testing.T) {
	for _, names := range [][]string{{""}, {"not a target"}, {"custom", "CUSTOM"}} {
		t.Run(names[0], func(t *testing.T) {
			c := qt.New(t)

			objects, err := schemaext.NewObjects(schemaext.Object{Ref: widgetRef("t", "w"), Value: &widget{ID: widgetKind}, Targets: names})

			c.Assert(err, qt.IsNotNil)
			c.Assert(objects.IsZero(), qt.IsTrue)
		})
	}
}

// TestObjectTargetScope_TravelsWithTheObject pins that a binding survives
// every collection the object is copied into, and that equality ignores it.
func TestObjectTargetScope_TravelsWithTheObject(t *testing.T) {
	c := qt.New(t)
	objects := scopedWidgets(c)
	foreign := widgetRef("t", "foreign")
	other := must.Must(schemaext.NewObjects(schemaext.Object{Ref: widgetRef("u", "w"), Value: &widget{ID: widgetKind}}))
	all := must.Must(objects.All())
	rebuilt := must.Must(schemaext.NewObjects(all...))
	unbound := must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: widgetRef("t", "open"), Value: &widget{ID: widgetKind}},
		schemaext.Object{Ref: widgetRef("t", "local"), Value: &widget{ID: widgetKind}},
		schemaext.Object{Ref: foreign, Value: &widget{ID: widgetKind}},
	))

	for _, copied := range []struct {
		name       string
		collection schemaext.Objects
	}{
		{"rebuilt", rebuilt},
		{"selected", objects.Select(func(objectidentity.ID) bool { return true })},
		{"by parent", objects.ForParent(objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "t", Normalized: "t"}})},
		{"merged", must.Must(objects.Merge(other))},
		{"another replaced", must.Must(objects.Without(widgetRef("t", "open")).With(schemaext.Object{Ref: widgetRef("t", "open"), Value: &widget{ID: widgetKind}}))},
	} {
		bound, found, err := copied.collection.Get(foreign)
		c.Assert(err, qt.IsNil, qt.Commentf(copied.name))
		c.Assert(found, qt.IsTrue, qt.Commentf(copied.name))
		c.Assert(bound.Targets, qt.DeepEquals, []string{"foreign"}, qt.Commentf(copied.name))
	}
	c.Assert(objects.Equal(unbound), qt.IsTrue)
}

// TestObjectTargetScope_Codec pins that encoding carries the binding and
// decoding restores it.
func TestObjectTargetScope_Codec(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired))))
	objects := scopedWidgets(c)

	wire, err := registry.EncodeObjects(t.Context(), schemaext.Desired, objects)
	c.Assert(err, qt.IsNil)
	restored, err := registry.DecodeObjects(t.Context(), schemaext.Desired, wire)

	c.Assert(err, qt.IsNil)
	c.Assert(wire[0].Targets, qt.DeepEquals, []string{"foreign"})
	wire[0].Targets[0] = "changed"
	c.Assert(must.Must(restored.All()), qt.DeepEquals, must.Must(objects.All()))
}
