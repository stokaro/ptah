package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
)

func TestFacetTargetScopePreservesExclusionsAndSnapshots(t *testing.T) {
	c := qt.New(t)
	targets := []string{" ALTERNATE "}
	original := must.Must(schemaext.NewFacets(&widget{ID: widgetKind}, &widget{ID: otherKind}))
	scoped := must.Must(original.WithTargetScope(widgetKind, targets...))
	targets[0] = "changed"
	c.Assert(scoped.TargetScope(widgetKind), qt.DeepEquals, []string{"alternate"})
	returned := scoped.TargetScope(widgetKind)
	returned[0] = "changed"
	local := must.Must(schemaext.NewTargetSelection("custom", "alternate"))
	foreign := must.Must(schemaext.NewTargetSelection("foreign"))
	selected := must.Must(scoped.ForTarget(local))
	c.Assert(selected.Kinds(), qt.DeepEquals, original.Kinds())
	excluded := must.Must(scoped.ForTarget(foreign))
	c.Assert(excluded.Kinds(), qt.DeepEquals, []schemaext.Kind{otherKind})
	c.Assert(excluded.DeclaredKinds(), qt.DeepEquals, original.Kinds())
	c.Assert(excluded.TargetScope(widgetKind), qt.DeepEquals, []string{"alternate"})
	again := must.Must(excluded.ForTarget(foreign))
	c.Assert(again, qt.DeepEquals, excluded)
	c.Assert(excluded.Without(otherKind).IsZero(), qt.IsFalse)
	c.Assert(excluded.Without(otherKind).Without(widgetKind).IsZero(), qt.IsTrue)
	_, err := excluded.ForTarget(local)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err = scoped.ForTarget(schemaext.TargetSelection{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err = excluded.With(&widget{ID: widgetKind})
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	_, err = excluded.Merge(original)
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(original.TargetScope(widgetKind), qt.HasLen, 0)
	replaced := must.Must(scoped.Replace(&widget{ID: widgetKind, Count: 2}))
	c.Assert(replaced.TargetScope(widgetKind), qt.DeepEquals, []string{"alternate"})
}

func TestFacetBindingsRefuseMalformedSourceNames(t *testing.T) {
	for _, names := range [][]string{{""}, {"not a target"}, {"custom", "CUSTOM"}} {
		t.Run(names[0], func(t *testing.T) {
			c := qt.New(t)
			facets := must.Must(schemaext.NewFacets(&widget{ID: widgetKind}))
			result, err := facets.WithTargetScope(widgetKind, names...)
			c.Assert(err, qt.IsNotNil)
			c.Assert(result.IsZero(), qt.IsTrue)
		})
	}
}

func TestFacetCodecPreservesBindingsWithoutExcludedModel(t *testing.T) {
	c := qt.New(t)
	source := must.Must(schemaext.NewFacets(&widget{ID: widgetKind}, &widget{ID: otherKind}))
	source = must.Must(source.WithTargetScope(widgetKind, "foreign"))
	selected := must.Must(source.ForTarget(must.Must(schemaext.NewTargetSelection("custom"))))
	registry := must.Must(schemaext.NewRegistry(owned(widgetCodec(otherKind, schemaext.Desired))))
	wire := must.Must(registry.EncodeFacets(t.Context(), schemaext.Desired, selected))
	restored := must.Must(registry.DecodeFacets(t.Context(), schemaext.Desired, wire))
	wire[1].Targets[0] = "changed"
	c.Assert(restored, qt.DeepEquals, selected)
	snapshot := must.Must(registry.SnapshotFacets(t.Context(), schemaext.Desired, restored))
	c.Assert(snapshot, qt.DeepEquals, selected)
	_, err := registry.EncodeFacets(t.Context(), schemaext.Desired, source)
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	_, err = registry.DecodeFacets(t.Context(), schemaext.Desired, []schemaext.EncodedFacet{{Kind: widgetKind}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err = registry.DecodeFacets(t.Context(), schemaext.Desired, []schemaext.EncodedFacet{{Kind: widgetKind, Targets: []string{"foreign"}}, {Kind: widgetKind, Targets: []string{"foreign"}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
}
