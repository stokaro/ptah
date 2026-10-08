package schemaext_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestObjectProjectionPreservesSiblingsAndKnowledgeLimits(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Observed)), owned(widgetCodec(otherKind, schemaext.Observed)))
	c.Assert(err, qt.IsNil)
	models := slices.DeleteFunc(registry.Definitions(), func(definition schemaext.CodecIdentity) bool { return definition.Kind != widgetKind })
	c.Assert(models, qt.HasLen, 1)
	model := models[0]
	unknown := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only selected objects were read"}
	created, changed, dropped, retained := widgetRef("table.with.dot", "created"), widgetRef("table.with.dot", "changed"), widgetRef("table.with.dot", "dropped"), widgetRef("table.with.dot", "retained")
	coverage, err := schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: unknown}},
		[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: created, Knowledge: schemaext.Knowledge{State: schemaext.Absent}}})
	c.Assert(err, qt.IsNil)
	objects, err := schemaext.NewObjects(
		schemaext.Object{Ref: changed, Value: &widget{ID: widgetKind, Count: 1}},
		schemaext.Object{Ref: dropped, Value: &widget{ID: widgetKind, Count: 2}},
		schemaext.Object{Ref: retained, Value: &widget{ID: widgetKind, Count: 3}},
	)
	c.Assert(err, qt.IsNil)
	projection := &widget{ID: widgetKind, Count: 4, Names: []string{"independent"}}
	result, err := registry.ProjectObjects(t.Context(), schemaext.ObjectState{Objects: objects, Coverage: coverage}, []schemaext.ObjectProjection{
		{Subject: created, Value: &widget{ID: widgetKind, Count: 5}},
		{Subject: changed, Value: projection},
		{Subject: dropped},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Objects.Len(), qt.Equals, 3)
	value, found, err := result.Objects.Get(changed)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Value.(*widget).Count, qt.Equals, uint64(4))
	projection.Names[0] = "mutated"
	c.Assert(value.Value.(*widget).Names, qt.DeepEquals, []string{"independent"})
	value, found, err = result.Objects.Get(retained)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Value.(*widget).Count, qt.Equals, uint64(3))
	_, found, err = result.Objects.Get(dropped)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
	c.Assert(result.Coverage.Lookup(widgetKind, created).State, qt.Equals, schemaext.Complete)
	c.Assert(result.Coverage.Lookup(widgetKind, dropped).State, qt.Equals, schemaext.Absent)
	c.Assert(result.Coverage.Lookup(widgetKind, retained), qt.Equals, unknown)
	c.Assert(result.Coverage.Lookup(otherKind, retained).State, qt.Equals, schemaext.Uninspected)
	c.Assert(coverage.Lookup(widgetKind, created).State, qt.Equals, schemaext.Absent)
	value, found, err = objects.Get(changed)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Value.(*widget).Count, qt.Equals, uint64(1))
}

func TestObjectProjectionRefusesUnknownOrContradictorySourceState(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Observed)))
	c.Assert(err, qt.IsNil)
	ref := widgetRef("table", "object")
	enrolled := []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}
	present, err := schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &widget{ID: widgetKind}})
	c.Assert(err, qt.IsNil)
	for _, test := range []struct {
		name    string
		objects schemaext.Objects
		kinds   []schemaext.KindCoverage
		claims  []schemaext.SubjectCoverage
	}{
		{name: "unenrolled model", objects: present},
		{name: "unknown missing object", kinds: enrolled, claims: []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}}}},
		{name: "unreadable present object", kinds: enrolled, objects: present, claims: []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown setting"}}}},
		{name: "present object claimed absent", kinds: enrolled, objects: present, claims: []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Absent}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			coverage, err := schemaext.NewCoverage(schemaext.Observed, test.kinds, test.claims)
			c.Assert(err, qt.IsNil)
			result, err := registry.ProjectObjects(t.Context(), schemaext.ObjectState{Objects: test.objects, Coverage: coverage}, []schemaext.ObjectProjection{{Subject: ref}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Objects.Len(), qt.Equals, 0)
			c.Assert(result.Coverage.IsZero(), qt.IsTrue)
		})
	}
}

func TestObjectProjectionValidatesCompleteBatchAndContext(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func([]schemaext.ObjectProjection) []schemaext.ObjectProjection
	}{
		{name: "duplicate subjects", edit: func(p []schemaext.ObjectProjection) []schemaext.ObjectProjection { return append(p, p[0]) }},
		{name: "typed nil", edit: func(p []schemaext.ObjectProjection) []schemaext.ObjectProjection {
			p[1].Value = (*widget)(nil)
			return p
		}},
		{name: "missing subject", edit: func(p []schemaext.ObjectProjection) []schemaext.ObjectProjection {
			p[1].Subject.Name.Source = ""
			return p
		}},
		{name: "wrong kind", edit: func(p []schemaext.ObjectProjection) []schemaext.ObjectProjection {
			p[1].Value = &widget{ID: otherKind}
			return p
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Observed)), owned(widgetCodec(otherKind, schemaext.Observed)))
			c.Assert(err, qt.IsNil)
			var kinds []schemaext.KindCoverage
			for _, definition := range registry.Definitions() {
				kinds = append(kinds, schemaext.KindCoverage{Model: definition, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
			}
			coverage, err := schemaext.NewCoverage(schemaext.Observed, kinds, nil)
			c.Assert(err, qt.IsNil)
			projections := []schemaext.ObjectProjection{
				{Subject: widgetRef("table", "one"), Value: &widget{ID: widgetKind}},
				{Subject: widgetRef("table", "two"), Value: &widget{ID: widgetKind}},
			}
			result, err := registry.ProjectObjects(t.Context(), schemaext.ObjectState{Coverage: coverage}, test.edit(projections))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Objects.Len(), qt.Equals, 0)
			c.Assert(result.Coverage.IsZero(), qt.IsTrue)
		})
	}
	t.Run("canceled", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := (schemaext.Registry{}).ProjectObjects(ctx, schemaext.ObjectState{}, nil)
		c.Assert(err, qt.ErrorIs, context.Canceled)
		c.Assert(result.Objects.Len(), qt.Equals, 0)
		c.Assert(result.Coverage.IsZero(), qt.IsTrue)
	})
}
