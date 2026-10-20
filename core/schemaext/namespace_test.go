package schemaext_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func TestObjectNamespaceSurvivesCollectionsCodecsAndCoverage(t *testing.T) {
	c := qt.New(t)
	first := widgetRef("orders", "events")
	first.Database = objectidentity.Part{Source: "tenant", Normalized: "tenant"}
	first.Namespace = objectidentity.NewParts(objectidentity.Part{Source: "a/b", Normalized: "a/b"})
	second := first
	second.Namespace = objectidentity.NewParts(objectidentity.Part{Source: "a", Normalized: "a"}, objectidentity.Part{Source: "b", Normalized: "b"})
	third := first
	third.Database = objectidentity.Part{Source: "other", Normalized: "other"}
	objects, err := schemaext.NewObjects(
		schemaext.Object{Ref: first, Value: &widget{ID: widgetKind}},
		schemaext.Object{Ref: second, Value: &widget{ID: widgetKind}},
		schemaext.Object{Ref: third, Value: &widget{ID: widgetKind}},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(objects.Len(), qt.Equals, 3)
	parent := objectidentity.ID{Kind: objectidentity.KindTable, Database: first.Database, Namespace: first.Namespace, Name: first.Parent}
	c.Assert(objects.ForParent(parent).Refs(), qt.DeepEquals, []objectidentity.ID{first})
	c.Assert(schemaext.CompareRefs(first, second), qt.Not(qt.Equals), 0)
	c.Assert(schemaext.CompareRefs(first, third), qt.Not(qt.Equals), 0)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	wire, err := registry.EncodeObjects(c.Context(), schemaext.Desired, objects)
	c.Assert(err, qt.IsNil)
	encoded, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	c.Assert(json.Unmarshal(encoded, &wire), qt.IsNil)
	restored, err := registry.DecodeObjects(c.Context(), schemaext.Desired, wire)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Refs(), qt.DeepEquals, objects.Refs())
	unknown := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only one namespace was read"}
	coverage, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: unknown}},
		[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(widgetKind, first).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(widgetKind, second), qt.Equals, unknown)
	c.Assert(coverage.Lookup(widgetKind, third), qt.Equals, unknown)
	c.Assert(coverage.ForParent(parent).SubjectRecords(), qt.HasLen, 1)
	other := parent
	other.Namespace = second.Namespace
	c.Assert(coverage.ForParent(other).SubjectRecords(), qt.HasLen, 0)
}
