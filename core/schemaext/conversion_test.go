package schemaext_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestConvertCoverage_PreservesKnowledgeWithoutRuntimeGrowth(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(
		owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(widgetKind, schemaext.Observed)),
		owned(widgetCodec(otherKind, schemaext.Desired)), owned(widgetCodec(otherKind, schemaext.Observed)),
	)
	c.Assert(err, qt.IsNil)
	models := slices.DeleteFunc(registry.Definitions(), func(definition schemaext.CodecIdentity) bool {
		return definition.Kind != widgetKind || definition.Representation != schemaext.Observed
	})
	c.Assert(models, qt.HasLen, 1)
	model := models[0]
	for _, knowledge := range []schemaext.Knowledge{
		{State: schemaext.Complete}, {State: schemaext.Absent},
		{State: schemaext.Uninspected, Reason: "not inspected"},
		{State: schemaext.Unrepresentable, Reason: "unsupported server setting"},
	} {
		t.Run(string(knowledge.State), func(t *testing.T) {
			c := qt.New(t)
			ref := widgetRef("items", "updates")
			source, err := schemaext.NewCoverage(schemaext.Observed,
				[]schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
				[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: ref, Knowledge: knowledge}})
			c.Assert(err, qt.IsNil)
			converted, err := registry.ConvertCoverage(t.Context(), schemaext.Observed, schemaext.Desired, source)
			c.Assert(err, qt.IsNil)
			c.Assert(converted.Representation(), qt.Equals, schemaext.Desired)
			c.Assert(converted.KindRecords(), qt.HasLen, 1)
			c.Assert(converted.Lookup(widgetKind, ref), qt.Equals, knowledge)
			c.Assert(converted.Lookup(otherKind, ref).State, qt.Equals, schemaext.Uninspected)
			c.Assert(source.Representation(), qt.Equals, schemaext.Observed)
		})
	}
}

// TestConvertCoverage_ProjectsADefaultRequestAsUnknown pins that a default
// request becomes an unknown observation: the declaration does not hold what
// the owner's default is, so a projection cannot report it observed.
func TestConvertCoverage_ProjectsADefaultRequestAsUnknown(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(widgetKind, schemaext.Observed)))
	c.Assert(err, qt.IsNil)
	source, err := schemaext.NewCoverage(schemaext.Desired,
		[]schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
		[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: widgetRef("t", "v"), Knowledge: schemaext.Knowledge{State: schemaext.Defaulted}}})
	c.Assert(err, qt.IsNil)

	converted, err := registry.ConvertCoverage(t.Context(), schemaext.Desired, schemaext.Observed, source)

	c.Assert(err, qt.IsNil)
	c.Assert(converted.Lookup(widgetKind, widgetRef("t", "v")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(converted.Lookup(widgetKind, widgetRef("t", "w")).State, qt.Equals, schemaext.Complete)
}

func TestConvertCoverage_RejectsWrongDirection(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(widgetKind, schemaext.Observed)))
	c.Assert(err, qt.IsNil)
	source, err := schemaext.NewCoverage(schemaext.Desired,
		[]schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
		[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: widgetRef("t", "v"), Knowledge: schemaext.Knowledge{State: schemaext.Defaulted}}})
	c.Assert(err, qt.IsNil)
	_, err = registry.ConvertCoverage(t.Context(), schemaext.Observed, schemaext.Desired, source)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = registry.ConvertCoverage(ctx, schemaext.Desired, schemaext.Observed, source)
	c.Assert(err, qt.ErrorIs, context.Canceled)
}
