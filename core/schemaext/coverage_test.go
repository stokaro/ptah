package schemaext_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestCoverage_RuntimeGrowthDoesNotInventAuthority(t *testing.T) {
	c := qt.New(t)
	original, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	coverage, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{
		{Model: original.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
	}, nil)
	c.Assert(err, qt.IsNil)
	document, err := original.EncodeCoverage(context.Background(), schemaext.Desired, coverage)
	c.Assert(err, qt.IsNil)
	grown, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(otherKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	decoded, err := grown.DecodeCoverage(context.Background(), document)
	c.Assert(err, qt.IsNil)
	ref := widgetRef("table", "name")
	c.Assert(decoded.Lookup(widgetKind, ref).State, qt.Equals, schemaext.Complete)
	c.Assert(decoded.Lookup(otherKind, ref).State, qt.Equals, schemaext.Uninspected)
	c.Assert((schemaext.Coverage{}).Lookup(widgetKind, ref).State, qt.Equals, schemaext.Uninspected)
	changed := widgetCodec(widgetKind, schemaext.Desired)
	changed.Definition = json.RawMessage(`{"revision":"changed model"}`)
	incompatible, err := schemaext.NewRegistry(owned(changed))
	c.Assert(err, qt.IsNil)
	_, err = incompatible.DecodeCoverage(context.Background(), document)
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
}

func TestCoverage_PreserveAbsenceDefaultsAndUnknownState(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	states := []schemaext.Knowledge{
		{State: schemaext.Absent}, {State: schemaext.Defaulted},
		{State: schemaext.Uninspected, Reason: "not requested"},
		{State: schemaext.Unrepresentable, Reason: "unknown field"},
	}
	for _, knowledge := range states {
		t.Run(string(knowledge.State), func(t *testing.T) {
			c := qt.New(t)
			ref := widgetRef("table", "name")
			coverage, err := schemaext.NewCoverage(schemaext.Desired,
				[]schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
				[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: ref, Knowledge: knowledge}})
			c.Assert(err, qt.IsNil)
			document, err := registry.EncodeCoverage(context.Background(), schemaext.Desired, coverage)
			c.Assert(err, qt.IsNil)
			decoded, err := registry.DecodeCoverage(context.Background(), document)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.Lookup(widgetKind, ref), qt.Equals, knowledge)
			c.Assert(decoded.Lookup(widgetKind, widgetRef("another", "name")).State, qt.Equals, schemaext.Complete)
		})
	}
}

func TestCoverage_RefuseInvalidClaims(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Observed)))
	c.Assert(err, qt.IsNil)
	kind := schemaext.KindCoverage{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}
	_, err = schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{kind, kind}, nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	_, err = schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{kind}, nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err = schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{kind}, []schemaext.SubjectCoverage{
		{Kind: widgetKind, Subject: widgetRef("table", "name"), Knowledge: schemaext.Knowledge{State: schemaext.Defaulted}},
	})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err = schemaext.NewCoverage(schemaext.Observed, nil, []schemaext.SubjectCoverage{
		{Kind: widgetKind, Subject: widgetRef("table", "name"), Knowledge: schemaext.Knowledge{State: schemaext.Absent}},
	})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	ref := widgetRef("table", "name")
	ref.Name.Normalized = ""
	_, err = schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{kind}, []schemaext.SubjectCoverage{
		{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
	})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	kind.Knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: " \t"}
	_, err = schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{kind}, nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
