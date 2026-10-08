package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestCoverageMerge_UnknownSourcesAndSubjectLimitsSurvive(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	known, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil)
	c.Assert(err, qt.IsNil)
	ref := widgetRef("items", "updates")
	limited, err := schemaext.NewCoverage(schemaext.Desired, known.KindRecords(), []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown setting"}}})
	c.Assert(err, qt.IsNil)
	merged, err := known.Merge(limited)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Lookup(widgetKind, ref), qt.Equals, limited.Lookup(widgetKind, ref))
	c.Assert(merged.Lookup(widgetKind, widgetRef("items", "another")).State, qt.Equals, schemaext.Complete)
	for _, pair := range [][2]schemaext.Coverage{{known, {}}, {{}, known}} {
		merged, err := pair[0].Merge(pair[1])
		c.Assert(err, qt.IsNil)
		c.Assert(merged.Lookup(widgetKind, ref).State, qt.Equals, schemaext.Uninspected)
	}
}

func TestCoverageMerge_RejectsConflictingClaimsAndDefinitions(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	kinds := []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}
	absent, err := schemaext.NewCoverage(schemaext.Desired, kinds, []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: widgetRef("t", "v"), Knowledge: schemaext.Knowledge{State: schemaext.Absent}}})
	c.Assert(err, qt.IsNil)
	defaulted, err := schemaext.NewCoverage(schemaext.Desired, kinds, []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: widgetRef("t", "v"), Knowledge: schemaext.Knowledge{State: schemaext.Defaulted}}})
	c.Assert(err, qt.IsNil)
	_, err = absent.Merge(defaulted)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	kinds[0].Model.Definition = "different definition"
	different, err := schemaext.NewCoverage(schemaext.Desired, kinds, nil)
	c.Assert(err, qt.IsNil)
	_, err = absent.Merge(different)
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
}
