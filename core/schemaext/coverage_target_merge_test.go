package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestCoverageMergeDoesNotBorrowAnotherTargetNamespace(t *testing.T) {
	for _, test := range []struct {
		target string
		want   schemaext.KnowledgeState
	}{
		{"ydb", schemaext.Complete}, {"spanner", schemaext.Uninspected},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
			c.Assert(err, qt.IsNil)
			model := registry.Definitions()[0]
			unknown := schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside source"})
			subject := widgetRef("table", "value")
			left, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: unknown}},
				[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: subject, Targets: []string{"ydb"}, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
			c.Assert(err, qt.IsNil)
			right, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: scopedKnowledge(c, schemaext.NamespaceScope{Targets: []string{test.target}})}}, nil)
			c.Assert(err, qt.IsNil)
			merged, err := left.Merge(right)
			c.Assert(err, qt.IsNil)
			selected, err := schemaext.NewTargetSelection("ydb")
			c.Assert(err, qt.IsNil)
			projected, err := merged.ForTarget(selected)
			c.Assert(err, qt.IsNil)
			c.Assert(projected.Lookup(widgetKind, subject).State, qt.Equals, test.want)
		})
	}
}

func TestCoverageMergeDoesNotUnionDisjointSubjectAuthority(t *testing.T) {
	for _, test := range []struct {
		target string
		want   schemaext.KnowledgeState
	}{
		{"ydb", schemaext.Complete}, {"spanner", schemaext.Uninspected},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
			c.Assert(err, qt.IsNil)
			kinds := []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside source"})}}
			subject := widgetRef("table", "value")
			left, err := schemaext.NewCoverage(schemaext.Desired, kinds, []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: subject, Targets: []string{"ydb"}, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
			c.Assert(err, qt.IsNil)
			right, err := schemaext.NewCoverage(schemaext.Desired, kinds, []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: subject, Targets: []string{test.target}, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
			c.Assert(err, qt.IsNil)
			merged, err := left.Merge(right)
			c.Assert(err, qt.IsNil)
			c.Assert(merged.Lookup(widgetKind, subject).State, qt.Equals, test.want)
		})
	}
}

func TestCoverageMergePreservesPossibleAliasLimits(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	model := registry.Definitions()[0]
	subject := widgetRef("table", "value")
	left, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}},
		[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: subject, Targets: []string{"pg"}, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
	c.Assert(err, qt.IsNil)
	domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceKnowledge{
		Scope: schemaext.NamespaceScope{Targets: []string{"postgres"}}, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "permission denied"},
	})
	c.Assert(err, qt.IsNil)
	right, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: domain}}, nil)
	c.Assert(err, qt.IsNil)
	merged, err := left.Merge(right)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Lookup(widgetKind, subject).State, qt.Equals, schemaext.Uninspected)
}
