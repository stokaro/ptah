package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func TestCoverageNamespaceSurvivesCaptureAndConversion(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(widgetKind, schemaext.Observed)))
	c.Assert(err, qt.IsNil)
	model, foundModel := registry.Identity(widgetKind, schemaext.Observed)
	c.Assert(foundModel, qt.IsTrue)
	domain := scopedKnowledge(c, schemaext.NamespaceScope{Targets: []string{"ydb"}, Database: new(namespacePart("db")), Schema: new(namespacePart("public"))})
	source, err := schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: domain}}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(source.HasTargetScopes(), qt.IsTrue)
	desired, err := registry.ConvertCoverage(t.Context(), schemaext.Observed, schemaext.Desired, source)
	c.Assert(err, qt.IsNil)
	header, err := registry.EncodeCoverageHeader(t.Context(), schemaext.Desired, desired)
	c.Assert(err, qt.IsNil)
	decoded, found, err := registry.DecodeCoverageHeader(t.Context(), schemaext.Desired, "// "+header)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(decoded.Equal(desired), qt.IsTrue)
	subject := widgetRef("table", "value")
	subject.Database, subject.Schema = namespacePart("db"), namespacePart("public")
	for _, test := range []struct {
		target string
		want   schemaext.KnowledgeState
	}{
		{"ydb", schemaext.Complete}, {"spanner", schemaext.Uninspected},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			selection, err := schemaext.NewTargetSelection(test.target)
			c.Assert(err, qt.IsNil)
			projected, err := decoded.ForTarget(selection)
			c.Assert(err, qt.IsNil)
			c.Assert(projected.Lookup(widgetKind, subject).State, qt.Equals, test.want)
			other := subject
			other.Database = namespacePart("other")
			c.Assert(projected.Lookup(widgetKind, other).State, qt.Equals, schemaext.Uninspected)
		})
	}
	document, err := registry.EncodeCoverage(t.Context(), schemaext.Desired, desired)
	c.Assert(err, qt.IsNil)
	document.Format = 1
	_, err = registry.DecodeCoverage(t.Context(), document)
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
}

func TestCoverageNamespaceLimitsAndExplicitSubjects(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	domain := scopedKnowledge(c, schemaext.NamespaceScope{Schema: new(namespacePart("public"))})
	subject := widgetRef("table", "value")
	subject.Schema = namespacePart("public")
	explicit := subject
	explicit.Name = namespacePart("explicit")
	source, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: domain}},
		[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: explicit, Knowledge: schemaext.Knowledge{State: schemaext.Absent}}})
	c.Assert(err, qt.IsNil)
	limits := coverage.Set{}.With(coverage.Object{Kind: coverage.Policy})
	limited, err := source.WithLimits(limits, schemaext.LimitMapping{Header: coverage.Policy, Model: widgetKind, Names: func(id objectidentity.ID) []string { return []string{id.Name.Source} }}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(limited.Lookup(widgetKind, subject).State, qt.Equals, schemaext.Uninspected)
	c.Assert(limited.Lookup(widgetKind, explicit).State, qt.Equals, schemaext.Absent)
	c.Assert(source.Lookup(widgetKind, subject).State, qt.Equals, schemaext.Complete)
	projected := source.SelectSubjects(func(objectidentity.ID) bool { return false }).SelectKinds([]schemaext.Kind{widgetKind}).ForParent(subject)
	c.Assert(projected.Lookup(widgetKind, subject).State, qt.Equals, schemaext.Complete)
	other := subject
	other.Schema = namespacePart("other")
	c.Assert(projected.Lookup(widgetKind, other).State, qt.Equals, schemaext.Uninspected)
	partial, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: scopedKnowledge(c, schemaext.NamespaceScope{Schema: new(namespacePart("other"))})}}, nil)
	c.Assert(err, qt.IsNil)
	merged, err := source.Merge(partial)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Lookup(widgetKind, subject).State, qt.Equals, schemaext.Uninspected)
	c.Assert(merged.Lookup(widgetKind, other).State, qt.Equals, schemaext.Uninspected)
	c.Assert(merged.Lookup(widgetKind, explicit).State, qt.Equals, schemaext.Uninspected)
}
