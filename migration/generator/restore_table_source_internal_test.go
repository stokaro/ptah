package generator

// White-box testing required: restored coverage must retain unknown state before
// conversion or planning can refuse it. Public plans cannot expose that input on
// failure, so these controls inspect the rollback source at its assembly boundary.

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

const restoredFeatureKind schemaext.Kind = "example.org/restoration/child"

// restoredKinds is the attached vocabulary of the target these tests restore
// on: the test model is a table's child there.
var restoredKinds = attachedVocabulary{restoredFeatureKind}

// attachedVocabulary names a fixed list of attached kinds on every target.
type attachedVocabulary []schemaext.Kind

func (v attachedVocabulary) ParentKinds(string) ([]schemaext.Kind, error) {
	return slices.Clone(v), nil
}

func TestRestoredTableSourceRetainsCoverageLimits(t *testing.T) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	unknown := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the namespace was not inspected"}
	unrepresentable := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the reader cannot express this child"}
	for _, test := range []struct {
		name      string
		namespace schemaext.Knowledge
		child     schemaext.Knowledge
	}{
		{name: "uninspected namespace", namespace: unknown, child: unknown},
		{name: "unrepresentable namespace", namespace: unrepresentable, child: unrepresentable},
		{name: "unrepresentable child", namespace: complete, child: unrepresentable},
		{name: "known child in an uninspected namespace", namespace: unknown, child: complete},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
			parent := builder.Table("items")
			child := restoredFeatureRef(parent, "updates")
			keptChild := restoredFeatureRef(builder.Table("kept"), "updates")
			captured := restoredFeatureCoverage(c, schemaext.Observed, []schemaext.SubjectCoverage{
				{Kind: restoredFeatureKind, Subject: parent, Knowledge: test.namespace},
				{Kind: restoredFeatureKind, Subject: child, Knowledge: test.child},
			})
			source := &catalog.Database{FeatureCoverage: restoredFeatureCoverage(c, schemaext.Observed, []schemaext.SubjectCoverage{
				{Kind: restoredFeatureKind, Subject: child, Knowledge: complete},
				{Kind: restoredFeatureKind, Subject: keptChild, Knowledge: unrepresentable},
			})}
			diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{
				Table: catalog.Table{Name: "items"}, FeatureCoverage: captured,
			}}}}
			restored, err := restoreTableSource(diff, source, "postgres", restoredKinds)
			c.Assert(err, qt.IsNil)
			c.Assert(restored.FeatureCoverage.Lookup(restoredFeatureKind, parent), qt.DeepEquals, test.namespace)
			c.Assert(restored.FeatureCoverage.Lookup(restoredFeatureKind, child), qt.DeepEquals, test.child)
			c.Assert(restored.FeatureCoverage.Lookup(restoredFeatureKind, restoredFeatureRef(parent, "unlisted")), qt.DeepEquals, test.namespace)
			c.Assert(restored.FeatureCoverage.Lookup(restoredFeatureKind, keptChild), qt.DeepEquals, unrepresentable)
			c.Assert(source.FeatureCoverage.Lookup(restoredFeatureKind, child), qt.DeepEquals, complete)
		})
	}
}

func TestRestoredTableSourceDoesNotBorrowUncapturedKnowledge(t *testing.T) {
	c := qt.New(t)
	parent := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table("items")
	source := &catalog.Database{FeatureCoverage: restoredFeatureCoverage(c, schemaext.Observed, nil)}
	diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}}}}}
	restored, err := restoreTableSource(diff, source, "postgres", restoredKinds)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.FeatureCoverage.Lookup(restoredFeatureKind, parent).State, qt.Equals, schemaext.Uninspected)
	c.Assert(restored.FeatureCoverage.Lookup(restoredFeatureKind, restoredFeatureRef(parent, "updates")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(source.FeatureCoverage.Lookup(restoredFeatureKind, parent).State, qt.Equals, schemaext.Complete)
}

func TestRestoredTableSourceRefusesForeignCoverage(t *testing.T) {
	for _, test := range []struct {
		name      string
		direction schemaext.Representation
		parent    string
	}{
		{name: "declaration used as observation", direction: schemaext.Desired, parent: "items"},
		{name: "another table", direction: schemaext.Observed, parent: "other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parent := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table(test.parent)
			captured := restoredFeatureCoverage(c, test.direction, []schemaext.SubjectCoverage{{Kind: restoredFeatureKind, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})
			diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}, FeatureCoverage: captured}}}}
			restored, err := restoreTableSource(diff, &catalog.Database{}, "postgres", restoredKinds)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(restored, qt.IsNil)
		})
	}
}

func TestRestoredTableSourceRefusesConflictingModelDefinitions(t *testing.T) {
	c := qt.New(t)
	captured := restoredFeatureCoverage(c, schemaext.Observed, nil)
	records := captured.KindRecords()
	records[0].Model.Definition = "another definition"
	sourceCoverage, err := schemaext.NewCoverage(schemaext.Observed, records, nil)
	c.Assert(err, qt.IsNil)
	diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}, FeatureCoverage: captured}}}}
	restored, err := restoreTableSource(diff, &catalog.Database{FeatureCoverage: sourceCoverage}, "postgres", restoredKinds)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(restored, qt.IsNil)
}

// unattachedFeatureKind is a model no table carries on the target, the way
// PostgreSQL default privileges belong to a schema or the whole database.
const unattachedFeatureKind schemaext.Kind = "example.org/restoration/schema-level"

// refusingVocabulary is a runtime that cannot name the attached kinds of the
// target it is asked about.
type refusingVocabulary struct{}

func (refusingVocabulary) ParentKinds(target string) ([]schemaext.Kind, error) {
	return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, target)
}

func TestRestoredTableSourceClaimsOnlyAttachedNamespaces(t *testing.T) {
	unknown := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the reader skipped this table"}
	parent := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table("items")
	for _, test := range []struct {
		name       string
		vocabulary featureplan.ParentVocabulary
		claims     []schemaext.SubjectCoverage
		want       []schemaext.Kind
	}{
		{name: "an attached kind only", vocabulary: restoredKinds, want: []schemaext.Kind{restoredFeatureKind}},
		{name: "a claim the capture made on the table", vocabulary: restoredKinds,
			claims: []schemaext.SubjectCoverage{{Kind: unattachedFeatureKind, Subject: parent, Knowledge: unknown}},
			want:   []schemaext.Kind{restoredFeatureKind, unattachedFeatureKind}},
		{name: "a runtime without a vocabulary", want: []schemaext.Kind{restoredFeatureKind, unattachedFeatureKind}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			captured := twoKindCoverage(c, test.claims)
			diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{
				Table: catalog.Table{Name: "items"}, FeatureCoverage: captured,
			}}}}
			restored, err := restoreTableSource(diff, &catalog.Database{FeatureCoverage: twoKindCoverage(c, nil)}, "postgres", test.vocabulary)
			c.Assert(err, qt.IsNil)
			var kinds []schemaext.Kind
			for _, record := range restored.FeatureCoverage.ForParent(parent).SubjectRecords() {
				kinds = append(kinds, record.Kind)
			}
			c.Assert(kinds, qt.DeepEquals, test.want)
			c.Assert(restored.FeatureCoverage.Lookup(restoredFeatureKind, parent).State, qt.Equals, schemaext.Complete)
		})
	}
}

func TestRestoredTableSourceAsksNoVocabularyWithoutRemovals(t *testing.T) {
	c := qt.New(t)
	source := &catalog.Database{FeatureCoverage: twoKindCoverage(c, nil)}
	restored, err := restoreTableSource(&difftypes.SchemaDiff{}, source, "postgres", refusingVocabulary{})
	c.Assert(err, qt.IsNil)
	c.Assert(restored, qt.Equals, source)
}

func TestRestoredTableSourceRefusesAnUnknownVocabulary(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "items", Current: schemacapture.TableObservation{
		Table: catalog.Table{Name: "items"}, FeatureCoverage: twoKindCoverage(c, nil),
	}}}}
	restored, err := restoreTableSource(diff, &catalog.Database{}, "postgres", refusingVocabulary{})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(restored, qt.IsNil)
}

// twoKindCoverage is an observation that knows both test models completely,
// with the given claims.
func twoKindCoverage(c *qt.C, subjects []schemaext.SubjectCoverage) schemaext.Coverage {
	c.Helper()
	records := make([]schemaext.KindCoverage, 0, 2)
	for _, kind := range []schemaext.Kind{restoredFeatureKind, unattachedFeatureKind} {
		records = append(records, schemaext.KindCoverage{
			Model:     schemaext.CodecIdentity{Owner: "example.org/restoration", Kind: kind, Version: 1, Definition: "captured definition", Representation: schemaext.Observed},
			Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete}),
		})
	}
	coverage, err := schemaext.NewCoverage(schemaext.Observed, records, subjects)
	c.Assert(err, qt.IsNil)
	return coverage
}

func restoredFeatureRef(parent objectidentity.ID, name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(restoredFeatureKind), Schema: parent.Schema, Parent: parent.Name, Name: objectidentity.Part{Source: name, Normalized: name}}
}

func restoredFeatureCoverage(c *qt.C, direction schemaext.Representation, subjects []schemaext.SubjectCoverage) schemaext.Coverage {
	c.Helper()
	coverage, err := schemaext.NewCoverage(direction, []schemaext.KindCoverage{{
		Model:     schemaext.CodecIdentity{Owner: "example.org/restoration", Kind: restoredFeatureKind, Version: 1, Definition: "captured definition", Representation: direction},
		Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete}),
	}}, subjects)
	c.Assert(err, qt.IsNil)
	return coverage
}
