package revisiontable_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/schemadiff"
)

func TestWithoutRemovesTableFacetCoverageBeforeComparison(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	models := runtime.Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(model schemaext.CodecIdentity) bool {
		return model.Kind == chschema.TableKind && model.Representation == schemaext.Observed
	})]
	identities := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	kept := identities.TableParts("app", "events")
	removed := identities.TableParts("app", revisiontable.Atlas)
	facets := must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}))
	source := &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "app", Name: "events", Facets: facets},
			{Schema: "app", Name: revisiontable.Atlas, Facets: facets},
		},
		FeatureCoverage: must.Must(schemaext.NewCoverage(schemaext.Observed,
			[]schemaext.KindCoverage{{Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were inspected"})}},
			[]schemaext.SubjectCoverage{
				{Kind: chschema.TableKind, Subject: kept, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
				{Kind: chschema.TableKind, Subject: removed, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
			},
		)),
	}
	filtered := revisiontable.Without(source, []string{revisiontable.Atlas})
	diff, err := schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{}, filtered, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
	c.Assert(filtered.Tables, qt.DeepEquals, source.Tables[:1])
	c.Assert(filtered.FeatureCoverage.SubjectRecords(), qt.DeepEquals, source.FeatureCoverage.ForParent(kept).SubjectRecords())
	c.Assert(filtered.FeatureCoverage.KindRecords(), qt.DeepEquals, source.FeatureCoverage.KindRecords())
	c.Assert(filtered.FeatureCoverage.Representation(), qt.Equals, schemaext.Observed)
	c.Assert(source.FeatureCoverage.SubjectRecords(), qt.HasLen, 2)
	c.Assert(source.Tables, qt.HasLen, 2)
}

// TestWithoutRemovesOwnedObjectsAndPreservesUnknownKnowledge leaves a
// revision table out of a ClickHouse read that holds a row policy on it and on
// a kept table. The kept table keeps its policy and every claim about its
// policies, an unknown one included; the revision table loses both, matched by
// name regardless of case, while the kind-wide knowledge stays the read's. The
// source is not changed.
func TestWithoutRemovesOwnedObjectsAndPreservesUnknownKnowledge(t *testing.T) {
	c := qt.New(t)
	identities := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	kept := identities.TableParts("app", "events")
	policy := func(table string) schemaext.Object {
		return schemaext.Object{Ref: chschema.RowPolicyRef("app", table, "updates"),
			Value: &chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true}}}
	}
	source := &catalog.Database{
		Tables:         []catalog.Table{{Schema: "app", Name: "events"}, {Schema: "app", Name: "CUSTOM_REVS"}},
		FeatureObjects: must.Must(schemaext.NewObjects(policy("events"), policy("CUSTOM_REVS"))),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
			{Kind: chschema.RowPolicyKind, Subject: kept, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
			{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("app", "events", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unmodeled option"}},
			{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("app", "CUSTOM_REVS", "limited"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not inspected"}},
		})),
	}
	filtered := revisiontable.Without(source, []string{"custom_revs"})
	c.Assert(filtered.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{chschema.RowPolicyRef("app", "events", "updates")})
	c.Assert(filtered.FeatureCoverage.SubjectRecords(), qt.DeepEquals, source.FeatureCoverage.ForParent(kept).SubjectRecords())
	c.Assert(filtered.FeatureCoverage.SubjectRecords(), qt.HasLen, 2)
	c.Assert(filtered.FeatureCoverage.KindRecords(), qt.DeepEquals, source.FeatureCoverage.KindRecords())
	c.Assert(source.FeatureObjects.Len(), qt.Equals, 2)
	c.Assert(source.FeatureCoverage.SubjectRecords(), qt.HasLen, 3)
}
