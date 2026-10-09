package chcompare_test

import (
	"context"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

// Common validation is outside this fixture. The test exercises selected
// feature dispatch and operand capture through the neutral comparison path.
type indexPipelineValidation struct{}

func (indexPipelineValidation) ValidateSchema(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
	return schemavalidation.Result{Complete: true}, nil
}

func TestIndexSettingsReachCapturedTableDiffWithoutCommonReplacement(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{
		ID:      "example.org/index-pipeline",
		Targets: []engine.Target{{Name: "clickhouse", Preparation: schemapreparation.Identity{}, Validation: indexPipelineValidation{}}},
		Codecs:  append(chschema.IndexCodecs(), chdiff.IndexCodecs()...),
		FacetComparisons: []engine.FacetComparison{{Target: "clickhouse", OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
			Kinds: []schemaext.Kind{chschema.IndexKind}, ChangeKinds: []schemaext.Kind{chdiff.IndexKind}, Service: chcompare.IndexService{}}},
	}))
	before := &chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}
	after := (&chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 64}).Desired()
	desired := &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: "events.name", StructName: "Event"}},
		Fields:  []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64"}},
		Indexes: []schemamodel.Index{{Name: "by.status", StructName: "Event", Fields: []string{"id"}, Facets: must.Must(schemaext.NewFacets(after))}},
	}
	current := &catalog.Database{
		Tables:  []catalog.Table{{Name: "events.name", Columns: []catalog.Column{{Name: "id", DataType: "UInt64"}}}},
		Indexes: []catalog.Index{{Name: "by.status", TableName: "events.name", Columns: []string{"id"}, Facets: must.Must(schemaext.NewFacets(before))}},
	}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	table := diff.TablesModified[0]
	c.Assert(table.FeatureChanges, qt.HasLen, 1)
	c.Assert(table.FeatureChanges[0].Subject.Name.Source, qt.Equals, "by.status")
	c.Assert(table.FeatureChanges[0].Subject.Parent.Source, qt.Equals, "events.name")
	c.Assert(table.FeatureChanges[0].Value, qt.DeepEquals, &chdiff.Index{Before: before, After: after})
	c.Assert(table.Current.Indexes, qt.HasLen, 1)
	c.Assert(table.Current.Indexes[0].Facets, qt.DeepEquals, current.Indexes[0].Facets)
	c.Assert(table.Desired.Indexes, qt.HasLen, 1)
	c.Assert(table.Desired.Indexes[0].Facets, qt.DeepEquals, desired.Indexes[0].Facets)
}
