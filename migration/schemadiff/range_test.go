package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func rangeChangesOf(diff *difftypes.SchemaDiff) []schemaext.ChangeRecord {
	var result []schemaext.ChangeRecord
	for _, change := range diff.FeatureChanges {
		if change.Value.Kind() == pgtype.RangeChangeKind {
			result = append(result, change)
		}
	}
	return result
}

func TestCompareRangeRemovalRequiresDesiredCoverage(t *testing.T) {
	for _, test := range []struct {
		name  string
		state schemaext.KnowledgeState
		count int
	}{
		{"complete", schemaext.Complete, 1}, {"uninspected", schemaext.Uninspected, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := pgtype.Ref(pgtype.RangeKind, "", "span")
			current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgtype.ObservedObject(ref, &pgtype.ObservedRange{Subtype: "int4"})))), FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Observed))}
			desired := &schemamodel.Database{FeatureCoverage: must.Must(pgtype.Coverage(pgtype.RangeKind, schemaext.Desired, schemaext.Knowledge{State: test.state, Reason: "source coverage"}, nil))}
			diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, "postgres", must.Must(builtin.New())))
			c.Assert(rangeChangesOf(diff), qt.HasLen, test.count)
		})
	}
}
