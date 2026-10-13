package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A table made unlogged is made logged again by the rollback, and the forward
// plan writes the change it was asked for.
func TestPlanBidirectionalSchemaDiff_UnloggedTableRollsBack(t *testing.T) {
	for _, test := range []struct {
		name             string
		unlogged         bool
		forward, reverse string
		inverse          *pgtable.PersistenceChange
	}{
		{"make unlogged", true, "UNLOGGED", "LOGGED", &pgtable.PersistenceChange{Before: &pgtable.ObservedPersistence{Unlogged: true}}},
		{"make logged", false, "LOGGED", "UNLOGGED", &pgtable.PersistenceChange{After: &pgtable.DesiredPersistence{Unlogged: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Cache", Schema: "app", Name: "cache", PrimaryKey: []string{"id"},
					Facets: must.Must(pgtable.WithPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: test.unlogged}))}},
				Fields:          []schemamodel.Field{{StructName: "Cache", Name: "id", Type: "bigint", Primary: true}},
				FeatureCoverage: must.Must(pgtable.PersistenceCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
			}
			current := &catalog.Database{Tables: []catalog.Table{{Schema: "app", Name: "cache",
				Facets:  must.Must(pgtable.WithObservedPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: !test.unlogged})),
				Columns: []catalog.Column{{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true}},
			}}, FeatureCoverage: must.Must(pgtable.PersistenceCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}
			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, runtime)
			c.Assert(err, qt.IsNil)

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(),
				generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff,
					DesiredSchema: desired,
					CurrentSchema: current,
					Dialect:       platform.Postgres,
					Capabilities:  capability.Postgres18(),
				})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQL(platform.Postgres, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)

			c.Assert(forward, qt.Contains, `ALTER TABLE "app"."cache" SET `+test.forward+`;`)
			c.Assert(plan.Reverse.Diff.TablesModified[0].FeatureChanges[0].Value, qt.DeepEquals,
				test.inverse)
			c.Assert(reverse, qt.Contains, `ALTER TABLE "app"."cache" SET `+test.reverse+`;`)
			expected, err := test.inverse.Reverse()
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified[0].FeatureChanges[0].Value, qt.DeepEquals, expected,
				qt.Commentf("the reversal must not write through to the forward diff"))
		})
	}
}
