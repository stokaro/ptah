package clickhouse_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func observedStorageCoverage() schemaext.Coverage {
	models := must.Must(builtin.New()).Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
	})]
	return must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil))
}

// A direct planner fixture carries the same complete parent observation as a
// comparison. An empty sorting key keeps these common-column tests independent
// of key mutation restrictions.
func capturedColumnFixture(table schemamodel.Table) difftypes.TableDiff {
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}
	table.Facets = must.Must(schemaext.NewFacets(storage.Desired()))
	return difftypes.TableDiff{
		TableName: table.QualifiedName(),
		Desired:   schemacapture.TableDeclaration{Table: table},
		Current:   schemacapture.TableObservation{Table: catalog.Table{Name: table.Name, Schema: table.Schema, Facets: must.Must(schemaext.NewFacets(storage))}, FeatureCoverage: observedStorageCoverage()},
	}
}

func TestTTLColumnReplacementPlansBothDirections(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", TTL: "old_time + INTERVAL 1 DAY"}
	after := before.Desired()
	after.TTL.Value = "new_time + INTERVAL 7 DAY"
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(after))}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64"}, {Name: "new_time", StructName: "Event", Type: "DateTime"}},
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: "events", Facets: must.Must(schemaext.NewFacets(before)),
		Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"}, {Name: "old_time", DataType: "DateTime", ColumnType: "DateTime", IsNullable: "NO"}},
	}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TABLE events ADD COLUMN new_time DateTime",
		"ALTER TABLE events MODIFY TTL new_time + INTERVAL 7 DAY",
		"ALTER TABLE events DROP COLUMN old_time",
	})
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "clickhouse",
	})
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	add, ttl, drop := strings.Index(reverse, "ADD COLUMN old_time"), strings.Index(reverse, "MODIFY TTL old_time"), strings.Index(reverse, "DROP COLUMN new_time")
	c.Assert(add >= 0 && add < ttl && ttl < drop, qt.IsTrue, qt.Commentf("%s", reverse))
	c.Assert(reverse, qt.Contains, "cannot recover rows or values")
	c.Assert(plan.Reverse.Recovery, qt.HasLen, 1)
	c.Assert(current.Tables[0].Facets, qt.DeepEquals, must.Must(schemaext.NewFacets(before)))
}
