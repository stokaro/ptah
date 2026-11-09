package safety_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

func ratedColumnCatalog(held string) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{Name: "t", Type: "BASE TABLE", Columns: []catalog.Column{{
		Name: "c", DataType: held, ColumnType: held, TypeIsDeclaredText: true, IsNullable: "YES",
	}}}}}
}

func ratedClickHouseCatalog() *catalog.Database {
	runtime := must.Must(builtin.New())
	models := runtime.Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
	})]
	current := ratedColumnCatalog("DateTime")
	current.Tables[0].Type = "TABLE"
	current.Tables[0].Facets = must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}))
	current.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{
		Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete}),
	}}, nil))
	return current
}

// A rating follows the type the plan writes. The declaration alone can make
// a widening look destructive, or hide the loss of values in SQL Server BIT.
func TestSchemaPlanRatesTheRenderedType(t *testing.T) {
	for _, test := range []struct {
		name, dialect, held, declared string
		current                       *catalog.Database
		caps                          capability.Capabilities
		wantNarrows                   bool
		wantRating                    safety.Severity
	}{
		{name: "SQL Server datetime2 to timestamp", dialect: "sqlserver", held: "DATETIME2(3)", declared: "timestamp", current: ratedColumnCatalog("DATETIME2(3)"), caps: capability.SQLServer2022(), wantRating: safety.Warning},
		{name: "SQL Server int to boolean", dialect: "sqlserver", held: "INT", declared: "boolean", current: ratedColumnCatalog("INT"), caps: capability.SQLServer2022(), wantNarrows: true, wantRating: safety.Destructive},
		{name: "ClickHouse DateTime to timestamp", dialect: "clickhouse", held: "DateTime", declared: "timestamp", current: ratedClickHouseCatalog(), caps: capability.ClickHouse2411(), wantRating: safety.Warning},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "c", Type: test.declared, Nullable: true}},
			}
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, test.current, test.dialect, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.TypeNarrows, qt.Equals, test.wantNarrows)
			c.Assert(column.PreviousType, qt.Equals, test.held)
			nodes, err := planner.GenerateSchemaDiffASTWithOptions(t.Context(), runtime, diff, test.dialect, planext.Options{Capabilities: test.caps})
			c.Assert(err, qt.IsNil)
			c.Assert(safety.HighestAssessment(safety.Assess(nodes)), qt.Equals, test.wantRating)
			c.Assert(safety.HasDestructive(safety.ClassifySchemaDiff(diff)), qt.Equals, test.wantNarrows)
		})
	}
}
