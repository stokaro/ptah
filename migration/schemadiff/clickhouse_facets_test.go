package schemadiff_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestPreparationDoesNotMaskInvalidSourceIdentity(t *testing.T) {
	c := qt.New(t)
	called := false
	runtime := comparisonPreparation{Runtime: must.Must(builtin.New()), prepare: func(context.Context, schemapreparation.Request) (schemapreparation.Result, error) {
		called = true
		return schemapreparation.Result{}, nil
	}}
	source := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Event"}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, &catalog.Database{}, "clickhouse", runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	var refusal *schemadiff.RefusalError
	c.Assert(err, qt.ErrorAs, &refusal)
	c.Assert(diff, qt.IsNil)
	c.Assert(called, qt.IsFalse)
}

func TestClickHouseResolvedFacetsReachCreatePlanning(t *testing.T) {
	for _, test := range []struct {
		name  string
		key   []string
		parts []schemamodel.PrimaryKeyPart
		want  string
	}{
		{name: "field key", want: "id, tenant"},
		{name: "ordered table key", key: []string{"tenant", "id"}, want: "tenant, id"},
		{name: "ordered key parts", key: []string{"tenant", "id"}, parts: []schemamodel.PrimaryKeyPart{{Name: "tenant"}, {Name: "id"}}, want: "tenant, id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			declared := &chschema.DesiredTable{}
			facets := must.Must(schemaext.NewFacets(declared))
			facets = must.Must(facets.WithTargetScope(chschema.TableKind, "clickhouse"))
			source := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "events", StructName: "Event", PrimaryKey: test.key, PrimaryKeyParts: test.parts, Facets: facets}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64", Primary: true}, {Name: "tenant", StructName: "Event", Type: "UInt64", Primary: true}},
			}
			diff, err := schemadiff.CompareWithDialect(t.Context(), source, &catalog.Database{}, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded, qt.HasLen, 1)
			prepared, found, err := schemaext.FacetAs[*chschema.DesiredTable](diff.TablesAdded[0].Table.Facets, chschema.TableKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(prepared.OrderBy, qt.DeepEquals, chschema.Setting{State: chschema.Explicit, Value: test.want})
			c.Assert(prepared.PrimaryKey, qt.DeepEquals, prepared.OrderBy)
			c.Assert(diff.TablesAdded[0].Table.Facets.TargetScope(chschema.TableKind), qt.DeepEquals, []string{"clickhouse"})
			c.Assert(diff.TablePreparation.Source[0].Desired.Table.Facets, qt.DeepEquals, facets)
			c.Assert(source.Tables[0].Facets, qt.DeepEquals, facets)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "clickhouse", planner.Options{})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, "ORDER BY ("+test.want+")")
			direct, err := builtin.GetOrderedCreateStatements(source, "clickhouse")
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(direct, "\n"), qt.Contains, "ORDER BY ("+test.want+")")
		})
	}
}

func TestClickHouseResolvedFacetsRetainCatalogStateBeforeComparison(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	models := runtime.Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
	})]
	coverage := must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil))
	facets := must.Must(schemaext.NewFacets(&chschema.DesiredTable{}))
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: facets}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64"}},
	}
	observed := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "", Settings: "index_granularity = 4096"}
	current := &catalog.Database{FeatureCoverage: coverage, Tables: []catalog.Table{{
		Name: "events", Facets: must.Must(schemaext.NewFacets(observed)),
		Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"}},
	}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	resolved, found, err := schemaext.FacetAs[*chschema.DesiredTable](diff.TablePreparation.Prepared[0].ResolvedFacets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(resolved, qt.DeepEquals, observed.Desired())
	c.Assert(diff.TablePreparation.Source[0].Desired.Table.Facets, qt.DeepEquals, facets)
	c.Assert(source.Tables[0].Facets, qt.DeepEquals, facets)
}

func TestClickHouseCreationLeavesUnrelatedUnmanagedSettingsAlone(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "existing", StructName: "Existing"},
			{Name: "added", StructName: "Added", Facets: must.Must(schemaext.NewFacets(&chschema.DesiredTable{}))},
		},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Existing", Type: "UInt64"},
			{Name: "id", StructName: "Added", Type: "UInt64", Primary: true},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{{
		Name: "existing", Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"}},
	}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "clickhouse", planner.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, "CREATE TABLE added")
}

func TestClickHouseScopedObservationUsesTheConnectionDatabase(t *testing.T) {
	for _, test := range []struct {
		name   string
		facets schemaext.Facets
	}{
		{"common declaration", schemaext.Facets{}},
		{"partial storage declaration", must.Must(schemaext.NewFacets(&chschema.DesiredTable{OrderBy: chschema.Setting{State: chschema.Explicit, Value: "id"}}))},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			models := runtime.Codecs().Definitions()
			model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
				return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
			})]
			unqualified := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).Table("events")
			coverage := must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{
				Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were inspected"},
			}}, []schemaext.SubjectCoverage{{Kind: chschema.TableKind, Subject: unqualified, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
			observed := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}
			current := &catalog.Database{FeatureCoverage: coverage, Tables: []catalog.Table{{Name: "events",
				Facets: must.Must(schemaext.NewFacets(observed)), Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO", IsPrimaryKey: true}},
			}}}
			source := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: test.facets}}, Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64", Primary: true}}}
			semantics := identifier.ForDialect("clickhouse")
			semantics.DefaultSchema = "tenant_database"
			info := catalog.ServerInfo{Dialect: "clickhouse", Schema: "tenant_database", IdentifierSemantics: semantics}
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), source, current, info, nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse)
			subject := objectidentity.NewBuilder(semantics).Table("events")
			c.Assert(diff.TablePreparation.Source[0].Current.FeatureCoverage.Lookup(chschema.TableKind, subject).State, qt.Equals, schemaext.Complete)
			c.Assert(current.FeatureCoverage.SubjectRecords()[0].Subject, qt.Equals, unqualified)
		})
	}
}
