package modelast_test

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

const loweringKind schemaext.Kind = "example.org/lowering-probe"

type loweringValue struct{ Name string }

func (*loweringValue) Kind() schemaext.Kind { return loweringKind }
func (v *loweringValue) Clone() schemaext.Value {
	return &loweringValue{Name: v.Name}
}
func (v *loweringValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*loweringValue)
	return ok && w != nil && *v == *w
}

func loweringFeatureFixture(dialect string) schemamodel.Database {
	builder := objectidentity.NewBuilder(identifier.ForDialect(dialect))
	ref := builder.ColumnParts("", "table.with.dot", "child.with.dot")
	ref.Kind = objectidentity.Kind(loweringKind)
	return schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "table.with.dot", StructName: "Probe"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Probe", Type: "BIGINT", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: ref, Value: &loweringValue{Name: "preserved"},
		})),
		FeatureCoverage: loweringCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil),
	}
}

func loweringCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) schemaext.Coverage {
	return must.Must(schemaext.NewCoverage(representation, []schemaext.KindCoverage{{
		Model: schemaext.CodecIdentity{
			Owner: "example.org/lowering", Kind: loweringKind, Representation: representation, Version: 1, Definition: "fixture-definition",
		},
		Knowledge: knowledge,
	}}, subjects))
}

func TestCollectDatabase_PreservesFeatureChildrenOnEveryTarget(t *testing.T) {
	c := qt.New(t)
	spellings := acceptedSpellings(c)
	c.Assert(len(spellings) > 0, qt.IsTrue)
	for _, dialect := range spellings {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			database := loweringFeatureFixture(dialect)
			list, err := modelast.CollectDatabase(database, dialect, modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.IsNil)
			c.Assert(list.Statements, qt.HasLen, 1)
			table, ok := list.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.OwnedObjects.Len(), qt.Equals, 1)
			c.Assert(table.OwnedObjects, qt.DeepEquals, database.FeatureObjects)
			c.Assert(database.FeatureCoverage, qt.DeepEquals, loweringFeatureFixture(dialect).FeatureCoverage)
		})
	}
}

func TestCollectDatabase_PreservesTableFacetsOnEveryTarget(t *testing.T) {
	c := qt.New(t)
	spellings := acceptedSpellings(c)
	c.Assert(len(spellings) > 0, qt.IsTrue)
	for _, dialect := range spellings {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			database := loweringFeatureFixture(dialect)
			facets := must.Must(schemaext.NewFacets(&loweringValue{Name: "table settings"}))
			database.Tables[0].Facets = must.Must(facets.WithTargetScope(loweringKind, "external"))
			list, err := modelast.CollectDatabase(database, dialect, modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.IsNil)
			c.Assert(list.Statements, qt.HasLen, 1)
			table, ok := list.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Facets, qt.DeepEquals, database.Tables[0].Facets)
			c.Assert(table.OwnedObjects, qt.DeepEquals, database.FeatureObjects)
			value, found, err := schemaext.FacetAs[*loweringValue](table.Facets, loweringKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			value.Name = "consumer copy"
			original, _, err := schemaext.FacetAs[*loweringValue](database.Tables[0].Facets, loweringKind)
			c.Assert(err, qt.IsNil)
			c.Assert(original.Name, qt.Equals, "table settings")
			c.Assert(table.Facets.TargetScope(loweringKind), qt.DeepEquals, []string{"external"})
		})
	}
}

// A materialized view's facets travel on its CREATE MATERIALIZED VIEW, where
// the selected renderer interprets a setting the statement states inline, such
// as a refresh schedule, or refuses it.
func TestCollectDatabase_PreservesMaterializedViewFacets(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(schemaext.NewFacets(&loweringValue{Name: "view settings"}))
	database := schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
		Name: "analytics.daily", StructName: "Daily", Body: "SELECT 1", Facets: must.Must(facets.WithTargetScope(loweringKind, "external")),
	}}}
	list, err := modelast.CollectDatabase(database, "clickhouse", modelast.Lowering{Context: context.Background()})
	c.Assert(err, qt.IsNil)
	c.Assert(list.Statements, qt.HasLen, 1)
	view, ok := list.Statements[0].(*ast.CreateMaterializedViewNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(view.Facets, qt.DeepEquals, database.MaterializedViews[0].Facets)
	c.Assert(view.Facets.TargetScope(loweringKind), qt.DeepEquals, []string{"external"})
}

func TestCollectDatabase_PreservesExcludedTableFacetBindings(t *testing.T) {
	c := qt.New(t)
	database := loweringFeatureFixture("postgres")
	facets := must.Must(schemaext.NewFacets(&loweringValue{Name: "external settings"}))
	facets = must.Must(facets.WithTargetScope(loweringKind, "external"))
	database.Tables[0].Facets = must.Must(facets.ForTarget(must.Must(schemaext.NewTargetSelection("postgres"))))
	list, err := modelast.CollectDatabase(database, "postgres", modelast.Lowering{Context: context.Background()})
	c.Assert(err, qt.IsNil)
	c.Assert(list.Statements, qt.HasLen, 1)
	table, ok := list.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Facets, qt.DeepEquals, database.Tables[0].Facets)
	c.Assert(table.Facets.Len(), qt.Equals, 0)
	c.Assert(table.Facets.DeclaredKinds(), qt.DeepEquals, []schemaext.Kind{loweringKind})
}

func TestCollectDatabase_CoverageDoesNotCreateOrRemoveDeclarations(t *testing.T) {
	for _, knowledge := range []schemaext.Knowledge{
		{State: schemaext.Complete}, {State: schemaext.Uninspected, Reason: "only the declared child is known"},
	} {
		t.Run(fmt.Sprintf("state=%q", knowledge.State), func(t *testing.T) {
			c := qt.New(t)
			database := loweringFeatureFixture("postgres")
			want, err := modelast.CollectDatabase(database, "postgres", modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.IsNil)
			database.FeatureCoverage = loweringCoverage(schemaext.Desired, knowledge, nil)
			got, err := modelast.CollectDatabase(database, "postgres", modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, want)
			c.Assert(got.Statements, qt.HasLen, 1)
		})
	}
}

// Populate every envelope independently of FacetSlots. If a new facet slot is
// omitted from the lowering guard, its populated fixture must still fail.
func unloweredFacetsFixture() schemamodel.Database {
	database := routingFixture()
	value := reflect.ValueOf(&database).Elem()
	for _, collection := range value.Fields() {
		if collection.Kind() != reflect.Slice || collection.Type().Elem().Kind() != reflect.Struct || collection.Len() > 0 {
			continue
		}
		if _, ok := collection.Type().Elem().FieldByName("Facets"); ok {
			collection.Set(reflect.MakeSlice(collection.Type(), 1, 1))
		}
	}
	for _, slot := range loweringFacetSlots(value) {
		slot.Set(reflect.ValueOf(must.Must(schemaext.NewFacets(&loweringValue{Name: "must survive"}))))
	}
	return database
}

func loweringFacetSlots(value reflect.Value) []reflect.Value {
	if value.Type() == reflect.TypeFor[schemaext.Facets]() {
		return []reflect.Value{value}
	}
	var result []reflect.Value
	switch value.Kind() {
	case reflect.Struct:
		for i := range value.NumField() {
			if value.Type().Field(i).IsExported() {
				result = append(result, loweringFacetSlots(value.Field(i))...)
			}
		}
	case reflect.Slice:
		for i := range value.Len() {
			result = append(result, loweringFacetSlots(value.Index(i))...)
		}
	}
	return result
}

func TestCollectDatabase_RefusesUnloweredFacetsBeforeVisiting(t *testing.T) {
	c := qt.New(t)
	database := unloweredFacetsFixture()
	slots := loweringFacetSlots(reflect.ValueOf(&database).Elem())
	c.Assert(len(slots) >= 16, qt.IsTrue)
	lowered := []uintptr{reflect.ValueOf(&database.Tables[0].Facets).Pointer(), reflect.ValueOf(&database.MaterializedViews[0].Facets).Pointer()}
	unsupported := slices.DeleteFunc(slices.Clone(slots), func(slot reflect.Value) bool {
		return slices.Contains(lowered, slot.Addr().Pointer())
	})
	c.Assert(unsupported, qt.HasLen, len(slots)-len(lowered))
	for i, chosen := range unsupported {
		t.Run(fmt.Sprintf("slot-%d", i), func(t *testing.T) {
			c := qt.New(t)
			for _, slot := range slots {
				slot.Set(reflect.ValueOf(schemaext.Facets{}))
			}
			chosen.Set(reflect.ValueOf(must.Must(schemaext.NewFacets(&loweringValue{Name: "retained"}))))
			visited := 0
			err := modelast.WalkDatabase(database, "postgres", func(ast.Node) error { visited++; return nil }, modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(visited, qt.Equals, 0)
			list, err := modelast.CollectDatabase(database, "postgres", modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(list, qt.IsNil)
		})
	}
}

func TestCollectDatabase_RefusesExcludedFacetsWithoutAnASTPlacement(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(schemaext.NewFacets(&loweringValue{Name: "external settings"}))
	facets = must.Must(facets.WithTargetScope(loweringKind, "external"))
	excluded := must.Must(facets.ForTarget(must.Must(schemaext.NewTargetSelection("postgres"))))
	visited := 0
	err := modelast.WalkDatabase(schemamodel.Database{Facets: excluded}, "postgres", func(ast.Node) error { visited++; return nil }, modelast.Lowering{Context: context.Background()})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*no schema-to-AST lowering for feature facet "example.org/lowering-probe".*`)
	c.Assert(visited, qt.Equals, 0)
}

func TestCollectDatabase_RefusesTableFacetClaimedAbsent(t *testing.T) {
	c := qt.New(t)
	database := loweringFeatureFixture("postgres")
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", database.Tables[0].Name)
	database.Tables[0].Facets = must.Must(schemaext.NewFacets(&loweringValue{Name: "present"}))
	database.FeatureCoverage = loweringCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{
		Kind: loweringKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Absent},
	}})
	visited := 0
	err := modelast.WalkDatabase(database, "postgres", func(ast.Node) error { visited++; return nil }, modelast.Lowering{Context: context.Background()})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*declared feature facet.*is marked absent.*`)
	c.Assert(visited, qt.Equals, 0)
}

func TestCollectDatabase_RefusesUnsupportedFeatureState(t *testing.T) {
	declared := loweringFeatureFixture("postgres")
	orphan := declared
	orphan.Tables = nil
	standalone := declared
	ref := standalone.FeatureObjects.Refs()[0]
	ref.Parent = objectidentity.Part{}
	standalone.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &loweringValue{Name: "standalone"}}))
	for _, test := range []struct {
		name     string
		database schemamodel.Database
		want     error
	}{
		{name: "missing parent", database: orphan, want: ptaherr.ErrInvalidSchemaDiff},
		{name: "standalone object", database: standalone, want: ptaherr.ErrUnsupportedFeature},
		{name: "observed coverage", database: schemamodel.Database{FeatureCoverage: loweringCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)}, want: schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			list, err := modelast.CollectDatabase(test.database, "postgres", modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(list, qt.IsNil)
		})
	}
}

func TestCollectDatabase_RefusesUnresolvedFeatureKnowledge(t *testing.T) {
	database := loweringFeatureFixture("postgres")
	unrepresentable := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "requires owner interpretation"}
	for _, test := range []struct {
		name     string
		coverage schemaext.Coverage
	}{
		{name: "unrepresentable kind", coverage: loweringCoverage(schemaext.Desired, unrepresentable, nil)},
		{name: "unrepresentable subject", coverage: loweringCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: loweringKind, Subject: database.FeatureObjects.Refs()[0], Knowledge: unrepresentable}})},
		{name: "default request", coverage: loweringCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: loweringKind, Subject: database.FeatureObjects.Refs()[0], Knowledge: schemaext.Knowledge{State: schemaext.Defaulted}}})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := database
			source.FeatureCoverage = test.coverage
			list, err := modelast.CollectDatabase(source, "postgres", modelast.Lowering{Context: context.Background()})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(list, qt.IsNil)
		})
	}
}

func TestCollectDatabase_RefusesPresentObjectClaimedAbsent(t *testing.T) {
	c := qt.New(t)
	database := loweringFeatureFixture("postgres")
	database.FeatureCoverage = loweringCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{
		Kind: loweringKind, Subject: database.FeatureObjects.Refs()[0], Knowledge: schemaext.Knowledge{State: schemaext.Absent},
	}})
	list, err := modelast.CollectDatabase(database, "postgres", modelast.Lowering{Context: context.Background()})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(list, qt.IsNil)
}
