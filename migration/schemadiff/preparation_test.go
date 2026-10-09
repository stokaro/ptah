package schemadiff_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestClickHousePreparationKeepsTableKeysIndependent(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "first", StructName: "Event", Overrides: map[string]map[string]string{"clickhouse": {"order_by": "id"}}},
			{Name: "second", StructName: "Event", Overrides: map[string]map[string]string{"clickhouse": {"order_by": "ts"}}},
		},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "UInt64"},
			{Name: "ts", StructName: "Event", Type: "DateTime"},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{
		{Name: "first", Columns: []catalog.Column{
			{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "ts", DataType: "DateTime", ColumnType: "DateTime", IsNullable: "NO"},
		}},
		{Name: "second", Columns: []catalog.Column{
			{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"},
			{Name: "ts", DataType: "DateTime", ColumnType: "DateTime", IsNullable: "NO", IsPrimaryKey: true},
		}},
	}}
	observePreparationTables(c, current, map[string]string{"first": "id", "second": "ts"})
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "clickhouse", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(diff.TablePreparation, qt.IsNotNil)
	c.Assert(diff.TablePreparation.Prepared[0].Desired.Fields[0].Primary, qt.IsTrue)
	c.Assert(diff.TablePreparation.Prepared[0].Desired.Fields[1].Primary, qt.IsFalse)
	c.Assert(diff.TablePreparation.Prepared[1].Desired.Fields[0].Primary, qt.IsFalse)
	c.Assert(diff.TablePreparation.Prepared[1].Desired.Fields[1].Primary, qt.IsTrue)
	c.Assert(diff.TablePreparation.Source[0].Desired.Fields[0].Primary, qt.IsFalse)
	c.Assert(desired.Fields[0].Primary, qt.IsFalse)
	desired.Fields[0].Name = "changed after comparison"
	current.Tables[0].Columns[0].Name = "changed observation"
	c.Assert(diff.TablePreparation.Source[0].Desired.Fields[0].Name, qt.Equals, "id")
	c.Assert(diff.TablePreparation.Source[0].Current.Table.Columns[0].Name, qt.Equals, "id")
}

type comparisonPreparation struct {
	*engine.Runtime
	prepare func(context.Context, schemapreparation.Request) (schemapreparation.Result, error)
}

func (s comparisonPreparation) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	return s.prepare(ctx, request)
}

func TestComparisonPreparationPreservesUnknownTableExistence(t *testing.T) {
	c := qt.New(t)
	var received schemapreparation.Request
	runtime := comparisonPreparation{Runtime: must.Must(builtin.New()), prepare: func(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		received = request.Clone()
		return (schemapreparation.Identity{}).PrepareTables(ctx, request)
	}}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Schema: "unread", Name: "events", StructName: "Event"}}}
	current := &catalog.Database{NotDescribed: coverage.Set{}.WithObject(coverage.Schema, "unread")}
	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, current, &config.CompareOptions{Dialect: "postgres"}, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff, qt.IsNotNil)
	c.Assert(received.Tables, qt.HasLen, 1)
	c.Assert(received.Tables[0].CurrentKnowledge.State, qt.Equals, schemaext.Uninspected)
	c.Assert(received.Tables[0].Current.HasTable(), qt.IsFalse)
	c.Assert(diagnostics.Common, qt.HasLen, 1)
}

func TestComparisonRejectsIncompletePreparationTransport(t *testing.T) {
	c := qt.New(t)
	runtime := comparisonPreparation{Runtime: must.Must(builtin.New()), prepare: func(context.Context, schemapreparation.Request) (schemapreparation.Result, error) {
		return schemapreparation.Result{}, nil
	}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{}, &catalog.Database{}, "postgres", runtime)
	c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
	c.Assert(diff, qt.IsNil)
}

// Normalized flags belong to paired-column comparison. CREATE, ADD COLUMN, and
// rebuild input retain the declaration that the renderer consumes.
func TestClickHousePreparationRetainsPlanningDeclarations(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "existing", StructName: "Event", Overrides: map[string]map[string]string{"clickhouse": {"order_by": "(id, ts)"}}},
			{Name: "created", StructName: "Event", Overrides: map[string]map[string]string{"clickhouse": {"order_by": "(id, ts)"}}},
		},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "UInt64"},
			{Name: "ts", StructName: "Event", Type: "DateTime"},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "existing", Columns: []catalog.Column{
		{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO", IsPrimaryKey: true},
	}}}}
	observePreparationTables(c, current, map[string]string{"existing": "id, ts"})
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "clickhouse", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
	c.Assert(diff.TablesAdded[0].Fields, qt.DeepEquals, desired.Fields)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	modified := diff.TablesModified[0]
	c.Assert(modified.ColumnsModified, qt.HasLen, 0)
	c.Assert(modified.ColumnsAdded, qt.HasLen, 1)
	c.Assert(modified.ColumnsAdded[0].Name, qt.Equals, "ts")
	c.Assert(modified.ColumnsAdded[0].Primary, qt.IsFalse)
	c.Assert(modified.Desired.Fields, qt.DeepEquals, desired.Fields)
	c.Assert(diff.TablePreparation.Prepared[0].Desired.Fields[1].Primary, qt.IsTrue)
	c.Assert(diff.TablePreparation.Prepared[1].CurrentKnowledge.State, qt.Equals, schemaext.Absent)
}

func observePreparationTables(c *qt.C, db *catalog.Database, keys map[string]string) {
	c.Helper()
	c.Assert(keys, qt.HasLen, len(db.Tables))
	runtime := must.Must(builtin.New())
	identities := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	var subjects []schemaext.SubjectCoverage
	for i, table := range db.Tables {
		key, found := keys[table.Name]
		c.Assert(found, qt.IsTrue)
		db.Tables[i].Facets = must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: key, PrimaryKey: key}))
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: chschema.TableKind, Subject: identities.TableParts(db.Tables[i].Schema, db.Tables[i].Name), Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	for _, model := range runtime.Codecs().Definitions() {
		if model.Kind == chschema.TableKind && model.Representation == schemaext.Observed {
			db.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, subjects))
		}
	}
}
