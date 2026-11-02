package providerhost

// White-box testing required: the host's index placeholder and the captured
// planning request are private adapter state, and the tables and steps they
// hand an owner decide whether an index change reaches that owner or is
// dropped on the way. The subprocess tests in integration/providercli cover
// the transport.

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

func ownedIndexRules() identifier.Semantics {
	return identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact,
		IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
}

// indexPlanner plans for a provider that owns the table lifecycle and reports
// the indexes its tables hold.
func indexPlanner() commonPlanner {
	return commonPlanner{target: "fixture", assessParents: true, indexes: true}
}

// preparedRecords is a diff whose only table, main.records, the plan leaves
// alone: it is prepared, not created, altered or removed.
func preparedRecords() *difftypes.SchemaDiff {
	rules := ownedIndexRules()
	table := schemamodel.Table{Name: "records", Schema: "main"}
	subject := objectidentity.NewBuilder(rules).TableParts("main", "records")
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules, TablePreparation: &schemapreparation.Capture{Prepared: []schemapreparation.Table{{
		Subject: subject,
		Desired: schemacapture.TableDeclaration{Table: table, Indexes: []schemamodel.Index{{Name: "records_email", Fields: []string{"email"}}}},
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "records", Schema: "main"}},
	}}}}
}

// A table the plan creates holds the indexes the diff carries beside it, so
// the owner creates them with the table.
func TestOwnedLifecycleCreatedTableHoldsItsIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)
	rules := ownedIndexRules()
	index := schemamodel.Index{Name: "records_email", Fields: []string{"email"}}
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded:  difftypes.TableChanges{{Name: "main.records", Table: schemamodel.Table{Name: "records", Schema: "main"}}},
		IndexesAdded: difftypes.IndexChanges{{Index: index, TableName: "main.records"}},
	}
	request, _, err := indexPlanner().planningContext(diff)
	c.Assert(err, qt.IsNil)
	c.Assert(request.Tables, qt.HasLen, 1)
	c.Assert(request.Tables[0].Action, qt.Equals, featureplan.CreateTable)
	c.Assert(request.Tables[0].Desired.Indexes, qt.DeepEquals, []schemamodel.Index{index})
}

// A table whose only change is an index is altered, so its owner compares the
// declared indexes with the held ones; an addition and a removal both do it.
func TestOwnedLifecycleIndexChangeAltersItsTable_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name    string
		added   difftypes.IndexChanges
		removed []difftypes.IndexRef
	}{
		{"addition", difftypes.IndexChanges{{Index: schemamodel.Index{Name: "records_email", Fields: []string{"email"}}, TableName: "main.records"}}, nil},
		{"removal", nil, []difftypes.IndexRef{{Name: "records_old", TableName: "main.records"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := preparedRecords()
			diff.IndexesAdded, diff.IndexesRemoved = row.added, row.removed
			request, _, err := indexPlanner().planningContext(diff)
			c.Assert(err, qt.IsNil)
			c.Assert(request.Tables, qt.HasLen, 1)
			c.Assert(request.Tables[0].Action, qt.Equals, featureplan.AlterTable)
			c.Assert(request.Tables[0].Desired.Indexes, qt.HasLen, 1)
		})
	}
}

// Without an index change the prepared table stays context, not an action.
func TestOwnedLifecycleUnchangedTableStaysContext_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, _, err := indexPlanner().planningContext(preparedRecords())
	c.Assert(err, qt.IsNil)
	c.Assert(request.Tables, qt.HasLen, 1)
	c.Assert(request.Tables[0].Action, qt.Equals, featureplan.ParentAction(""))
}

func TestOwnedLifecycleIndexOfAnUnheldTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.IndexesRemoved = []difftypes.IndexRef{{Name: "elsewhere_ix", TableName: "main.elsewhere"}}
	_, _, err := indexPlanner().planningContext(diff)
	c.Assert(err, qt.ErrorMatches, `.*an index change names table "main\.elsewhere", which the provider plan does not hold`)
}

// A provider whose tables the host plans with common statements has no index
// step on the wire, so an index change there is still refused.
func TestCommonTablesStillRefuseIndexChanges_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.IndexesRemoved = []difftypes.IndexRef{{Name: "records_old", TableName: "main.records"}}
	_, err := (commonPlanner{target: "fixture", assessParents: true, commonTables: true, indexes: true}).commonObjectSteps(diff)
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.IndexesRemoved`)
}

// A provider that does not report the indexes its tables hold gives the
// comparison no current side, so an index change there is refused rather
// than planned against an empty table.
func TestOwnedLifecycleWithoutObservedIndexesRefusesIndexChanges_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.IndexesAdded = difftypes.IndexChanges{{Index: schemamodel.Index{Name: "records_email", Fields: []string{"email"}}, TableName: "main.records"}}
	_, err := ownedPlanner().commonObjectSteps(diff)
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.IndexesAdded`)
}

// An index change on a kept table reaches its owner as a step that reads the
// table and creates or drops one index of it, and the removal comes first.
func TestOwnedLifecycleIndexChangeReachesOwner_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &columnClaimer{Runtime: base}
	diff := preparedRecords()
	diff.IndexesAdded = difftypes.IndexChanges{{Index: schemamodel.Index{Name: "records_email", Fields: []string{"email"}}, TableName: "main.records"}}
	diff.IndexesRemoved = []difftypes.IndexRef{{Name: "records_old", TableName: "main.records"}}
	nodes, err := indexPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)

	c.Assert(runtime.request.Tables, qt.HasLen, 1)
	table := runtime.request.Tables[0]
	c.Assert(table.Action, qt.Equals, featureplan.AlterTable)
	builder := objectidentity.NewBuilder(runtime.request.Identifiers)
	index := func(name string) objectidentity.ID {
		return planningChild(table.Subject, builder.IndexParts("main", "records", name))
	}
	// The window markers carry no effects; the index steps carry two.
	steps := slices.DeleteFunc(slices.Clone(runtime.request.CommonSteps), func(step featureplan.CommonStep) bool { return len(step.Effects) != 2 })
	c.Assert(steps, qt.HasLen, 2)
	c.Assert(steps[0].Parent, qt.DeepEquals, table.Subject)
	c.Assert(steps[0].Effects, qt.DeepEquals, []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: index("records_old"), Action: plangraph.Drop}})
	c.Assert(steps[1].Parent, qt.DeepEquals, table.Subject)
	c.Assert(steps[1].Effects, qt.DeepEquals, []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: index("records_email"), Action: plangraph.Create}})
}

func TestOwnedLifecycleUnclaimedIndexChange_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name  string
		leave string
		want  string
	}{
		{"addition", "records_email", `.*unclaimed provider addition of index records_email to table main\.records.*`},
		{"removal", "records_old", `.*unclaimed provider removal of index records_old from table main\.records.*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New()
			c.Assert(err, qt.IsNil)
			runtime := &columnClaimer{Runtime: base, leave: map[string]bool{row.leave: true}}
			diff := preparedRecords()
			diff.IndexesAdded = difftypes.IndexChanges{{Index: schemamodel.Index{Name: "records_email", Fields: []string{"email"}}, TableName: "main.records"}}
			diff.IndexesRemoved = []difftypes.IndexRef{{Name: "records_old", TableName: "main.records"}}
			nodes, err := indexPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// A created table's indexes travel in its declaration and a removed table
// takes its indexes with it, so neither gets an index step.
func TestOwnedLifecycleIndexChangesOfCreatedAndRemovedTables_HappyPath(t *testing.T) {
	c := qt.New(t)
	rules := ownedIndexRules()
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded:    difftypes.TableChanges{{Name: "main.records", Table: schemamodel.Table{Name: "records", Schema: "main"}}},
		TablesRemoved:  difftypes.TableRemovals{{Name: "main.old", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "old", Schema: "main"}}}},
		IndexesAdded:   difftypes.IndexChanges{{Index: schemamodel.Index{Name: "records_email", Fields: []string{"email"}}, TableName: "main.records"}},
		IndexesRemoved: []difftypes.IndexRef{{Name: "old_email", TableName: "main.old"}},
	}
	removals, additions, err := indexPlanner().indexChanges(diff)
	c.Assert(err, qt.IsNil)
	c.Assert(removals, qt.HasLen, 0)
	c.Assert(additions, qt.HasLen, 0)
}

// The host reads back every field of a common index it sends a provider.
func TestCatalogIndexReadsBackAnObservedIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(ownedIndexRules())
	index := catalog.Index{
		ManagedBy: builder.ConstraintParts("main", "records", "records_key"), Name: "records_email", TableName: "records", Schema: "main",
		Columns: []string{"email"}, Parts: []catalog.IndexPart{{Name: "email", Operator: "text_ops", Desc: true, NullsOrder: catalog.NullsOrderFirst, Prefix: "8"}, {Expr: "lower(email)"}},
		IsUnique: true, Definition: "CREATE UNIQUE INDEX", KeyPartsIncomplete: true, Condition: "email IS NOT NULL", Comment: "kept", Invisible: true,
		NullsDistinct: new(false), Method: "global", IncludeColumns: []string{"id"}, StorageParams: map[string]string{"fillfactor": "70"},
		Requires: []objectidentity.ID{builder.SchemaScopedParts(objectidentity.KindExtension, "", "pg_trgm")}, PartitionAttached: true, HashShardBuckets: 4,
	}
	observed, err := observedPlanningIndex(index)
	c.Assert(err, qt.IsNil)
	got, err := catalogIndex(observed)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, index)
}

// acceptingRuntime accepts every schema, so a comparison reaches its rules.
type acceptingRuntime struct{ *engine.Runtime }

func (acceptingRuntime) ValidateSchema(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
	return schemavalidation.Result{Complete: true}, nil
}

// With the rules a provider that reports held indexes registers, an index
// whose key changes under the same name is dropped and built again. Without
// them the comparison pairs indexes by name and plans nothing.
func TestObservedIndexRulesCompareTheKey(t *testing.T) {
	for _, row := range []struct {
		name    string
		rules   schemacomparison.Rules
		removed []difftypes.IndexRef
		added   []difftypes.IndexRef
	}{
		{"with the rules", schemacomparison.Rules{Indexes: observedIndexRules{}},
			[]difftypes.IndexRef{{Name: "orders_by", TableName: "orders"}}, []difftypes.IndexRef{{Name: "orders_by", TableName: "orders"}}},
		{"without them", schemacomparison.Rules{}, nil, nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New(engine.Provider{ID: "example.org/indexes", Targets: []engine.Target{{
				Name: "custom", Preparation: schemapreparation.Identity{}, Comparison: row.rules,
			}}})
			c.Assert(err, qt.IsNil)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Orders", Name: "orders"}},
				Fields: []schemamodel.Field{
					{StructName: "Orders", Name: "status", Type: "TEXT", Nullable: true},
					{StructName: "Orders", Name: "region", Type: "TEXT", Nullable: true},
				},
				Indexes: []schemamodel.Index{{Name: "orders_by", StructName: "Orders", Fields: []string{"status"}}},
			}
			current := &catalog.Database{
				Tables: []catalog.Table{{Name: "orders", Type: "TABLE", Columns: []catalog.Column{
					{Name: "status", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "YES", OrdinalPosition: 1},
					{Name: "region", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "YES", OrdinalPosition: 2},
				}}},
				Indexes: []catalog.Index{{Name: "orders_by", TableName: "orders", Columns: []string{"region"}}},
			}
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: "custom"}, nil, acceptingRuntime{Runtime: base})
			c.Assert(err, qt.IsNil)
			c.Assert(diff.IndexesRemoved, qt.DeepEquals, row.removed)
			c.Assert(diff.IndexesAdded.Refs(), qt.DeepEquals, row.added)
		})
	}
}
