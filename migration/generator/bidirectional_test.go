package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/renderer"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPlanBidirectionalSchemaDiff_MySQLForeignKeyBackingIndexes(t *testing.T) {
	tests := []struct {
		name       string
		prior      []dbschematypes.DBIndex
		wantRemove []types.IndexRef
	}{
		{
			name: "new backing indexes are removed with table-qualified identity",
			wantRemove: []types.IndexRef{
				{Name: "fk_tenant", TableName: "a.b"},
				{Name: "fk_tenant", TableName: "orders"},
			},
		},
		{
			name: "pre-existing same-named index is preserved",
			prior: []dbschematypes.DBIndex{
				{Name: "fk_tenant", TableName: "orders", Columns: []string{"tenant_id"}},
				// This dotted identity must not collide with (table=a.b, name=fk_tenant).
				{Name: "b.fk_tenant", TableName: "a", Columns: []string{"tenant_id"}},
			},
			wantRemove: []types.IndexRef{{Name: "fk_tenant", TableName: "a.b"}},
		},
		{
			name: "pre-existing differently named covering prefix is preserved",
			prior: []dbschematypes.DBIndex{
				{
					Name: "idx_tenant", TableName: "orders",
					Columns: []string{"tenant_id", "created_at"},
				},
			},
			wantRemove: []types.IndexRef{{Name: "fk_tenant", TableName: "a.b"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{
				ConstraintsAdded: []string{"fk_tenant", "fk_tenant"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
					{
						Name: "fk_tenant", TableName: "orders", Type: "FOREIGN KEY",
						Columns: []string{"tenant_id"}, ForeignTable: "tenants", ForeignColumns: []string{"id"},
					},
					{
						Name: "fk_tenant", TableName: "a.b", Type: "FOREIGN KEY",
						Columns: []string{"tenant_id"}, ForeignTable: "tenants", ForeignColumns: []string{"id"},
					},
				},
			}
			current := &dbschematypes.DBSchema{Indexes: tt.prior}

			plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
				Diff:          diff,
				DesiredSchema: &goschema.Database{},
				CurrentSchema: current,
				Dialect:       platform.MySQL,
				Capabilities:  capability.MySQL84(),
				Policy: generator.BidirectionalPlanPolicy{
					Create: generator.ConcurrentIndexDisabled,
					Drop:   generator.ConcurrentIndexDisabled,
				},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.DeepEquals, tt.wantRemove)
			c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsFalse)
		})
	}
}

func TestPlanBidirectionalSchemaDiff_MySQLSameRunCoveringIndexPreventsBackingIndex(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		ConstraintsAdded: []string{"fk_parent"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name: "fk_parent", TableName: "children", Type: "FOREIGN KEY",
			Columns: []string{"parent_id"}, ForeignTable: "parents", ForeignColumns: []string{"id"},
		}},
	}
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_parent", TableName: "children"}})
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Child", Name: "children"}},
		Indexes: []goschema.Index{{
			StructName: "Child", Name: "idx_parent", Fields: []string{"parent_id", "created_at"},
		}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: &dbschematypes.DBSchema{},
		Dialect:       platform.MySQL,
		Capabilities:  capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.DeepEquals, []types.IndexRef{{
		Name: "idx_parent", TableName: "children",
	}})
}

func TestPlanBidirectionalSchemaDiff_MySQLRefusesSameNamedNonCoveringIndex(t *testing.T) {
	c := qt.New(t)
	diff := singleMySQLForeignKeyDiff("children")

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &goschema.Database{},
		CurrentSchema: &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
			Name: "FK_PARENT", TableName: "children", Columns: []string{"other_id"},
		}}},
		Dialect:      platform.MySQL,
		Capabilities: capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(plan, qt.IsNil)
	c.Assert(err, qt.ErrorMatches,
		`cannot add foreign key "fk_parent" on table "children" for dialect "mysql": `+
			`existing same-named index does not cover the foreign-key columns, `+
			`so the server cannot create the required backing index`)
}

func TestPlanBidirectionalSchemaDiff_MySQLRefusesRemovalOfOnlyCoveringIndex(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		ConstraintsAdded: []string{"fk_parent"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name: "fk_parent", TableName: "children", Type: "FOREIGN KEY",
			Columns: []string{"parent_id"}, ForeignTable: "parents", ForeignColumns: []string{"id"},
		}},
	}
	diff.SetIndexRemovals([]types.IndexRef{{Name: "idx_parent", TableName: "children"}})

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &goschema.Database{},
		CurrentSchema: &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
			Name: "idx_parent", TableName: "children", Columns: []string{"parent_id"},
		}}},
		Dialect:      platform.MySQL,
		Capabilities: capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(plan, qt.IsNil)
	c.Assert(err, qt.ErrorMatches,
		`cannot add foreign key "fk_parent" on table "children" for dialect "mysql": every covering index is removed later in the forward plan`)
}

func TestPlanBidirectionalSchemaDiff_MySQLConstraintAdditionCoversForeignKey(t *testing.T) {
	tests := []struct {
		name           string
		constraintName string
		constraintType string
	}{
		{name: "unique", constraintName: "uq_parent", constraintType: "UNIQUE"},
		{name: "primary key", constraintName: "PRIMARY", constraintType: "PRIMARY KEY"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := singleMySQLForeignKeyDiff("children")
			diff.ConstraintsAdded = append([]string{test.constraintName}, diff.ConstraintsAdded...)
			diff.ConstraintsAddedWithTables = append([]types.ConstraintAdditionInfo{{
				Name: test.constraintName, TableName: "children", Type: test.constraintType,
				Columns: []string{"parent_id"},
			}}, diff.ConstraintsAddedWithTables...)

			plan, err := planMySQLBidirectional(diff, &goschema.Database{}, &dbschematypes.DBSchema{})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Not(qt.Contains), types.IndexRef{
				Name: "fk_parent", TableName: "children",
			})
		})
	}
}

func TestPlanBidirectionalSchemaDiff_MySQLCompositeAddedColumnBackingCleanupOrder(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "children", ColumnsAdded: []string{"tenant_id"},
		}},
		ConstraintsAdded: []string{"fk_parent"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name: "fk_parent", TableName: "children", Type: "FOREIGN KEY",
			Columns:        []string{"parent_id", "tenant_id"},
			ForeignTable:   "parents",
			ForeignColumns: []string{"id", "tenant_id"},
		}},
	}
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Child", Name: "children"}},
		Fields: []goschema.Field{
			{StructName: "Child", Name: "parent_id", Type: "BIGINT"},
			{StructName: "Child", Name: "tenant_id", Type: "BIGINT"},
		},
	}

	plan, err := planMySQLBidirectional(diff, desired, &dbschematypes.DBSchema{})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Contains, types.IndexRef{
		Name: "fk_parent", TableName: "children",
	})
	dropForeignKey, dropColumn, dropIndex := mysqlReverseMutationPositions(
		plan.Reverse.Nodes,
		"children",
		"fk_parent",
		"tenant_id",
	)
	c.Assert(dropForeignKey >= 0, qt.IsTrue)
	c.Assert(dropColumn >= 0, qt.IsTrue)
	c.Assert(dropIndex >= 0, qt.IsTrue)
	c.Assert(dropForeignKey < dropColumn, qt.IsTrue)
	c.Assert(dropColumn < dropIndex, qt.IsTrue)
}

func TestPlanBidirectionalSchemaDiff_MySQLReferencedAddedColumnDropsForeignKeyFirst(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "parents", ColumnsAdded: []string{"code"},
		}},
		ConstraintsAdded: []string{"fk_parent_code"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name: "fk_parent_code", TableName: "children", Type: "FOREIGN KEY",
			Columns:        []string{"parent_code"},
			ForeignTable:   "parents",
			ForeignColumns: []string{"code"},
		}},
	}
	desired := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "code", Type: "VARCHAR(36)", Unique: true},
			{StructName: "Child", Name: "parent_code", Type: "VARCHAR(36)"},
		},
	}
	current := &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
		Name: "idx_parent_code", TableName: "children", Columns: []string{"parent_code"},
	}}}

	plan, err := planMySQLBidirectional(diff, desired, current)

	c.Assert(err, qt.IsNil)
	dropForeignKey, dropColumn := mysqlReverseForeignKeyAndColumnPositions(
		plan.Reverse.Nodes,
		"children",
		"fk_parent_code",
		"parents",
		"code",
	)
	c.Assert(dropForeignKey >= 0, qt.IsTrue)
	c.Assert(dropColumn >= 0, qt.IsTrue)
	c.Assert(dropForeignKey < dropColumn, qt.IsTrue)
}

func TestPlanBidirectionalSchemaDiff_MySQLConstraintRemovalCannotStrandForeignKey(t *testing.T) {
	tests := []struct {
		name           string
		constraintName string
		constraintType string
	}{
		{name: "unique", constraintName: "uq_parent", constraintType: "UNIQUE"},
		{name: "primary key", constraintName: "PRIMARY", constraintType: "PRIMARY KEY"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := singleMySQLForeignKeyDiff("children")
			diff.ConstraintsRemoved = []string{test.constraintName}
			diff.ConstraintsRemovedWithTables = []types.ConstraintRemovalInfo{{
				Name: test.constraintName, TableName: "children", Type: test.constraintType,
			}}
			current := &dbschematypes.DBSchema{Constraints: []dbschematypes.DBConstraint{{
				Name: test.constraintName, TableName: "children", Type: test.constraintType,
				ColumnNames: []string{"parent_id"},
			}}}

			plan, err := planMySQLBidirectional(diff, &goschema.Database{}, current)

			c.Assert(plan, qt.IsNil)
			c.Assert(err, qt.ErrorMatches,
				`cannot add foreign key "fk_parent" on table "children" for dialect "mysql": every covering index is removed later in the forward plan`)
		})
	}
}

func TestPlanBidirectionalSchemaDiff_MySQLUniqueReplacementStopsCoveringForeignKey(t *testing.T) {
	c := qt.New(t)
	diff := singleMySQLForeignKeyDiff("children")
	diff.ConstraintsAdded = append([]string{"uq_parent"}, diff.ConstraintsAdded...)
	diff.ConstraintsRemoved = []string{"uq_parent"}
	diff.ConstraintsAddedWithTables = append([]types.ConstraintAdditionInfo{{
		Name: "uq_parent", TableName: "children", Type: "UNIQUE", Columns: []string{"other_id"},
	}}, diff.ConstraintsAddedWithTables...)
	diff.ConstraintsRemovedWithTables = []types.ConstraintRemovalInfo{{
		Name: "uq_parent", TableName: "children", Type: "UNIQUE",
	}}
	current := &dbschematypes.DBSchema{Constraints: []dbschematypes.DBConstraint{{
		Name: "uq_parent", TableName: "children", Type: "UNIQUE", ColumnNames: []string{"parent_id"},
	}}}

	plan, err := planMySQLBidirectional(diff, &goschema.Database{}, current)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Contains, types.IndexRef{
		Name: "fk_parent", TableName: "children",
	})
}

func TestPlanBidirectionalSchemaDiff_MySQLNewTableInlineKeyAvoidsPhantomCleanup(t *testing.T) {
	tests := []struct {
		name    string
		primary bool
		unique  bool
	}{
		{name: "primary key", primary: true},
		{name: "unique", unique: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{TablesAdded: []string{"children"}}
			desired := &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "Parent", Name: "parents"},
					{StructName: "Child", Name: "children"},
				},
				Fields: []goschema.Field{
					{StructName: "Parent", Name: "id", Type: "BIGINT", Primary: true},
					{
						StructName: "Child", Name: "parent_id", Type: "BIGINT",
						Primary: test.primary, Unique: test.unique,
						Foreign: "parents(id)", ForeignKeyName: "fk_parent",
					},
				},
			}

			plan, err := planMySQLBidirectional(diff, desired, &dbschematypes.DBSchema{})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Not(qt.Contains), types.IndexRef{
				Name: "fk_parent", TableName: "children",
			})
		})
	}
}

func TestPlanBidirectionalSchemaDiff_MySQLAddedColumnInlineKeyAvoidsPhantomCleanup(t *testing.T) {
	tests := []struct {
		name    string
		primary bool
		unique  bool
	}{
		{name: "primary key", primary: true},
		{name: "unique", unique: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName: "children", ColumnsAdded: []string{"parent_id"},
			}}}
			desired := &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "Parent", Name: "parents"},
					{StructName: "Child", Name: "children"},
				},
				Fields: []goschema.Field{
					{StructName: "Parent", Name: "id", Type: "BIGINT", Primary: true},
					{
						StructName: "Child", Name: "parent_id", Type: "BIGINT",
						Primary: test.primary, Unique: test.unique,
						Foreign: "parents(id)", ForeignKeyName: "fk_parent",
					},
				},
			}

			plan, err := planMySQLBidirectional(diff, desired, &dbschematypes.DBSchema{})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Not(qt.Contains), types.IndexRef{
				Name: "fk_parent", TableName: "children",
			})
			dropForeignKey, dropColumn, _ := mysqlReverseMutationPositions(
				plan.Reverse.Nodes,
				"children",
				"fk_parent",
				"parent_id",
			)
			c.Assert(dropForeignKey >= 0, qt.IsTrue)
			c.Assert(dropColumn >= 0, qt.IsTrue)
			c.Assert(dropForeignKey < dropColumn, qt.IsTrue)
		})
	}
}

func TestPlanBidirectionalSchemaDiff_MySQLModifyColumnUniqueCoversForeignKey(t *testing.T) {
	c := qt.New(t)
	diff := singleMySQLForeignKeyDiff("children")
	diff.TablesModified = []types.TableDiff{{
		TableName: "children",
		ColumnsModified: []types.ColumnDiff{{
			ColumnName: "parent_id", Changes: map[string]string{"unique": "false -> true"},
		}},
	}}
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Child", Name: "children"}},
		Fields: []goschema.Field{{
			StructName: "Child", Name: "parent_id", Type: "BIGINT", Unique: true,
		}},
	}

	plan, err := planMySQLBidirectional(diff, desired, &dbschematypes.DBSchema{})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Not(qt.Contains), types.IndexRef{
		Name: "fk_parent", TableName: "children",
	})
}

func TestPlanBidirectionalSchemaDiff_MySQLReplacementCoverSurvivesCaseEquivalentRemoval(t *testing.T) {
	c := qt.New(t)
	diff := singleMySQLForeignKeyDiff("children")
	diff.SetIndexAdditions([]types.IndexRef{{Name: "IDX_PARENT", TableName: "children"}})
	diff.SetIndexRemovals([]types.IndexRef{{Name: "idx_parent", TableName: "children"}})
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Child", Name: "children"}},
		Indexes: []goschema.Index{{
			StructName: "Child", Name: "Idx_Parent", Fields: []string{"parent_id"},
		}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
			Name: "idx_parent", TableName: "children", Columns: []string{"other_id"},
		}}},
		Dialect:      platform.MySQL,
		Capabilities: capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Not(qt.Contains), types.IndexRef{
		Name: "fk_parent", TableName: "children",
	})
}

func TestPlanBidirectionalSchemaDiff_MySQLReplacementDropsOldCoverBeforeForeignKey(t *testing.T) {
	c := qt.New(t)
	diff := singleMySQLForeignKeyDiff("children")
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_parent", TableName: "children"}})
	diff.SetIndexRemovals([]types.IndexRef{{Name: "idx_parent", TableName: "children"}})
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Child", Name: "children"}},
		Indexes: []goschema.Index{{
			StructName: "Child", Name: "idx_parent", Fields: []string{"other_id"},
		}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
			Name: "idx_parent", TableName: "children", Columns: []string{"parent_id"},
		}}},
		Dialect:      platform.MySQL,
		Capabilities: capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.Contains, types.IndexRef{
		Name: "fk_parent", TableName: "children",
	})
}

func TestPlanBidirectionalSchemaDiff_MySQLDefaultSchemaMatchesUnqualifiedCatalogIndex(t *testing.T) {
	c := qt.New(t)
	diff := singleMySQLForeignKeyDiff("app.children")
	semantics := identifier.ForDialect(platform.MySQL)
	semantics.DefaultSchema = "app"
	diff.IdentifierSemantics = &semantics

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &goschema.Database{},
		CurrentSchema: &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
			Name: "idx_parent", TableName: "children", Columns: []string{"parent_id"},
		}}},
		Dialect:      platform.MySQL,
		Capabilities: capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.HasLen, 0)
}

func TestPlanBidirectionalSchemaDiff_MySQLIncompleteKeyPositionRefusesAmbiguousCleanup(t *testing.T) {
	c := qt.New(t)
	diff := singleMySQLForeignKeyDiff("children")

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &goschema.Database{},
		CurrentSchema: &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
			Name: "idx_parent_expr", TableName: "children", Columns: []string{"parent_id"},
			KeyPartsIncomplete: true,
		}}},
		Dialect:      platform.MySQL,
		Capabilities: capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(plan, qt.IsNil)
	c.Assert(err, qt.ErrorMatches,
		`cannot determine whether incomplete index "idx_parent_expr" on table "children" covers foreign key "fk_parent" for dialect "mysql"; refusing to plan backing-index cleanup`)
}

func TestPlanBidirectionalSchemaDiff_MySQLStructuredFunctionalKeyPositions(t *testing.T) {
	tests := []struct {
		name         string
		parts        []dbschematypes.DBIndexPart
		wantRemovals []types.IndexRef
	}{
		{
			name: "foreign key column before expression covers",
			parts: []dbschematypes.DBIndexPart{
				{Name: "parent_id"},
				{Expr: "(other_id + 1)"},
			},
		},
		{
			name: "expression before foreign key column does not cover",
			parts: []dbschematypes.DBIndexPart{
				{Expr: "(other_id + 1)"},
				{Name: "parent_id"},
			},
			wantRemovals: []types.IndexRef{{Name: "fk_parent", TableName: "children"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
				Diff:          singleMySQLForeignKeyDiff("children"),
				DesiredSchema: &goschema.Database{},
				CurrentSchema: &dbschematypes.DBSchema{Indexes: []dbschematypes.DBIndex{{
					Name: "idx_parent_expr", TableName: "children", Parts: test.parts,
					KeyPartsIncomplete: true,
				}}},
				Dialect:      platform.MySQL,
				Capabilities: capability.MySQL84(),
				Policy: generator.BidirectionalPlanPolicy{
					Create: generator.ConcurrentIndexDisabled,
					Drop:   generator.ConcurrentIndexDisabled,
				},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Reverse.Diff.IndexRemovals(), qt.DeepEquals, test.wantRemovals)
		})
	}
}

func TestPlanBidirectionalSchemaDiff_SwapsExactConcurrentIndexRefs(t *testing.T) {
	c := qt.New(t)
	refs := []types.IndexRef{
		{Name: "idx_shared", TableName: "app.orders"},
		{Name: "idx_shared", TableName: "audit.users"},
	}
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions(refs)
	desired := concurrentIndexSchema()
	current := &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{
		{Name: "orders", Schema: "app", Type: "BASE TABLE"},
		{Name: "users", Schema: "audit", Type: "BASE TABLE"},
	}}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: current,
		Dialect:       platform.Postgres,
		Capabilities:  capability.Postgres17(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexAll,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Forward.ConcurrentIndexRefs, qt.DeepEquals, refs)
	c.Assert(plan.Forward.ConcurrentIndexDropRefs, qt.HasLen, 0)
	c.Assert(plan.Reverse.ConcurrentIndexRefs, qt.HasLen, 0)
	c.Assert(plan.Reverse.ConcurrentIndexDropRefs, qt.DeepEquals, refs)
	c.Assert(plan.Forward.RequiresNoTransaction, qt.IsTrue)
	c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsTrue)
}

func TestPlanBidirectionalSchemaDiff_SwapsExactConcurrentIndexDropRefs(t *testing.T) {
	c := qt.New(t)
	refs := []types.IndexRef{
		{Name: "idx_shared", TableName: "app.orders"},
		{Name: "idx_shared", TableName: "audit.users"},
	}
	diff := &types.SchemaDiff{}
	diff.SetIndexRemovals(refs)
	current := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "orders", Schema: "app", Type: "BASE TABLE"},
			{Name: "users", Schema: "audit", Type: "BASE TABLE"},
		},
		Indexes: []dbschematypes.DBIndex{
			{Name: "idx_shared", TableName: "orders", Schema: "app", Columns: []string{"reference"}},
			{Name: "idx_shared", TableName: "users", Schema: "audit", Columns: []string{"reference"}},
		},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &goschema.Database{},
		CurrentSchema: current,
		Dialect:       platform.Postgres,
		Capabilities:  capability.Postgres17(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexAll,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Forward.ConcurrentIndexRefs, qt.HasLen, 0)
	c.Assert(plan.Forward.ConcurrentIndexDropRefs, qt.DeepEquals, refs)
	c.Assert(plan.Reverse.ConcurrentIndexRefs, qt.DeepEquals, refs)
	c.Assert(plan.Reverse.ConcurrentIndexDropRefs, qt.HasLen, 0)
	c.Assert(plan.Forward.RequiresNoTransaction, qt.IsTrue)
	c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsTrue)
}

func TestPlanBidirectionalSchemaDiff_ConcurrentCreateUsesBlockingRollbackWithoutDropCapability(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_users_reference", TableName: "users"}})
	caps := capability.Postgres17().With(capability.DropIndexConcurrently, false)

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: singleConcurrentIndexSchema(),
		CurrentSchema: &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{
			Name: "users", Type: "BASE TABLE",
		}}},
		Dialect:      platform.Postgres,
		Capabilities: caps,
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexAll,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	up, renderErr := renderer.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Forward.Nodes...)
	c.Assert(renderErr, qt.IsNil)
	down, renderErr := renderer.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)
	c.Assert(renderErr, qt.IsNil)
	c.Assert(up, qt.Equals, "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n")
	c.Assert(down, qt.Equals, "DROP INDEX IF EXISTS \"idx_users_reference\";\n")
	c.Assert(plan.Forward.ConcurrentIndexRefs, qt.DeepEquals, []types.IndexRef{{
		Name: "idx_users_reference", TableName: "users",
	}})
	c.Assert(plan.Reverse.ConcurrentIndexDropRefs, qt.HasLen, 0)
	c.Assert(plan.Forward.RequiresNoTransaction, qt.IsTrue)
	c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsFalse)
}

func TestPlanBidirectionalSchemaDiff_ExplicitConcurrentModeRequiresCapability(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		caps       capability.Capabilities
		createMode generator.ConcurrentIndexMode
		dropMode   generator.ConcurrentIndexMode
		wantErr    string
	}{
		{
			name: "sqlite create", dialect: platform.SQLite, caps: capability.SQLite3(),
			createMode: generator.ConcurrentIndexAll,
			wantErr:    `CREATE INDEX CONCURRENTLY requested by diff\.concurrent_index\.create cannot be generated for dialect "sqlite": target capability create_index_concurrently is unavailable`,
		},
		{
			name: "cockroach create", dialect: platform.CockroachDB, caps: capability.CockroachDB23(),
			createMode: generator.ConcurrentIndexAll,
			wantErr:    `CREATE INDEX CONCURRENTLY requested by diff\.concurrent_index\.create cannot be generated for dialect "cockroachdb": target capability create_index_concurrently is unavailable`,
		},
		{
			name: "spanner create", dialect: platform.Spanner, caps: capability.SpannerPostgres(),
			createMode: generator.ConcurrentIndexAll,
			wantErr:    `CREATE INDEX CONCURRENTLY requested by diff\.concurrent_index\.create cannot be generated for dialect "spanner": target capability create_index_concurrently is unavailable`,
		},
		{
			name: "yugabyte drop", dialect: platform.YugabyteDB, caps: capability.YugabyteDB25(),
			dropMode: generator.ConcurrentIndexAll,
			wantErr:  `DROP INDEX CONCURRENTLY requested by diff\.concurrent_index\.drop cannot be generated for dialect "yugabytedb": target capability drop_index_concurrently is unavailable`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{}
			diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_shared", TableName: "users"}})
			diff.SetIndexRemovals([]types.IndexRef{{Name: "idx_old", TableName: "users"}})
			plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
				Diff:          diff,
				DesiredSchema: &goschema.Database{},
				CurrentSchema: &dbschematypes.DBSchema{},
				Dialect:       test.dialect,
				Capabilities:  test.caps,
				Policy: generator.BidirectionalPlanPolicy{
					Create: test.createMode,
					Drop:   test.dropMode,
				},
			})

			c.Assert(plan, qt.IsNil)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestPlanBidirectionalSchemaDiff_YugabyteExplicitConcurrentCreateKeepsBlockingRollback(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_users_reference", TableName: "users"}})

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: singleConcurrentIndexSchema(),
		CurrentSchema: &dbschematypes.DBSchema{},
		Dialect:       platform.YugabyteDB,
		Capabilities:  capability.YugabyteDB25(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexAll,
		},
	})

	c.Assert(err, qt.IsNil)
	up, renderErr := renderer.RenderSQLWithCapabilities(
		platform.YugabyteDB,
		capability.YugabyteDB25(),
		plan.Forward.Nodes...,
	)
	c.Assert(renderErr, qt.IsNil)
	down, renderErr := renderer.RenderSQLWithCapabilities(
		platform.YugabyteDB,
		capability.YugabyteDB25(),
		plan.Reverse.Nodes...,
	)
	c.Assert(renderErr, qt.IsNil)
	c.Assert(up, qt.Equals, "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n")
	c.Assert(down, qt.Equals, "DROP INDEX IF EXISTS \"idx_users_reference\";\n")
	c.Assert(plan.Forward.RequiresNoTransaction, qt.IsTrue)
	c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsFalse)
}

func TestPlanBidirectionalSchemaDiff_AutomaticModeIsBidirectionallyCapabilitySafe(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{
			name: "cockroach stays blocking", dialect: platform.CockroachDB,
			caps: capability.CockroachDB23(),
		},
		{
			name: "spanner stays blocking", dialect: platform.Spanner,
			caps: capability.SpannerPostgres(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{}
			diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_users_reference", TableName: "users"}})
			plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
				Diff:          diff,
				DesiredSchema: singleConcurrentIndexSchema(),
				CurrentSchema: &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{
					Name: "users", Type: "BASE TABLE", EstimatedRows: 10,
				}}},
				Dialect:      test.dialect,
				Capabilities: test.caps,
				Policy: generator.BidirectionalPlanPolicy{
					Create: generator.ConcurrentIndexAutomatic,
					Drop:   generator.ConcurrentIndexDisabled,
				},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Forward.ConcurrentIndexRefs, qt.HasLen, 0)
			c.Assert(plan.Reverse.ConcurrentIndexDropRefs, qt.HasLen, 0)
			c.Assert(plan.Forward.RequiresNoTransaction, qt.IsFalse)
			c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsFalse)
		})
	}
}

func TestPlanBidirectionalSchemaDiff_AutomaticYugabyteCreateKeepsBlockingRollback(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_users_reference", TableName: "users"}})

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: singleConcurrentIndexSchema(),
		CurrentSchema: &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{
			Name: "users", Type: "BASE TABLE", EstimatedRows: 10,
		}}},
		Dialect:      platform.YugabyteDB,
		Capabilities: capability.YugabyteDB25(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexAutomatic,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Forward.ConcurrentIndexRefs, qt.DeepEquals, []types.IndexRef{{
		Name: "idx_users_reference", TableName: "users",
	}})
	c.Assert(plan.Reverse.ConcurrentIndexDropRefs, qt.HasLen, 0)
	c.Assert(plan.Forward.RequiresNoTransaction, qt.IsTrue)
	c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsFalse)
}

func TestPlanBidirectionalSchemaDiff_ConcurrentDropUsesBlockingReverseCreateWithoutCreateCapability(t *testing.T) {
	c := qt.New(t)
	ref := types.IndexRef{Name: "idx_users_reference", TableName: "users"}
	diff := &types.SchemaDiff{}
	diff.SetIndexRemovals([]types.IndexRef{ref})
	caps := capability.Postgres17().
		With(capability.CreateIndexConcurrently, false).
		With(capability.DropIndexConcurrently, true)
	current := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 10}},
		Indexes: []dbschematypes.DBIndex{{
			Name: "idx_users_reference", TableName: "users", Columns: []string{"reference"},
		}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &goschema.Database{},
		CurrentSchema: current,
		Dialect:       platform.Postgres,
		Capabilities:  caps,
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexAll,
		},
	})

	c.Assert(err, qt.IsNil)
	up, renderErr := renderer.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Forward.Nodes...)
	c.Assert(renderErr, qt.IsNil)
	down, renderErr := renderer.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)
	c.Assert(renderErr, qt.IsNil)
	c.Assert(up, qt.Equals, "DROP INDEX CONCURRENTLY IF EXISTS \"idx_users_reference\";\n")
	c.Assert(down, qt.Equals, "CREATE INDEX IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n")
	c.Assert(plan.Forward.ConcurrentIndexDropRefs, qt.DeepEquals, []types.IndexRef{ref})
	c.Assert(plan.Reverse.ConcurrentIndexRefs, qt.HasLen, 0)
	c.Assert(plan.Forward.RequiresNoTransaction, qt.IsTrue)
	c.Assert(plan.Reverse.RequiresNoTransaction, qt.IsFalse)
}

func TestPlanBidirectionalSchemaDiff_PartitionedParentRefusesExplicitPolicy(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}})
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Event", Name: "events"}},
		Indexes: []goschema.Index{{
			StructName: "Event", Name: "idx_events_tenant", Fields: []string{"tenant"},
		}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{
			Name: "events", Type: "BASE TABLE", Partitioned: true,
		}}},
		Dialect:      platform.Postgres,
		Capabilities: capability.Postgres17(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexAll,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(plan, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `CREATE INDEX CONCURRENTLY requested by diff\.concurrent_index\.create cannot be generated for partitioned table\(s\): .*`)
}

func concurrentIndexSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Order", Name: "orders", Schema: "app"},
			{StructName: "User", Name: "users", Schema: "audit"},
		},
		Indexes: []goschema.Index{
			{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}},
			{StructName: "User", Name: "idx_shared", Fields: []string{"reference"}},
		},
	}
}

func singleConcurrentIndexSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Indexes: []goschema.Index{{
			StructName: "User", Name: "idx_users_reference", Fields: []string{"reference"},
		}},
	}
}

func planMySQLBidirectional(
	diff *types.SchemaDiff,
	desired *goschema.Database,
	current *dbschematypes.DBSchema,
) (*generator.BidirectionalSchemaPlan, error) {
	return generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: current,
		Dialect:       platform.MySQL,
		Capabilities:  capability.MySQL84(),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
}

func mysqlReverseMutationPositions(
	nodes []ast.Node,
	table,
	foreignKey,
	column string,
) (dropForeignKey, dropColumn, dropIndex int) {
	dropForeignKey, dropColumn = mysqlReverseForeignKeyAndColumnPositions(
		nodes,
		table,
		foreignKey,
		table,
		column,
	)
	dropIndex = -1
	for position, node := range nodes {
		if typed, ok := node.(*ast.DropIndexNode); ok && typed.Table == table && typed.Name == foreignKey {
			dropIndex = position
		}
	}
	return dropForeignKey, dropColumn, dropIndex
}

func mysqlReverseForeignKeyAndColumnPositions(
	nodes []ast.Node,
	foreignKeyTable,
	foreignKey,
	columnTable,
	column string,
) (dropForeignKey, dropColumn int) {
	dropForeignKey, dropColumn = -1, -1
	for position, node := range nodes {
		typed, ok := node.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		for _, operation := range typed.Operations {
			switch alter := operation.(type) {
			case *ast.DropConstraintOperation:
				if typed.Name == foreignKeyTable && alter.ForeignKey && alter.ConstraintName == foreignKey {
					dropForeignKey = position
				}
			case *ast.DropColumnOperation:
				if typed.Name == columnTable && alter.ColumnName == column {
					dropColumn = position
				}
			}
		}
	}
	return dropForeignKey, dropColumn
}

func singleMySQLForeignKeyDiff(table string) *types.SchemaDiff {
	return &types.SchemaDiff{
		ConstraintsAdded: []string{"fk_parent"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name: "fk_parent", TableName: table, Type: "FOREIGN KEY",
			Columns: []string{"parent_id"}, ForeignTable: "parents", ForeignColumns: []string{"id"},
		}},
	}
}
