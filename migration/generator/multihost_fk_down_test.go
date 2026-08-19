package generator

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// These tests pin the DOWN (rollback) half of issue #197: a mixin-shared FK name
// whose on_delete action drifts on >=2 host tables. The UP fix (PR #198 commit
// 894902d) emits a table-qualified per-host DROP+ADD, but the generated DOWN was
// still name-only — it re-added only one host and dropped only one host, so the
// 2nd host's re-add collided (Postgres 42710 / MySQL 1826) and the rollback
// aborted half-applied. The fix repopulates the reversed
// ConstraintsAddedWithTables from the introspected (pre-change) DB so the DOWN
// add-path fans out per host and restores each host's PRIOR action.
//
// These run the REAL generator down-path (generateDownMigrationSQL ->
// reverseSchemaDiffWithSchema -> reverseConstraintAdditions), not a hand-rolled
// reversal, which is the gap that let the original bug ship.

// multiHostMixinGenerated builds a generated schema with an "Ownable" inline
// mixin carrying a shared tenant FK (fk_entity_tenant, ON DELETE = onDelete)
// embedded into each host table.
func multiHostMixinGenerated(onDelete string, hosts ...string) *goschema.Database {
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Tenant", Name: "tenants"}},
		Fields: []goschema.Field{
			{StructName: "Tenant", Name: "id", Type: "VARCHAR(255)", Primary: true},
			{
				StructName:     "Ownable",
				Name:           "tenant_id",
				Type:           "VARCHAR(255)",
				Foreign:        "tenants(id)",
				ForeignKeyName: "fk_entity_tenant",
				OnDelete:       onDelete,
			},
		},
	}
	for _, h := range hosts {
		structName := strings.ToUpper(h[:1]) + h[1:]
		db.Tables = append(db.Tables, goschema.Table{StructName: structName, Name: h})
		db.Fields = append(db.Fields,
			goschema.Field{StructName: structName, Name: "id", Type: "TEXT", Primary: true},
		)
		db.EmbeddedFields = append(db.EmbeddedFields, goschema.EmbeddedField{
			StructName: structName, Mode: "inline", EmbeddedTypeName: "Ownable",
		})
	}
	return db
}

// multiHostMixinDB builds the introspected (pre-change) DB: each host has the
// tenant FK with the given delete rule.
func multiHostMixinDB(deleteRule string, hosts ...string) *dbschematypes.DBSchema {
	varcharLength := 255
	db := &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{
		Name: "tenants",
		Columns: []dbschematypes.DBColumn{
			{Name: "id", DataType: "varchar", CharacterMaxLength: &varcharLength, IsNullable: "NO", IsPrimaryKey: true},
		},
	}}}
	for _, h := range hosts {
		db.Tables = append(db.Tables, dbschematypes.DBTable{
			Name: h,
			Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "varchar", CharacterMaxLength: &varcharLength, IsNullable: "NO", IsPrimaryKey: true},
				{Name: "tenant_id", DataType: "varchar", CharacterMaxLength: &varcharLength, IsNullable: "NO"},
			},
		})
		db.Constraints = append(db.Constraints, dbschematypes.DBConstraint{
			Name: "fk_entity_tenant", TableName: h, Type: "FOREIGN KEY", ColumnName: "tenant_id",
			ForeignTable: new("tenants"), ForeignColumn: new("id"),
			DeleteRule: new(deleteRule), UpdateRule: new("NO ACTION"),
		})
	}
	return db
}

// TestGenerateDownMigration_MultiHostMixinFKModify_RestoresPriorActionPerHost is
// the regression test for the issue #197 DOWN path. The UP changes the shared
// tenant FK from the prior NO ACTION to ON DELETE CASCADE on every host; the
// generated DOWN must, for EACH host, drop the (CASCADE) constraint and re-add
// it with the prior NO ACTION — table-qualified so no host collides.
func TestGenerateDownMigration_MultiHostMixinFKModify_RestoresPriorActionPerHost(t *testing.T) {
	c := qt.New(t)
	hosts := []string{"locations", "areas", "commodities"}
	gen := multiHostMixinGenerated("CASCADE", hosts...)
	dbSchema := multiHostMixinDB("NO ACTION", hosts...)

	upDiff := schemadiff.CompareWithDialect(gen, dbSchema, "postgres")
	c.Assert(upDiff.HasChanges(), qt.IsTrue)
	c.Assert(countConstraint(upDiff.ConstraintsAdded, "fk_entity_tenant"), qt.Equals, len(hosts))
	c.Assert(countConstraint(upDiff.ConstraintsRemoved, "fk_entity_tenant"), qt.Equals, len(hosts))

	downSQL, err := generateDownMigrationSQL(upDiff, gen, dbSchema, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	for _, host := range hosts {
		dropStatement := "ALTER TABLE " + host + " DROP CONSTRAINT IF EXISTS fk_entity_tenant;"
		addStatement := "ALTER TABLE " + host + " ADD CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)"

		c.Assert(strings.Count(downSQL, dropStatement), qt.Equals, 1,
			qt.Commentf("host %s: expected exactly one table-qualified DROP in DOWN, got:\n%s", host, downSQL))
		c.Assert(strings.Count(downSQL, addStatement), qt.Equals, 1,
			qt.Commentf("host %s: expected exactly one re-ADD in DOWN, got:\n%s", host, downSQL))

		dropIndex := strings.Index(downSQL, dropStatement)
		addIndex := strings.Index(downSQL, addStatement)
		c.Assert(dropIndex >= 0 && addIndex >= 0 && dropIndex < addIndex, qt.IsTrue,
			qt.Commentf("host %s: DOWN DROP must precede the re-ADD; drop@%d add@%d\n%s", host, dropIndex, addIndex, downSQL))
	}

	c.Assert(downSQL, qt.Not(qt.Contains), "ON DELETE CASCADE",
		qt.Commentf("DOWN must restore the prior action, not re-apply CASCADE:\n%s", downSQL))
	c.Assert(downSQL, qt.Not(qt.Contains), "Ownable",
		qt.Commentf("DOWN must not reference the mixin struct name:\n%s", downSQL))
}

func TestGenerateDownMigration_MultiHostMixinFKModify_MySQLRejectsDuplicateNames(t *testing.T) {
	c := qt.New(t)
	hosts := []string{"locations", "areas", "commodities"}
	generated := multiHostMixinGenerated("CASCADE", hosts...)
	dbSchema := multiHostMixinDB("NO ACTION", hosts...)
	diff := schemadiff.CompareWithDialect(generated, dbSchema, "mysql")

	downSQL, err := generateDownMigrationSQL(diff, generated, dbSchema, "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `error generating down migration SQL: invalid foreign key: foreign-key name "fk_entity_tenant" is duplicated in database constraint namespace`)
	c.Assert(downSQL, qt.Equals, "")
}

// TestGenerateDownMigration_MultiHostMixinFKModify_NameOnlyCounterfactual proves
// the bug is actually fixed by the table-qualified reversed additions: without
// ConstraintsAddedWithTables on the reversed diff the DOWN add-path would emit a
// single name-only re-ADD (one host) instead of one per host. We assert the
// fixed DOWN re-adds the FK for ALL hosts — the property the old code violated.
func TestGenerateDownMigration_MultiHostMixinFKModify_NameOnlyCounterfactual(t *testing.T) {
	c := qt.New(t)
	hosts := []string{"locations", "areas", "commodities"}

	gen := multiHostMixinGenerated("CASCADE", hosts...)
	dbSchema := multiHostMixinDB("NO ACTION", hosts...)
	upDiff := schemadiff.CompareWithDialect(gen, dbSchema, "postgres")

	downSQL, err := generateDownMigrationSQL(upDiff, gen, dbSchema, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	// One re-ADD per host (the old name-only path produced exactly one total).
	total := strings.Count(downSQL, "ADD CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)")
	c.Assert(total, qt.Equals, len(hosts),
		qt.Commentf("DOWN must re-add the FK once per host (%d), got %d:\n%s", len(hosts), total, downSQL))
}

// TestReverseConstraintAdditions_RestoresPerHostBody unit-tests the new helper
// directly: each up-removal becomes a down-addition carrying the FULL FK body
// (columns, target, prior action) read from the introspected DB, keyed per host.
func TestReverseConstraintAdditions_RestoresPerHostBody(t *testing.T) {
	c := qt.New(t)
	hosts := []string{"locations", "areas"}

	dbSchema := multiHostMixinDB("NO ACTION", hosts...)
	// Up diff: the shared FK was removed (then re-added) on each host.
	upDiff := &types.SchemaDiff{}
	for _, h := range hosts {
		upDiff.ConstraintsRemovedWithTables = append(upDiff.ConstraintsRemovedWithTables,
			types.ConstraintRemovalInfo{Name: "fk_entity_tenant", TableName: h, Type: "FOREIGN KEY"})
	}

	additions := reverseConstraintAdditions(upDiff, dbSchema)
	c.Assert(additions, qt.HasLen, len(hosts))

	byTable := make(map[string]types.ConstraintAdditionInfo)
	for _, a := range additions {
		byTable[a.TableName] = a
	}
	for _, h := range hosts {
		a, ok := byTable[h]
		c.Assert(ok, qt.IsTrue, qt.Commentf("missing reversed addition for host %s", h))
		c.Assert(a.Name, qt.Equals, "fk_entity_tenant")
		c.Assert(a.Type, qt.Equals, "FOREIGN KEY")
		c.Assert(a.Columns, qt.DeepEquals, []string{"tenant_id"})
		c.Assert(a.ForeignTable, qt.Equals, "tenants")
		c.Assert(a.ForeignColumn, qt.Equals, "id")
		c.Assert(a.ForeignColumns, qt.DeepEquals, []string{"id"})
		c.Assert(a.OnDelete, qt.Equals, "NO ACTION")
	}
}

func TestReverseConstraintAdditions_RestoresPrimaryKeyColumns(t *testing.T) {
	c := qt.New(t)

	dbSchema := &dbschematypes.DBSchema{
		Constraints: []dbschematypes.DBConstraint{{
			Name:        "PRIMARY",
			TableName:   "memberships",
			Type:        "PRIMARY KEY",
			ColumnNames: []string{"org_id", "user_id"},
		}},
	}
	upDiff := &types.SchemaDiff{
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
			Name:      "PRIMARY",
			TableName: "memberships",
			Type:      "PRIMARY KEY",
		}},
	}

	additions := reverseConstraintAdditions(upDiff, dbSchema)
	c.Assert(additions, qt.DeepEquals, []types.ConstraintAdditionInfo{{
		Name:      "PRIMARY",
		TableName: "memberships",
		Type:      "PRIMARY KEY",
		Columns:   []string{"org_id", "user_id"},
	}})
}

func TestReverseConstraintAdditions_RestoresCompositeForeignKeyBody(t *testing.T) {
	c := qt.New(t)
	dbSchema := &dbschematypes.DBSchema{
		Constraints: []dbschematypes.DBConstraint{
			{
				Name:           "fk_orders_accounts",
				TableName:      "orders",
				Type:           "FOREIGN KEY",
				ColumnName:     "tenant_id",
				ColumnNames:    []string{"tenant_id", "owner_id"},
				ForeignTable:   new("accounts"),
				ForeignColumn:  new("tenant_id"),
				ForeignColumns: []string{"tenant_id", "id"},
				DeleteRule:     new("CASCADE"),
				UpdateRule:     new("NO ACTION"),
			},
		},
	}
	upDiff := &types.SchemaDiff{
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "fk_orders_accounts", TableName: "orders", Type: "FOREIGN KEY"},
		},
	}

	additions := reverseConstraintAdditions(upDiff, dbSchema)

	c.Assert(additions, qt.DeepEquals, []types.ConstraintAdditionInfo{
		{
			Name:           "fk_orders_accounts",
			TableName:      "orders",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "owner_id"},
			ForeignTable:   "accounts",
			ForeignColumn:  "tenant_id",
			ForeignColumns: []string{"tenant_id", "id"},
			OnDelete:       "CASCADE",
			OnUpdate:       "NO ACTION",
		},
	})
}

func TestReverseConstraintAdditions_RestoresCheckConstraintBody(t *testing.T) {
	c := qt.New(t)
	checkClause := "quantity > 0"
	dbSchema := &dbschematypes.DBSchema{
		Constraints: []dbschematypes.DBConstraint{{
			Name:        "products_quantity_check",
			TableName:   "products",
			Type:        "CHECK",
			CheckClause: &checkClause,
		}},
	}
	upDiff := &types.SchemaDiff{
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "products_quantity_check", TableName: "products", Type: "CHECK"},
		},
	}

	additions := reverseConstraintAdditions(upDiff, dbSchema)

	c.Assert(additions, qt.DeepEquals, []types.ConstraintAdditionInfo{{
		Name:            "products_quantity_check",
		TableName:       "products",
		Type:            "CHECK",
		CheckExpression: "quantity > 0",
	}})
}

func TestReverseConstraintAdditions_RestoresUniqueConstraintBody(t *testing.T) {
	c := qt.New(t)
	nullsDistinct := false
	dbSchema := &dbschematypes.DBSchema{
		Constraints: []dbschematypes.DBConstraint{{
			Name:           "accounts_identity_unique",
			TableName:      "accounts",
			Type:           "UNIQUE",
			ColumnNames:    []string{"email"},
			IncludeColumns: []string{"updated_at"},
			NullsDistinct:  &nullsDistinct,
		}},
	}
	upDiff := &types.SchemaDiff{
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "accounts_identity_unique", TableName: "accounts", Type: "UNIQUE"},
		},
	}

	additions := reverseConstraintAdditions(upDiff, dbSchema)

	c.Assert(additions, qt.HasLen, 1)
	c.Assert(additions[0].Name, qt.Equals, "accounts_identity_unique")
	c.Assert(additions[0].TableName, qt.Equals, "accounts")
	c.Assert(additions[0].Type, qt.Equals, "UNIQUE")
	c.Assert(additions[0].Columns, qt.DeepEquals, []string{"email"})
	c.Assert(additions[0].IncludeColumns, qt.DeepEquals, []string{"updated_at"})
	c.Assert(additions[0].NullsDistinct, qt.IsNotNil)
	c.Assert(*additions[0].NullsDistinct, qt.Equals, false)
}

// TestReverseConstraintAdditions_NilDBSchema is the legacy-caller path: with no
// introspected schema there is no body to restore, so the helper returns nil and
// the names ride the name-only fallback via ConstraintsAdded.
func TestReverseConstraintAdditions_NilDBSchema(t *testing.T) {
	c := qt.New(t)
	upDiff := &types.SchemaDiff{
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "fk_x", TableName: "t", Type: "FOREIGN KEY"},
		},
	}
	c.Assert(reverseConstraintAdditions(upDiff, nil), qt.IsNil)
}

func countConstraint(names []string, want string) int {
	n := 0
	for _, name := range names {
		if name == want {
			n++
		}
	}
	return n
}
