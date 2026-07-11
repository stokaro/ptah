package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestConstraints_EmbeddedInlineMixinForeignKey covers issue #197 — a
// `foreign=` annotation declared on an embedded inline mixin (a base struct
// that is embedded via //migrator:embedded mode="inline" into several concrete
// tables) must synthesize one FK per real embedding table, against the host
// table name, never against the mixin's Go struct name.
//
// The #189 synthesis iterated the raw parse result, where the mixin's FK fields
// carry the mixin StructName (not a table). That produced
// `ALTER TABLE <MixinStruct> ADD CONSTRAINT ...` once per embedding host, all
// collapsed onto the same bogus name with no DROP and no ON DELETE. The fix
// resolves fields through the same CREATE-path embedded expansion the column
// diff uses, so each host gets its own correctly-targeted FK.
//
// The mixin here intentionally pins explicit foreign_key_name= values that are
// shared across every host (mirroring inventario's TenantGroupAwareEntityID,
// whose fk_entity_* names repeat on every embedding table). FK names are scoped
// per table in the database, so the same name legitimately appears once per
// host table — the assertions therefore count occurrences per (table, name).
func TestConstraints_EmbeddedInlineMixinForeignKey(t *testing.T) {
	// ownerCol returns the standard introspected FK row for <table>.<col> ->
	// <refTable>.id with the given delete rule (nil == rule absent).
	dbFK := func(name, table, col, refTable string, deleteRule *string) types.DBConstraint {
		return types.DBConstraint{
			Name:          name,
			TableName:     table,
			Type:          "FOREIGN KEY",
			ColumnName:    col,
			ForeignTable:  new(refTable),
			ForeignColumn: new("id"),
			DeleteRule:    deleteRule,
			UpdateRule:    new("NO ACTION"),
		}
	}

	// mixinFields are the FK fields owned by the "Ownable" mixin struct. They
	// are declared on the mixin, not on any table.
	mixinFields := []goschema.Field{
		{
			StructName:     "Ownable",
			Name:           "tenant_id",
			Type:           "TEXT",
			Foreign:        "tenants(id)",
			ForeignKeyName: "fk_entity_tenant",
			OnDelete:       "CASCADE",
		},
		{
			StructName:     "Ownable",
			Name:           "created_by_user_id",
			Type:           "TEXT",
			Foreign:        "users(id)",
			ForeignKeyName: "fk_entity_created_by",
			// OnDelete intentionally empty -> NO ACTION.
		},
	}

	// Two concrete tables embed the mixin inline.
	embedded := []goschema.EmbeddedField{
		{StructName: "Location", Mode: "inline", EmbeddedTypeName: "Ownable"},
		{StructName: "Area", Mode: "inline", EmbeddedTypeName: "Ownable"},
	}

	// dbColumns: both host tables already exist in the database with the mixin
	// columns materialized, so the field-level FKs get synthesized.
	dbTable := func(name string) types.DBTable {
		return types.DBTable{
			Name: name,
			Columns: []types.DBColumn{
				{Name: "id"},
				{Name: "tenant_id"},
				{Name: "created_by_user_id"},
			},
		}
	}

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Location", Name: "locations"},
			{StructName: "Area", Name: "areas"},
		},
		Fields: append([]goschema.Field{
			{StructName: "Location", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Area", Name: "id", Type: "TEXT", Primary: true},
		}, mixinFields...),
		EmbeddedFields: embedded,
	}

	t.Run("unchanged actions round-trip to a no-op across all embedding hosts", func(t *testing.T) {
		c := qt.New(t)

		database := &types.DBSchema{
			Tables: []types.DBTable{dbTable("locations"), dbTable("areas")},
			Constraints: []types.DBConstraint{
				// CASCADE matches the mixin annotation on both hosts.
				dbFK("fk_entity_tenant", "locations", "tenant_id", "tenants", new("CASCADE")),
				dbFK("fk_entity_tenant", "areas", "tenant_id", "tenants", new("CASCADE")),
				// NO ACTION matches the action-less created_by FK on both hosts.
				dbFK("fk_entity_created_by", "locations", "created_by_user_id", "users", new("NO ACTION")),
				dbFK("fk_entity_created_by", "areas", "created_by_user_id", "users", new("NO ACTION")),
			},
		}

		diff := &difftypes.SchemaDiff{}
		compare.Constraints(generated, database, diff, nil)

		c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("added=%v", diff.ConstraintsAdded))
		c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("removed=%v", diff.ConstraintsRemoved))
	})

	t.Run("never targets the mixin struct name", func(t *testing.T) {
		c := qt.New(t)

		// Empty database -> every synthesized FK is "added". This is the path
		// that previously emitted ALTER TABLE Ownable ADD CONSTRAINT ...; with
		// the fix the synthesis is keyed on real host tables, so the removal
		// info (and therefore the eventual ALTER) never references the struct.
		database := &types.DBSchema{
			Tables: []types.DBTable{dbTable("locations"), dbTable("areas")},
			// No matching DB constraints -> all synthesized FKs are additions,
			// but crucially against real tables.
			Constraints: []types.DBConstraint{
				dbFK("fk_entity_tenant", "locations", "tenant_id", "tenants", new("CASCADE")),
				dbFK("fk_entity_tenant", "areas", "tenant_id", "tenants", new("CASCADE")),
				dbFK("fk_entity_created_by", "locations", "created_by_user_id", "users", new("NO ACTION")),
				dbFK("fk_entity_created_by", "areas", "created_by_user_id", "users", new("NO ACTION")),
			},
		}

		// Re-run the unchanged-state diff (a no-op) and additionally assert the
		// removal-info table names never contain the mixin struct. The removal
		// slice is the structure the planner reads to build ALTER statements.
		diff := &difftypes.SchemaDiff{}
		compare.Constraints(generated, database, diff, nil)
		for _, info := range diff.ConstraintsRemovedWithTables {
			c.Assert(info.TableName, qt.Not(qt.Equals), "Ownable",
				qt.Commentf("removal must target a real table, got %q", info.TableName))
		}
	})

	t.Run("action change on one host surfaces once for that host, not duplicated", func(t *testing.T) {
		c := qt.New(t)

		// The live FK on locations.tenant_id drifted to NO ACTION; the model
		// (mixin) wants CASCADE. areas still matches CASCADE. Expect exactly one
		// drop + one add of fk_entity_tenant, and the removal must be scoped to
		// the locations table.
		database := &types.DBSchema{
			Tables: []types.DBTable{dbTable("locations"), dbTable("areas")},
			Constraints: []types.DBConstraint{
				dbFK("fk_entity_tenant", "locations", "tenant_id", "tenants", new("NO ACTION")), // drifted
				dbFK("fk_entity_tenant", "areas", "tenant_id", "tenants", new("CASCADE")),       // unchanged
				dbFK("fk_entity_created_by", "locations", "created_by_user_id", "users", new("NO ACTION")),
				dbFK("fk_entity_created_by", "areas", "created_by_user_id", "users", new("NO ACTION")),
			},
		}

		diff := &difftypes.SchemaDiff{}
		compare.Constraints(generated, database, diff, nil)

		c.Assert(diff.ConstraintsAdded, qt.DeepEquals, []string{"fk_entity_tenant"},
			qt.Commentf("added=%v", diff.ConstraintsAdded))
		c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, []string{"fk_entity_tenant"},
			qt.Commentf("removed=%v", diff.ConstraintsRemoved))

		// The drop must target the locations table (where the drift is), never
		// areas (unchanged) and never the mixin struct.
		c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 1)
		c.Assert(diff.ConstraintsRemovedWithTables[0].TableName, qt.Equals, "locations")
		c.Assert(diff.ConstraintsRemovedWithTables[0].Name, qt.Equals, "fk_entity_tenant")
	})

	t.Run("no diff and no host-collapse when the whole mixin is unchanged on three hosts", func(t *testing.T) {
		c := qt.New(t)

		// Add a third embedding host to prove the per-host de-duplication does
		// not accidentally drop legitimate distinct (table,name) pairs.
		gen3 := &goschema.Database{
			Tables: []goschema.Table{
				{StructName: "Location", Name: "locations"},
				{StructName: "Area", Name: "areas"},
				{StructName: "Commodity", Name: "commodities"},
			},
			Fields: append([]goschema.Field{
				{StructName: "Location", Name: "id", Type: "TEXT", Primary: true},
				{StructName: "Area", Name: "id", Type: "TEXT", Primary: true},
				{StructName: "Commodity", Name: "id", Type: "TEXT", Primary: true},
			}, mixinFields...),
			EmbeddedFields: []goschema.EmbeddedField{
				{StructName: "Location", Mode: "inline", EmbeddedTypeName: "Ownable"},
				{StructName: "Area", Mode: "inline", EmbeddedTypeName: "Ownable"},
				{StructName: "Commodity", Mode: "inline", EmbeddedTypeName: "Ownable"},
			},
		}

		hosts := []string{"locations", "areas", "commodities"}
		var dbTables []types.DBTable
		var dbConstraints []types.DBConstraint
		for _, h := range hosts {
			dbTables = append(dbTables, dbTable(h))
			dbConstraints = append(dbConstraints,
				dbFK("fk_entity_tenant", h, "tenant_id", "tenants", new("CASCADE")),
				dbFK("fk_entity_created_by", h, "created_by_user_id", "users", new("NO ACTION")),
			)
		}

		diff := &difftypes.SchemaDiff{}
		compare.Constraints(gen3, &types.DBSchema{Tables: dbTables, Constraints: dbConstraints}, diff, nil)

		c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("added=%v", diff.ConstraintsAdded))
		c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("removed=%v", diff.ConstraintsRemoved))
	})

	t.Run("mixin column missing from DB on one host is not synthesized for that host", func(t *testing.T) {
		c := qt.New(t)

		// areas has not yet materialized the mixin columns (fresh table mid-add):
		// its FKs ship inline with the column add, so they must NOT be
		// synthesized here. locations is fully migrated and matches -> no-op.
		database := &types.DBSchema{
			Tables: []types.DBTable{
				dbTable("locations"),
				{Name: "areas", Columns: []types.DBColumn{{Name: "id"}}}, // mixin cols absent
			},
			Constraints: []types.DBConstraint{
				dbFK("fk_entity_tenant", "locations", "tenant_id", "tenants", new("CASCADE")),
				dbFK("fk_entity_created_by", "locations", "created_by_user_id", "users", new("NO ACTION")),
			},
		}

		diff := &difftypes.SchemaDiff{}
		compare.Constraints(generated, database, diff, nil)

		c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("added=%v", diff.ConstraintsAdded))
		c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("removed=%v", diff.ConstraintsRemoved))
	})
}

// TestConstraints_FieldLevelForeignKeyOnDeleteNotStripped covers the
// SYMPTOM 2 facet of issue #197 in isolation: a single-column field-level FK
// whose ON DELETE is declared via the field annotation, with a matching live
// DB action, must round-trip to a no-op — the synthesized constraint must carry
// the field's OnDelete so foreignKeyConstraintChanged sees CASCADE == CASCADE
// rather than reading the desired side as action-less (which would strip the
// existing ON DELETE on every generate).
func TestConstraints_FieldLevelForeignKeyOnDeleteNotStripped(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "AuditRow", Name: "audit_rows"}},
		Fields: []goschema.Field{
			{StructName: "AuditRow", Name: "id", Type: "TEXT", Primary: true},
			{
				StructName:     "AuditRow",
				Name:           "migration_id",
				Type:           "TEXT",
				Foreign:        "migrations(id)",
				ForeignKeyName: "fk_audit_migration",
				OnDelete:       "CASCADE",
			},
		},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{
			{Name: "audit_rows", Columns: []types.DBColumn{{Name: "id"}, {Name: "migration_id"}}},
		},
		Constraints: []types.DBConstraint{
			{
				Name:          "fk_audit_migration",
				TableName:     "audit_rows",
				Type:          "FOREIGN KEY",
				ColumnName:    "migration_id",
				ForeignTable:  new("migrations"),
				ForeignColumn: new("id"),
				DeleteRule:    new("CASCADE"),
				UpdateRule:    new("NO ACTION"),
			},
		},
	}

	diff := &difftypes.SchemaDiff{}
	compare.Constraints(generated, database, diff, nil)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("CASCADE must not be stripped, added=%v", diff.ConstraintsAdded))
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("CASCADE must not be stripped, removed=%v", diff.ConstraintsRemoved))
}
