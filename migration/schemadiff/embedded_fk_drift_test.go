package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// reverseConstraintDiff mirrors the constraint half of the generator's down
// migration: what the up migration ADDED is what the down migration must
// REMOVE, preserving the table-qualified info so the planner targets the right
// table. (The generator's reverseSchemaDiffWithSchema is unexported; this
// reproduces only the constraint reversal exercised here.)
func reverseConstraintDiff(up *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	down := &difftypes.SchemaDiff{
		ConstraintsRemoved: append([]string(nil), up.ConstraintsAdded...),
	}
	for _, add := range up.ConstraintsAddedWithTables {
		down.ConstraintsRemovedWithTables = append(down.ConstraintsRemovedWithTables, difftypes.ConstraintRemovalInfo{
			Name:      add.Name,
			TableName: add.TableName,
			Type:      add.Type,
		})
	}
	return down
}

// ownableMixinSchema builds a generated schema with an "Ownable" embedded
// inline mixin that carries two `foreign=` fields (tenant_id and
// created_by_user_id, each with an explicit shared foreign_key_name). The mixin
// is embedded inline into the named host tables, so each host materializes the
// same columns + the same FK names.
func ownableMixinSchema(hostTables ...string) *goschema.Database {
	db := &goschema.Database{
		Fields: []goschema.Field{
			{
				StructName:     "Ownable",
				Name:           "tenant_id",
				Type:           "TEXT",
				Foreign:        "tenants(id)",
				ForeignKeyName: "fk_entity_tenant",
			},
			{
				StructName:     "Ownable",
				Name:           "created_by_user_id",
				Type:           "TEXT",
				Foreign:        "users(id)",
				ForeignKeyName: "fk_entity_created_by",
			},
		},
	}
	for _, ht := range hostTables {
		// Derive a struct name from the table name for the test (e.g. locations -> Locations).
		structName := strings.ToUpper(ht[:1]) + ht[1:]
		db.Tables = append(db.Tables, goschema.Table{StructName: structName, Name: ht})
		db.Fields = append(db.Fields, goschema.Field{StructName: structName, Name: "id", Type: "TEXT", Primary: true})
		db.EmbeddedFields = append(db.EmbeddedFields, goschema.EmbeddedField{
			StructName: structName, Mode: "inline", EmbeddedTypeName: "Ownable",
		})
	}
	return db
}

// ownableMixinColumnsOnlyDB builds the introspected DB for the given host
// tables with the mixin columns present but the FKs missing (so they surface as
// additions).
func ownableMixinColumnsOnlyDB(hostTables ...string) *types.DBSchema {
	db := &types.DBSchema{}
	for _, ht := range hostTables {
		db.Tables = append(db.Tables, ownableHostTable(ht))
	}
	return db
}

// ownableMixinConvergedDB builds the introspected DB for the given host tables
// with both the mixin columns and the tenant/created_by FKs already present
// (NO ACTION), i.e. the schema is converged.
func ownableMixinConvergedDB(hostTables ...string) *types.DBSchema {
	db := ownableMixinColumnsOnlyDB(hostTables...)
	for _, ht := range hostTables {
		db.Constraints = append(db.Constraints,
			types.DBConstraint{Name: "fk_entity_tenant", TableName: ht, Type: "FOREIGN KEY", ColumnName: "tenant_id", ForeignTable: strPtr("tenants"), ForeignColumn: strPtr("id"), DeleteRule: strPtr("NO ACTION"), UpdateRule: strPtr("NO ACTION")},
			types.DBConstraint{Name: "fk_entity_created_by", TableName: ht, Type: "FOREIGN KEY", ColumnName: "created_by_user_id", ForeignTable: strPtr("users"), ForeignColumn: strPtr("id"), DeleteRule: strPtr("NO ACTION"), UpdateRule: strPtr("NO ACTION")},
		)
	}
	return db
}

func ownableHostTable(name string) types.DBTable {
	return types.DBTable{
		Name: name,
		Columns: []types.DBColumn{
			{Name: "id", DataType: "text", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "tenant_id", DataType: "text", IsNullable: "NO"},
			{Name: "created_by_user_id", DataType: "text", IsNullable: "NO"},
		},
	}
}

// TestEmbeddedInlineMixinFK_NeverTargetsStructName is the end-to-end acceptance
// test for issue #197 through the postgres planner + renderer. When an embedded
// inline-relation mixin contributes `foreign=` fields to several host tables,
// the generated migration must add one FK per real host table (correct table
// name, no duplication) and never emit `ALTER TABLE <MixinStruct> ...`.
func TestEmbeddedInlineMixinFK_NeverTargetsStructName(t *testing.T) {
	hosts := []string{"locations", "areas", "commodities"}

	t.Run("missing mixin FKs are added once per real host table, never the struct", func(t *testing.T) {
		c := qt.New(t)

		gen := ownableMixinSchema(hosts...)
		diff := schemadiff.Compare(gen, ownableMixinColumnsOnlyDB(hosts...))
		c.Assert(diff.HasChanges(), qt.IsTrue)

		nodes := postgres.New().GenerateMigrationAST(diff, gen)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		// Never the mixin struct name.
		c.Assert(strings.Contains(sql, "ALTER TABLE Ownable"), qt.IsFalse,
			qt.Commentf("must not target the mixin struct name, got:\n%s", sql))

		// One correctly-targeted ADD per host table per FK, no duplication.
		for _, h := range hosts {
			tenantStmt := "ALTER TABLE " + h + " ADD CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)"
			createdByStmt := "ALTER TABLE " + h + " ADD CONSTRAINT fk_entity_created_by FOREIGN KEY (created_by_user_id) REFERENCES users(id)"
			c.Assert(strings.Count(sql, tenantStmt), qt.Equals, 1, qt.Commentf("host %s tenant FK, sql:\n%s", h, sql))
			c.Assert(strings.Count(sql, createdByStmt), qt.Equals, 1, qt.Commentf("host %s created_by FK, sql:\n%s", h, sql))
		}
	})

	t.Run("down migration drops each FK from its real host table", func(t *testing.T) {
		c := qt.New(t)

		gen := ownableMixinSchema(hosts...)
		diff := schemadiff.Compare(gen, ownableMixinColumnsOnlyDB(hosts...))

		// Reverse the diff the way the generator does for the down migration.
		downDiff := reverseConstraintDiff(diff)
		nodes := postgres.New().GenerateMigrationAST(downDiff, gen)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Contains(sql, "Ownable"), qt.IsFalse,
			qt.Commentf("down must not reference the mixin struct, got:\n%s", sql))
		for _, h := range hosts {
			c.Assert(strings.Contains(sql, "ALTER TABLE "+h+" DROP CONSTRAINT IF EXISTS fk_entity_tenant"), qt.IsTrue,
				qt.Commentf("down must drop tenant FK from %s, got:\n%s", h, sql))
			c.Assert(strings.Contains(sql, "ALTER TABLE "+h+" DROP CONSTRAINT IF EXISTS fk_entity_created_by"), qt.IsTrue,
				qt.Commentf("down must drop created_by FK from %s, got:\n%s", h, sql))
		}
	})

	t.Run("converged mixin FKs are a no-op (no churn)", func(t *testing.T) {
		c := qt.New(t)

		gen := ownableMixinSchema(hosts...)
		diff := schemadiff.Compare(gen, ownableMixinConvergedDB(hosts...))
		c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("added=%v removed=%v", diff.ConstraintsAdded, diff.ConstraintsRemoved))
	})
}
