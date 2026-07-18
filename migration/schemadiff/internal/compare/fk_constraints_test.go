package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestConstraints_FieldLevelForeignKey covers issue #189 — on_delete /
// on_update drift on a field-level `foreign=` annotation against an
// already-applied schema must surface as a migration. The compare layer
// synthesizes a goschema.Constraint of type FOREIGN KEY from the field (for
// columns that already exist in the introspected database) and lets the
// matching DB-side FK through, so an action change runs through the standard
// Constraints() → foreignKeyConstraintChanged path as a drop+add. An unchanged
// FK — including the "" == NO ACTION default — stays a no-op.
//
// This is the diff/drift counterpart to the CREATE/ALTER-time emission fixed by
// #117 / PR #122; see TestConstraints_FieldLevelCheck for the analogous CHECK
// coverage that this mirrors.
func TestConstraints_FieldLevelForeignKey(t *testing.T) {
	// Shared setup: an "exports" table whose "file_id" column already exists in
	// the database, so the field-level FK gets synthesized.
	exportsTable := types.DBTable{
		Name:    "exports",
		Columns: []types.DBColumn{{Name: "id"}, {Name: "file_id"}},
	}

	// dbFK builds the introspected FK row for exports.file_id -> files.id with
	// the given delete/update rules (nil pointer == rule absent).
	dbFK := func(deleteRule, updateRule *string) types.DBConstraint {
		return types.DBConstraint{
			Name:          "fk_export_file",
			TableName:     "exports",
			Type:          "FOREIGN KEY",
			ColumnName:    "file_id",
			ForeignTable:  new("files"),
			ForeignColumn: new("id"),
			DeleteRule:    deleteRule,
			UpdateRule:    updateRule,
		}
	}

	tests := []struct {
		name      string
		generated *goschema.Database
		database  *types.DBSchema
		expected  *difftypes.SchemaDiff
	}{
		{
			// The headline bug: model says SET NULL, the live FK is NO ACTION.
			// Expect a drop + add so a non-empty up/down migration is produced.
			name: "on_delete NO ACTION -> SET NULL surfaces as drop + add",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Export",
						Name:           "file_id",
						Type:           "TEXT",
						Foreign:        "files(id)",
						ForeignKeyName: "fk_export_file",
						OnDelete:       "SET NULL",
					},
				},
			},
			database: &types.DBSchema{
				Tables:      []types.DBTable{exportsTable},
				Constraints: []types.DBConstraint{dbFK(new("NO ACTION"), new("NO ACTION"))},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"fk_export_file"},
				ConstraintsRemoved: []string{"fk_export_file"},
			},
		},
		{
			// Idempotency #1: SET NULL == SET NULL is a no-op.
			name: "on_delete SET NULL matches existing — no diff (idempotent)",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Export",
						Name:           "file_id",
						Type:           "TEXT",
						Foreign:        "files(id)",
						ForeignKeyName: "fk_export_file",
						OnDelete:       "SET NULL",
					},
				},
			},
			database: &types.DBSchema{
				Tables:      []types.DBTable{exportsTable},
				Constraints: []types.DBConstraint{dbFK(new("SET NULL"), new("NO ACTION"))},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			// Idempotency #2: the model leaves the action empty while Postgres
			// reports the default as NO ACTION. The normalization ("" == NO
			// ACTION, trim + upper-case) must make this a no-op, otherwise every
			// `generate` would churn the same FK forever.
			name: "empty action == NO ACTION — no diff (idempotent)",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Export",
						Name:           "file_id",
						Type:           "TEXT",
						Foreign:        "files(id)",
						ForeignKeyName: "fk_export_file",
						// OnDelete / OnUpdate intentionally empty.
					},
				},
			},
			database: &types.DBSchema{
				Tables:      []types.DBTable{exportsTable},
				Constraints: []types.DBConstraint{dbFK(new("NO ACTION"), new("NO ACTION"))},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			// Casing / whitespace differences between the model and the
			// introspected rule must not register as drift.
			name: "case-insensitive action match — no diff (idempotent)",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Export",
						Name:           "file_id",
						Type:           "TEXT",
						Foreign:        "files(id)",
						ForeignKeyName: "fk_export_file",
						OnDelete:       "cascade",
					},
				},
			},
			database: &types.DBSchema{
				Tables:      []types.DBTable{exportsTable},
				Constraints: []types.DBConstraint{dbFK(new("CASCADE"), new("NO ACTION"))},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			// on_update drift is detected independently of on_delete.
			name: "on_update CASCADE drift surfaces as drop + add",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Export",
						Name:           "file_id",
						Type:           "TEXT",
						Foreign:        "files(id)",
						ForeignKeyName: "fk_export_file",
						OnUpdate:       "CASCADE",
					},
				},
			},
			database: &types.DBSchema{
				Tables:      []types.DBTable{exportsTable},
				Constraints: []types.DBConstraint{dbFK(new("NO ACTION"), new("NO ACTION"))},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"fk_export_file"},
				ConstraintsRemoved: []string{"fk_export_file"},
			},
		},
		{
			// Fallback name: no foreign_key_name= on the annotation, so the
			// synthesized constraint uses the conventional fk_<table>_<column>
			// name. The DB FK is introspected under that same generated name, so
			// the action change still matches and surfaces.
			name: "fallback fk_<table>_<column> name still matches and detects drift",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName: "Export",
						Name:       "file_id",
						Type:       "TEXT",
						Foreign:    "files(id)",
						OnDelete:   "SET NULL",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{exportsTable},
				Constraints: []types.DBConstraint{
					{
						Name:          "fk_exports_file_id",
						TableName:     "exports",
						Type:          "FOREIGN KEY",
						ColumnName:    "file_id",
						ForeignTable:  new("files"),
						ForeignColumn: new("id"),
						DeleteRule:    new("NO ACTION"),
						UpdateRule:    new("NO ACTION"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"fk_exports_file_id"},
				ConstraintsRemoved: []string{"fk_exports_file_id"},
			},
		},
		{
			// Self-referencing FK declared at the field level: parent_id -> the
			// same table. Changing on_delete must surface just like any other
			// field-level FK.
			name: "self-referencing field-level FK action drift surfaces",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Category", Name: "categories"}},
				Fields: []goschema.Field{
					{StructName: "Category", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Category",
						Name:           "parent_id",
						Type:           "TEXT",
						Nullable:       true,
						Foreign:        "categories(id)",
						ForeignKeyName: "fk_categories_parent",
						OnDelete:       "SET NULL",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{
					{
						Name:    "categories",
						Columns: []types.DBColumn{{Name: "id"}, {Name: "parent_id"}},
					},
				},
				Constraints: []types.DBConstraint{
					{
						Name:          "fk_categories_parent",
						TableName:     "categories",
						Type:          "FOREIGN KEY",
						ColumnName:    "parent_id",
						ForeignTable:  new("categories"),
						ForeignColumn: new("id"),
						DeleteRule:    new("NO ACTION"),
						UpdateRule:    new("NO ACTION"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"fk_categories_parent"},
				ConstraintsRemoved: []string{"fk_categories_parent"},
			},
		},
		{
			// Non-regression: a field-level FK on a column that does NOT yet
			// exist in the database must not be synthesized — the FK ships
			// inline with the CREATE TABLE / ADD COLUMN, so emitting an ADD
			// CONSTRAINT here would double-create it. No diff expected.
			name: "field-level FK on column not yet in DB → no synthesized constraint",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Export",
						Name:           "new_file_id",
						Type:           "TEXT",
						Foreign:        "files(id)",
						ForeignKeyName: "fk_export_new_file",
						OnDelete:       "SET NULL",
					},
				},
			},
			database: &types.DBSchema{
				// exports exists but new_file_id column does not yet.
				Tables: []types.DBTable{{Name: "exports", Columns: []types.DBColumn{{Name: "id"}}}},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			// Changing the referenced column (files(id) -> files(uuid)) is a
			// definitional change and must also surface as drop + add.
			name: "referenced column change surfaces as drop + add",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
				Fields: []goschema.Field{
					{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "Export",
						Name:           "file_id",
						Type:           "TEXT",
						Foreign:        "files(uuid)",
						ForeignKeyName: "fk_export_file",
					},
				},
			},
			database: &types.DBSchema{
				Tables:      []types.DBTable{exportsTable},
				Constraints: []types.DBConstraint{dbFK(new("NO ACTION"), new("NO ACTION"))},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"fk_export_file"},
				ConstraintsRemoved: []string{"fk_export_file"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.generated, tt.database, diff, nil)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expected.ConstraintsAdded),
				qt.Commentf("ConstraintsAdded=%v", diff.ConstraintsAdded))
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded, qt.Contains, expected)
			}

			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(tt.expected.ConstraintsRemoved),
				qt.Commentf("ConstraintsRemoved=%v", diff.ConstraintsRemoved))
			for _, expected := range tt.expected.ConstraintsRemoved {
				c.Assert(diff.ConstraintsRemoved, qt.Contains, expected)
			}
		})
	}
}

func TestConstraints_FieldLevelForeignKeyDeduplicatesRepeatedIntrospectionColumns(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Area", Name: "ptah_area"}},
		Fields: []goschema.Field{
			{StructName: "Area", Name: "id", Type: "TEXT", Primary: true},
			{
				StructName:     "Area",
				Name:           "tenant_id",
				Type:           "TEXT",
				Foreign:        "ptah_tenants(id)",
				ForeignKeyName: "fk_entity_tenant",
			},
		},
	}
	foreignTable := "ptah_tenants"
	database := &types.DBSchema{
		Tables: []types.DBTable{
			{Name: "ptah_area", Columns: []types.DBColumn{{Name: "id"}, {Name: "tenant_id"}}},
			{Name: "ptah_tenants", Columns: []types.DBColumn{{Name: "id"}}},
		},
		Constraints: []types.DBConstraint{
			{
				Name:           "fk_entity_tenant",
				TableName:      "ptah_area",
				Type:           "FOREIGN KEY",
				ColumnNames:    []string{"tenant_id", "tenant_id", "tenant_id"},
				ForeignTable:   &foreignTable,
				ForeignColumns: []string{"id", "id", "id"},
				DeleteRule:     new("NO ACTION"),
				UpdateRule:     new("NO ACTION"),
			},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Constraints(generated, database, diff, nil)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("added=%v removed=%v", diff.ConstraintsAdded, diff.ConstraintsRemoved))
}
