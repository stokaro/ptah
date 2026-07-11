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
)

// exportsSchema returns a generated schema with an exports.file_id field-level
// FK whose ON DELETE action is set to onDelete (empty string == no action).
func exportsSchema(onDelete string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Export", Name: "exports"}},
		Fields: []goschema.Field{
			{StructName: "Export", Name: "id", Type: "TEXT", Primary: true},
			{
				StructName:     "Export",
				Name:           "file_id",
				Type:           "TEXT",
				Nullable:       true,
				Foreign:        "files(id)",
				ForeignKeyName: "fk_export_file",
				OnDelete:       onDelete,
			},
		},
	}
}

// exportsDBSchema returns an introspected DB schema for the exports table whose
// existing FK carries the given delete rule (empty string == NO ACTION default).
func exportsDBSchema(deleteRule string) *types.DBSchema {
	if deleteRule == "" {
		deleteRule = "NO ACTION"
	}
	return &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "exports",
				// Realistic column shapes so the column comparator is silent
				// and the FK action is the only variable under test. id is the
				// applied TEXT primary key; file_id is a nullable TEXT FK column.
				Columns: []types.DBColumn{
					{Name: "id", DataType: "text", IsNullable: "NO", IsPrimaryKey: true},
					{Name: "file_id", DataType: "text", IsNullable: "YES"},
				},
			},
		},
		Constraints: []types.DBConstraint{
			{
				Name:          "fk_export_file",
				TableName:     "exports",
				Type:          "FOREIGN KEY",
				ColumnName:    "file_id",
				ForeignTable:  new("files"),
				ForeignColumn: new("id"),
				DeleteRule:    new(deleteRule),
				UpdateRule:    new("NO ACTION"),
			},
		},
	}
}

// TestCompare_FieldLevelForeignKeyActionDrift is the end-to-end acceptance test
// for issue #189: an on_delete change on an existing field-level FK must be a
// real, non-empty change through the public schemadiff.Compare API, while an
// unchanged FK (including "" == NO ACTION) must be a no-op.
func TestCompare_FieldLevelForeignKeyActionDrift(t *testing.T) {
	t.Run("NO ACTION -> SET NULL is detected as a change", func(t *testing.T) {
		c := qt.New(t)

		diff := schemadiff.Compare(exportsSchema("SET NULL"), exportsDBSchema("NO ACTION"))

		c.Assert(diff.HasChanges(), qt.IsTrue)
		// Drop + add of the same FK name (modified constraints are expressed as
		// removed + added today).
		c.Assert(diff.ConstraintsAdded, qt.Contains, "fk_export_file")
		c.Assert(diff.ConstraintsRemoved, qt.Contains, "fk_export_file")
	})

	t.Run("unchanged SET NULL FK is a no-op", func(t *testing.T) {
		c := qt.New(t)

		diff := schemadiff.Compare(exportsSchema("SET NULL"), exportsDBSchema("SET NULL"))

		c.Assert(diff.HasChanges(), qt.IsFalse)
	})

	t.Run("empty action vs NO ACTION default is a no-op", func(t *testing.T) {
		c := qt.New(t)

		diff := schemadiff.Compare(exportsSchema(""), exportsDBSchema("NO ACTION"))

		c.Assert(diff.HasChanges(), qt.IsFalse)
	})
}

// TestCompare_FieldLevelForeignKeyActionIdempotency proves the fix does not
// introduce a perpetual drop+add loop: once the schema and the database agree
// on the FK action, repeated Compare runs keep reporting no changes. This is
// the explicit idempotency proof called for by the issue acceptance criteria.
func TestCompare_FieldLevelForeignKeyActionIdempotency(t *testing.T) {
	c := qt.New(t)

	generated := exportsSchema("SET NULL")
	// The database now reflects the applied SET NULL action.
	database := exportsDBSchema("SET NULL")

	// Run Compare twice against the same (already-converged) inputs. Both runs
	// must report no changes — no churn on repeated `generate`.
	first := schemadiff.Compare(generated, database)
	c.Assert(first.HasChanges(), qt.IsFalse, qt.Commentf("first run should be a no-op"))

	second := schemadiff.Compare(generated, database)
	c.Assert(second.HasChanges(), qt.IsFalse, qt.Commentf("second run should remain a no-op"))

	// And the empty-action default likewise converges and stays converged.
	noActionGen := exportsSchema("")
	noActionDB := exportsDBSchema("NO ACTION")
	c.Assert(schemadiff.Compare(noActionGen, noActionDB).HasChanges(), qt.IsFalse)
	c.Assert(schemadiff.Compare(noActionGen, noActionDB).HasChanges(), qt.IsFalse)
}

// TestCompare_FieldLevelForeignKeyActionMigrationSQL pins the emitted migration
// for a field-level FK action change end-to-end through the postgres planner +
// renderer. The comparator routes the synthesized FK into ConstraintsAdded /
// ConstraintsRemoved (a modification), so the planner must emit a DROP of the
// old constraint BEFORE the ADD of the new one — otherwise the ADD CONSTRAINT
// collides with the still-present same-named constraint. Without the planner
// fallback that re-synthesizes the field-level FK, the migration would drop the
// FK and never re-add it (a destructive, silently-broken migration).
func TestCompare_FieldLevelForeignKeyActionMigrationSQL(t *testing.T) {
	c := qt.New(t)

	gen := exportsSchema("SET NULL")
	diff := schemadiff.Compare(gen, exportsDBSchema("NO ACTION"))
	c.Assert(diff.HasChanges(), qt.IsTrue)

	nodes := postgres.New().GenerateMigrationAST(diff, gen)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	// The new FK with the action clause is emitted.
	const addStmt = "ALTER TABLE exports ADD CONSTRAINT fk_export_file FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE SET NULL;"
	c.Assert(sql, qt.Contains, addStmt,
		qt.Commentf("expected migration to ADD the FK with ON DELETE SET NULL, got:\n%s", sql))

	// The old constraint is dropped first. The comparator records the concrete
	// host table (exports) for this modify, so the planner emits a direct
	// table-qualified ALTER TABLE drop rather than the name-only information_schema
	// DO block — the latter resolves the owning table with LIMIT 1 and could drop a
	// same-named constraint on the wrong table (issue #199).
	const dropStmt = "ALTER TABLE exports DROP CONSTRAINT IF EXISTS fk_export_file;"
	c.Assert(sql, qt.Contains, dropStmt,
		qt.Commentf("expected migration to DROP the old FK from its known host table, got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
		qt.Commentf("a known-host modify must not fall back to the name-only DO block, got:\n%s", sql))

	// Ordering: the DROP must precede the ADD so the re-add does not collide.
	dropIdx := strings.Index(sql, dropStmt)
	addIdx := strings.Index(sql, addStmt)
	c.Assert(dropIdx >= 0, qt.IsTrue)
	c.Assert(addIdx >= 0, qt.IsTrue)
	c.Assert(dropIdx < addIdx, qt.IsTrue,
		qt.Commentf("DROP must come before ADD; drop@%d add@%d\n%s", dropIdx, addIdx, sql))

	// Idempotency: once applied, regenerating produces no statements.
	converged := schemadiff.Compare(exportsSchema("SET NULL"), exportsDBSchema("SET NULL"))
	noopNodes := postgres.New().GenerateMigrationAST(converged, exportsSchema("SET NULL"))
	noopSQL, err := renderer.RenderSQL("postgres", noopNodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.TrimSpace(noopSQL), qt.Equals, "",
		qt.Commentf("converged schema must produce an empty migration, got:\n%s", noopSQL))
}
