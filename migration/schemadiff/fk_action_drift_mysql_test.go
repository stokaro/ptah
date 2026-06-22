package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// TestCompare_FieldLevelForeignKeyActionDrift_MySQL is the MySQL/MariaDB
// counterpart of the PostgreSQL end-to-end acceptance test for issue #189. The
// comparator is dialect-agnostic, so it detects the same field-level FK action
// drift; this test pins that the MySQL planner now renders a real
// `ALTER TABLE ... DROP FOREIGN KEY` followed by a re-ADD carrying the new
// action (previously it emitted only a non-actionable TODO comment, producing a
// perpetually re-firing, non-functional migration).
func TestCompare_FieldLevelForeignKeyActionDrift_MySQL(t *testing.T) {
	c := qt.New(t)

	gen := exportsSchema("SET NULL")
	diff := schemadiff.CompareWithDialect(gen, exportsDBSchema("NO ACTION"), "mysql")
	c.Assert(diff.HasChanges(), qt.IsTrue)
	c.Assert(diff.ConstraintsAdded, qt.Contains, "fk_export_file")
	c.Assert(diff.ConstraintsRemoved, qt.Contains, "fk_export_file")
	// The removal info must carry the owning table and the FK type so the
	// planner can pick the DROP FOREIGN KEY syntax.
	c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemovedWithTables[0].TableName, qt.Equals, "exports")
	c.Assert(diff.ConstraintsRemovedWithTables[0].Type, qt.Equals, "FOREIGN KEY")

	nodes := mysql.New().GenerateMigrationAST(diff, gen)
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)

	const dropStmt = "ALTER TABLE exports DROP FOREIGN KEY fk_export_file;"
	const addStmt = "ALTER TABLE exports ADD CONSTRAINT fk_export_file FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE SET NULL;"

	c.Assert(strings.Contains(sql, dropStmt), qt.IsTrue,
		qt.Commentf("expected a real DROP FOREIGN KEY (not a TODO comment), got:\n%s", sql))
	c.Assert(strings.Contains(sql, "TODO"), qt.IsFalse,
		qt.Commentf("must not emit a TODO placeholder, got:\n%s", sql))

	// Ordering: the DROP must precede the ADD so the re-add does not collide
	// with the still-present same-named constraint.
	dropIdx := strings.Index(sql, dropStmt)
	addIdx := strings.Index(sql, "ADD CONSTRAINT fk_export_file")
	c.Assert(dropIdx >= 0, qt.IsTrue)
	c.Assert(addIdx >= 0, qt.IsTrue)
	c.Assert(dropIdx < addIdx, qt.IsTrue,
		qt.Commentf("DROP must come before ADD; drop@%d add@%d\n%s", dropIdx, addIdx, sql))

	// The re-added FK carries the new ON DELETE action.
	c.Assert(strings.Contains(sql, addStmt), qt.IsTrue,
		qt.Commentf("expected the FK to be re-added with ON DELETE SET NULL, got:\n%s", sql))
}

// TestCompare_FieldLevelForeignKeyActionIdempotency_MySQL proves the MySQL path
// does not loop drop+add. SET NULL == SET NULL is a no-op, and crucially the
// MariaDB RESTRICT-as-default fold makes "" / NO ACTION == RESTRICT a no-op too
// (MariaDB reports an unspecified action as RESTRICT). Without the dialect fold
// MariaDB would re-fire the migration on every generate.
func TestCompare_FieldLevelForeignKeyActionIdempotency_MySQL(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect+" SET NULL is a no-op", func(t *testing.T) {
			cc := qt.New(t)
			diff := schemadiff.CompareWithDialect(exportsSchema("SET NULL"), exportsDBSchema("SET NULL"), dialect)
			cc.Assert(diff.HasChanges(), qt.IsFalse)
		})

		t.Run(dialect+" empty action vs NO ACTION default is a no-op", func(t *testing.T) {
			cc := qt.New(t)
			diff := schemadiff.CompareWithDialect(exportsSchema(""), exportsDBSchema("NO ACTION"), dialect)
			cc.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}

	// MariaDB reports the default action as RESTRICT; InnoDB treats RESTRICT and
	// NO ACTION identically, so an FK declared without an action must round-trip
	// to no change against a RESTRICT-reporting database.
	mariaDiff := schemadiff.CompareWithDialect(exportsSchema(""), exportsDBSchema("RESTRICT"), "mariadb")
	c.Assert(mariaDiff.HasChanges(), qt.IsFalse,
		qt.Commentf("MariaDB RESTRICT default must fold to NO ACTION; got %+v", mariaDiff))
}

// TestCompare_ForeignKeyRestrictIsRealOnPostgres guards the dialect-scoping of
// the RESTRICT fold: on PostgreSQL RESTRICT and NO ACTION are genuinely
// different (RESTRICT is checked immediately, NO ACTION is deferrable), so a
// change between them MUST still be detected. Folding them globally would mask
// this real change.
func TestCompare_ForeignKeyRestrictIsRealOnPostgres(t *testing.T) {
	c := qt.New(t)

	// Entity declares ON DELETE RESTRICT, database has the default NO ACTION:
	// this is a genuine change on PostgreSQL and must be detected.
	diff := schemadiff.CompareWithDialect(exportsSchema("RESTRICT"), exportsDBSchema("NO ACTION"), "postgres")
	c.Assert(diff.HasChanges(), qt.IsTrue,
		qt.Commentf("PostgreSQL RESTRICT != NO ACTION must be detected; got %+v", diff))

	// On MySQL/MariaDB the same pair is intentionally a no-op.
	for _, dialect := range []string{"mysql", "mariadb"} {
		mysqlDiff := schemadiff.CompareWithDialect(exportsSchema("RESTRICT"), exportsDBSchema("NO ACTION"), dialect)
		c.Assert(mysqlDiff.HasChanges(), qt.IsFalse,
			qt.Commentf("%s RESTRICT == NO ACTION must be a no-op; got %+v", dialect, mysqlDiff))
	}
}
