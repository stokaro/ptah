package generator

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// TestGenerateMigration_ForeignKeyColumnTypeChange_UpDownInverse runs the real
// generator up- and down-paths for issue #694: a foreign-key column widening on
// MySQL/MariaDB. The bare ALTER TABLE ... MODIFY the planner used to emit is
// rejected while the key exists (MySQL errno 3780, MariaDB errno 1832), so the
// migration must drop the key, MODIFY the column, then recreate the key — and
// the generated down migration must be the exact inverse.
//
// The scenario widens posts.user_slug from VARCHAR(50) to VARCHAR(100) while it
// carries a foreign key to users(slug); the referenced column keeps its type, so
// only the referencing side changes. This runs the REAL down-path
// (generateDownMigrationSQL -> reverseSchemaDiffWithSchema over the introspected
// pre-change schema), not a hand-rolled reversal.
func TestGenerateMigration_ForeignKeyColumnTypeChange_UpDownInverse(t *testing.T) {
	oldLen, newLen := 50, 100

	gen := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "BIGINT", StructName: "User", Primary: true, AutoInc: true},
			{Name: "slug", Type: "VARCHAR(100)", StructName: "User", Nullable: false, Unique: true},
			{Name: "id", Type: "BIGINT", StructName: "Post", Primary: true, AutoInc: true},
			{Name: "user_slug", Type: "VARCHAR(100)", StructName: "Post", Nullable: false, Foreign: "users(slug)", OnDelete: "CASCADE"},
		},
	}
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
				{Name: "slug", DataType: "varchar", ColumnType: "varchar(100)", CharacterMaxLength: &newLen, IsNullable: "NO", IsUnique: true},
			}},
			{Name: "posts", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
				{Name: "user_slug", DataType: "varchar", ColumnType: "varchar(50)", CharacterMaxLength: &oldLen, IsNullable: "NO"},
			}},
		},
		Constraints: []dbschematypes.DBConstraint{
			{
				Name: "fk_posts_user_slug", TableName: "posts", Type: "FOREIGN KEY", ColumnName: "user_slug",
				ForeignTable: new("users"), ForeignColumn: new("slug"),
				DeleteRule: new("CASCADE"), UpdateRule: new("NO ACTION"),
			},
		},
	}

	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			upDiff := schemadiff.CompareWithDialect(gen, dbSchema, dialect)
			c.Assert(upDiff.HasChanges(), qt.IsTrue)
			// Only the referencing column changes type; the foreign key itself is
			// not added or removed by the diff.
			c.Assert(upDiff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(upDiff.ConstraintsRemoved, qt.HasLen, 0)

			// MariaDB guards the drop with IF EXISTS; MySQL does not.
			dropStmt := "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_slug"
			if dialect == "mariadb" {
				dropStmt = "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user_slug"
			}
			addStmt := "ALTER TABLE posts ADD CONSTRAINT fk_posts_user_slug FOREIGN KEY (user_slug) REFERENCES users(slug)"

			up, err := generateUpMigrationSQL(upDiff, gen, dialect)
			c.Assert(err, qt.IsNil)
			up = legacyRenderedSQL(up)

			assertOrderedOnce(c, up, dropStmt, "ALTER TABLE posts MODIFY COLUMN user_slug VARCHAR(100) NOT NULL;", addStmt)

			down, err := generateDownMigrationSQL(upDiff, gen, dbSchema, dialect)
			c.Assert(err, qt.IsNil)
			down = legacyRenderedSQL(down)

			// The down migration reverses the widening and is the exact inverse:
			// drop, MODIFY back to varchar(50), recreate.
			assertOrderedOnce(c, down, dropStmt, "ALTER TABLE posts MODIFY COLUMN user_slug varchar(50) NOT NULL;", addStmt)
		})
	}
}

// assertOrderedOnce asserts that drop, modify, and add each appear exactly once
// in sql and in that order.
func assertOrderedOnce(c *qt.C, sql, drop, modify, add string) {
	c.Helper()
	c.Assert(strings.Count(sql, drop), qt.Equals, 1, qt.Commentf("want one %q in:\n%s", drop, sql))
	c.Assert(strings.Count(sql, modify), qt.Equals, 1, qt.Commentf("want one %q in:\n%s", modify, sql))
	c.Assert(strings.Count(sql, add), qt.Equals, 1, qt.Commentf("want one %q in:\n%s", add, sql))

	dropIdx := strings.Index(sql, drop)
	modifyIdx := strings.Index(sql, modify)
	addIdx := strings.Index(sql, add)
	c.Assert(dropIdx < modifyIdx, qt.IsTrue, qt.Commentf("drop must precede modify in:\n%s", sql))
	c.Assert(modifyIdx < addIdx, qt.IsTrue, qt.Commentf("modify must precede re-add in:\n%s", sql))
}
