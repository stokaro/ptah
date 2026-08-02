package generator

// White-box testing required: these tests drive the unexported up/down
// generation entry points (generateUpMigrationSQL / generateDownMigrationSQL)
// over the real schemadiff comparator, which the exported migration-file API
// does not expose without a filesystem and database connection.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// fkColumnTypeDialects carries the per-dialect DROP FOREIGN KEY spelling: MariaDB
// guards the drop with IF EXISTS, MySQL does not. Kept as data so the test
// bodies stay free of dialect conditionals.
var fkColumnTypeDialects = []struct {
	name string
	drop string
}{
	{name: "mysql", drop: "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_slug"},
	{name: "mariadb", drop: "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user_slug"},
}

// TestGenerateMigration_ForeignKeyColumnTypeChange_UpDownInverse runs the real
// generator up- and down-paths for issue #694: widening posts.user_slug from
// VARCHAR(50) to VARCHAR(100) while it carries an unchanged foreign key to
// users(slug). The bare ALTER TABLE ... MODIFY the planner used to emit is
// rejected while the key exists (MySQL errno 3780, MariaDB errno 1832), so the
// migration must drop the key, MODIFY the column, then recreate the key — and
// the generated down migration must be the exact inverse. This runs the REAL
// down-path (generateDownMigrationSQL -> reverseSchemaDiffWithSchema over the
// introspected pre-change schema), not a hand-rolled reversal.
func TestGenerateMigration_ForeignKeyColumnTypeChange_UpDownInverse(t *testing.T) {
	// The target FK keeps the database's CASCADE action, so only the column type
	// changes — the foreign key is not added or removed by the diff.
	gen, dbSchema := fkColumnTypeFixtures("CASCADE")

	for _, tc := range fkColumnTypeDialects {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			upDiff := schemadiff.CompareWithDialect(gen, dbSchema, tc.name)
			c.Assert(upDiff.HasChanges(), qt.IsTrue)
			// Only the referencing column changes type; the foreign key itself is
			// not added or removed by the diff.
			c.Assert(upDiff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(upDiff.ConstraintsRemoved, qt.HasLen, 0)

			addStmt := "ALTER TABLE posts ADD CONSTRAINT fk_posts_user_slug FOREIGN KEY (user_slug) REFERENCES users(slug)"

			up, err := generateUpMigrationSQL(upDiff, gen, tc.name)
			c.Assert(err, qt.IsNil)
			up = legacyRenderedSQL(up)
			assertOrderedOnce(c, up, tc.drop, "ALTER TABLE posts MODIFY COLUMN user_slug VARCHAR(100) NOT NULL;", addStmt)

			down, err := generateDownMigrationSQL(upDiff, gen, dbSchema, tc.name)
			c.Assert(err, qt.IsNil)
			down = legacyRenderedSQL(down)
			assertOrderedOnce(c, down, tc.drop, "ALTER TABLE posts MODIFY COLUMN user_slug varchar(50) NOT NULL;", addStmt)
		})
	}
}

// TestGenerateMigration_ForeignKeyColumnTypeChange_CoincidentActionChange covers
// a column-type change that coincides with an ON DELETE change on the same
// existing foreign key (blocker 1). The bracketing owns the pre-MODIFY drop and
// the constraint machinery owns the post-MODIFY re-add with the new action, so
// the emitted order must be DROP -> MODIFY -> ADD with exactly one drop and one
// add. The down migration restores the prior action, still after its own drop
// and MODIFY.
func TestGenerateMigration_ForeignKeyColumnTypeChange_CoincidentActionChange(t *testing.T) {
	gen, dbSchema := fkColumnTypeFixtures("SET NULL")
	gen.Fields[3].Nullable = true
	dbSchema.Tables[1].Columns[1].IsNullable = "YES"

	for _, tc := range fkColumnTypeDialects {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			upDiff := schemadiff.CompareWithDialect(gen, dbSchema, tc.name)
			c.Assert(upDiff.HasChanges(), qt.IsTrue)
			// The FK definition change is recorded as a same-name add + remove.
			c.Assert(upDiff.ConstraintsAdded, qt.Contains, "fk_posts_user_slug")
			c.Assert(upDiff.ConstraintsRemoved, qt.Contains, "fk_posts_user_slug")

			up, err := generateUpMigrationSQL(upDiff, gen, tc.name)
			c.Assert(err, qt.IsNil)
			up = legacyRenderedSQL(up)
			// One re-add (owned by the constraint machinery); the drop count is
			// asserted by assertOrderedOnce against the dialect-specific spelling.
			c.Assert(strings.Count(up, "ADD CONSTRAINT fk_posts_user_slug"), qt.Equals, 1, qt.Commentf("UP:\n%s", up))
			assertOrderedOnce(c, up, tc.drop,
				"ALTER TABLE posts MODIFY COLUMN user_slug VARCHAR(100);",
				"ALTER TABLE posts ADD CONSTRAINT fk_posts_user_slug FOREIGN KEY (user_slug) REFERENCES users(slug) ON DELETE SET NULL;")

			down, err := generateDownMigrationSQL(upDiff, gen, dbSchema, tc.name)
			c.Assert(err, qt.IsNil)
			down = legacyRenderedSQL(down)
			c.Assert(strings.Count(down, "ADD CONSTRAINT fk_posts_user_slug"), qt.Equals, 1, qt.Commentf("DOWN:\n%s", down))
			// Down restores the prior CASCADE action, after its own drop and MODIFY.
			assertOrderedOnce(c, down, tc.drop,
				"ALTER TABLE posts MODIFY COLUMN user_slug varchar(50);",
				"ALTER TABLE posts ADD CONSTRAINT fk_posts_user_slug FOREIGN KEY (user_slug) REFERENCES users(slug) ON DELETE CASCADE")
		})
	}
}

// fkColumnTypeFixtures builds the target schema and introspected pre-change
// database for the posts.user_slug widening. When genOnDelete is non-empty the
// target's ON DELETE action differs from the database's CASCADE, producing a
// coincident foreign-key definition change alongside the column-type change.
func fkColumnTypeFixtures(genOnDelete string) (*goschema.Database, *dbschematypes.DBSchema) {
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
			{Name: "user_slug", Type: "VARCHAR(100)", StructName: "Post", Nullable: false, Foreign: "users(slug)", OnDelete: genOnDelete},
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
	return gen, dbSchema
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
