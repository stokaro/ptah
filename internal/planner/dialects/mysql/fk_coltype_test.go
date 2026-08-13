package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// These tests pin issue #694: a column-type change on a column that participates
// in a foreign key must, on MySQL and MariaDB, drop the affected foreign key(s),
// MODIFY the column(s), and recreate the foreign key(s) — otherwise the bare
// MODIFY is rejected (MySQL errno 3780, MariaDB errno 1832). The down migration
// runs the same planner on the reversed diff and the introspected pre-change
// schema, so exercising both input shapes here proves up and down are inverses.

// typeChangeDiff builds a single-column type-change diff for one table.
func typeChangeDiff(table, column, change string) *types.SchemaDiff {
	return &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{
				TableName: table,
				ColumnsModified: []types.ColumnDiff{
					{ColumnName: column, Changes: map[string]string{"type": change}},
				},
			},
		},
	}
}

func TestPlanner_ColumnTypeChange_ReferencingFKColumn_DropModifyReadd(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := typeChangeDiff("posts", "user_id", "INTEGER -> BIGINT")
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
					{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: false, Foreign: "users(id)", OnDelete: "CASCADE"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql,
				"ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_id",
				"ALTER TABLE posts MODIFY COLUMN user_id BIGINT NOT NULL;")
			assertContainsBefore(c, sql,
				"ALTER TABLE posts MODIFY COLUMN user_id BIGINT NOT NULL;",
				"ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;")

			// Exactly one drop and one re-add — no duplication.
			c.Assert(strings.Count(sql, "DROP FOREIGN KEY fk_posts_user_id"), qt.Equals, 1)
			c.Assert(strings.Count(sql, "ADD CONSTRAINT fk_posts_user_id"), qt.Equals, 1)
		})
	}
}

func TestPlanner_ColumnTypeChange_ReferencedColumn_DropsReferencingFK(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			// Only the referenced column (users.id) changes type. The foreign key
			// lives on posts, so the DROP/ADD must target posts even though posts
			// is not the modified table.
			diff := typeChangeDiff("users", "id", "INTEGER -> BIGINT")
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
					{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: false, Foreign: "users(id)"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql,
				"ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_id",
				"ALTER TABLE users MODIFY COLUMN id BIGINT")
			assertContainsBefore(c, sql,
				"ALTER TABLE users MODIFY COLUMN id BIGINT",
				"ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id);")
		})
	}
}

func TestPlanner_ColumnTypeChange_DefaultSchemaQualifiedRemovalMatchesBareAddition(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("mysql")
	semantics.DefaultSchema = "app"
	diff := typeChangeDiff("posts", "user_id", "INTEGER -> BIGINT")
	diff.IdentifierSemantics = &semantics
	diff.ConstraintsAdded = []string{"fk_posts_user_id"}
	diff.ConstraintsAddedWithTables = []types.ConstraintAdditionInfo{{
		Name: "fk_posts_user_id", TableName: "posts", Type: "FOREIGN KEY",
		Columns: []string{"user_id"}, ForeignTable: "users", ForeignColumns: []string{"id"},
	}}
	diff.ConstraintsRemoved = []string{"fk_posts_user_id"}
	diff.ConstraintsRemovedWithTables = []types.ConstraintRemovalInfo{{
		Name: "fk_posts_user_id", TableName: "app.posts", Type: "FOREIGN KEY",
	}}
	diff.ForeignKeysRemovedWithTables = []types.ForeignKeyRemovalInfo{{
		Name: "fk_posts_user_id", TableName: "app.posts", Columns: []string{"user_id"},
		ForeignTable: "users", ForeignColumns: []string{"id"},
	}}
	desired := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
			{
				Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: false,
				Foreign: "users(id)", ForeignKeyName: "fk_posts_user_id",
			},
		},
	}

	sql := renderMySQLFamily(c, "mysql", diff, desired)

	assertContainsBefore(c, sql,
		"ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_id",
		"ALTER TABLE posts MODIFY COLUMN user_id BIGINT NOT NULL;")
	assertContainsBefore(c, sql,
		"ALTER TABLE posts MODIFY COLUMN user_id BIGINT NOT NULL;",
		"ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id);")
	c.Assert(strings.Count(sql, "DROP FOREIGN KEY fk_posts_user_id"), qt.Equals, 1)
	c.Assert(strings.Count(sql, "ADD CONSTRAINT fk_posts_user_id"), qt.Equals, 1)
}

func TestPlanner_ColumnTypeChange_IgnoresUnmatchedSupplementalForeignKeyRemoval(t *testing.T) {
	c := qt.New(t)
	diff := typeChangeDiff("posts", "user_id", "INTEGER -> BIGINT")
	diff.ForeignKeysRemovedWithTables = []types.ForeignKeyRemovalInfo{{
		Name: "fk_posts_user_id", TableName: "posts", Columns: []string{"user_id"},
		ForeignTable: "users", ForeignColumns: []string{"id"},
	}}
	desired := &goschema.Database{
		Tables: []goschema.Table{{Name: "posts", StructName: "Post"}},
		Fields: []goschema.Field{{
			Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: false,
		}},
	}

	sql := renderMySQLFamily(c, "mysql", diff, desired)

	c.Assert(sql, qt.Contains, "ALTER TABLE posts MODIFY COLUMN user_id BIGINT NOT NULL;")
	c.Assert(sql, qt.Not(qt.Contains), "DROP FOREIGN KEY")
}

func TestPlanner_ColumnTypeChange_BothEnds_SingleDropAndReadd(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			// A valid widening changes both ends of the key so the recreated
			// constraint stays type-compatible. The key must be dropped once,
			// both columns modified, then the key recreated once.
			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "posts", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "user_id", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
					}},
					{TableName: "users", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "code", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
					}},
				},
			}
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
					{Name: "code", Type: "BIGINT", StructName: "User", Unique: true},
					{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: false, Foreign: "users(code)"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			c.Assert(strings.Count(sql, "DROP FOREIGN KEY fk_posts_user_id"), qt.Equals, 1,
				qt.Commentf("expected exactly one drop, got:\n%s", sql))
			c.Assert(strings.Count(sql, "ADD CONSTRAINT fk_posts_user_id"), qt.Equals, 1,
				qt.Commentf("expected exactly one re-add, got:\n%s", sql))

			// Both column modifications sit between the single drop and re-add.
			assertContainsBefore(c, sql, "DROP FOREIGN KEY fk_posts_user_id", "MODIFY COLUMN user_id BIGINT")
			assertContainsBefore(c, sql, "DROP FOREIGN KEY fk_posts_user_id", "MODIFY COLUMN code BIGINT")
			assertContainsBefore(c, sql, "MODIFY COLUMN user_id BIGINT", "ADD CONSTRAINT fk_posts_user_id")
			assertContainsBefore(c, sql, "MODIFY COLUMN code BIGINT", "ADD CONSTRAINT fk_posts_user_id")
		})
	}
}

func TestPlanner_ColumnTypeChange_TableLevelForeignKey(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "orders", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "tenant_id", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
					}},
				},
			}
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "orders", StructName: "Order"},
					{Name: "accounts", StructName: "Account"},
				},
				Fields: []goschema.Field{
					{Name: "tenant_id", Type: "BIGINT", StructName: "Order", Nullable: false},
					{Name: "owner_id", Type: "BIGINT", StructName: "Order", Nullable: false},
				},
				Constraints: []goschema.Constraint{
					{
						Name: "fk_orders_accounts", Type: "FOREIGN KEY", Table: "orders",
						Columns: []string{"tenant_id", "owner_id"}, ForeignTable: "accounts",
						ForeignColumns: []string{"tenant_id", "id"}, OnDelete: "CASCADE",
					},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql,
				"ALTER TABLE orders DROP FOREIGN KEY fk_orders_accounts",
				"ALTER TABLE orders MODIFY COLUMN tenant_id BIGINT NOT NULL;")
			assertContainsBefore(c, sql,
				"ALTER TABLE orders MODIFY COLUMN tenant_id BIGINT NOT NULL;",
				"ALTER TABLE orders ADD CONSTRAINT fk_orders_accounts FOREIGN KEY (tenant_id, owner_id) REFERENCES accounts(tenant_id, id) ON DELETE CASCADE;")
		})
	}
}

func TestPlanner_ColumnTypeChange_SelfReferencingForeignKey(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := typeChangeDiff("categories", "parent_id", "INTEGER -> BIGINT")
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "categories", StructName: "Category"}},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "Category", Primary: true},
					{Name: "parent_id", Type: "BIGINT", StructName: "Category", Nullable: true},
				},
				SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{
					"categories": {{
						FieldName: "parent_id", Foreign: "categories(id)",
						ForeignKeyName: "fk_categories_parent", OnDelete: "SET NULL",
					}},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql,
				"ALTER TABLE categories DROP FOREIGN KEY fk_categories_parent",
				"ALTER TABLE categories MODIFY COLUMN parent_id BIGINT;")
			assertContainsBefore(c, sql,
				"ALTER TABLE categories MODIFY COLUMN parent_id BIGINT;",
				"ALTER TABLE categories ADD CONSTRAINT fk_categories_parent FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL;")
		})
	}
}

// TestPlanner_ColumnTypeChange_DownInverse feeds the planner the reverse-diff and
// introspected-schema shape the generator uses for down migrations: the type
// change runs BIGINT -> INTEGER and the foreign key is reconstructed as a
// field-level FK carrying the real database constraint name. The result must be
// the exact inverse of the up migration — drop, MODIFY back, recreate.
func TestPlanner_ColumnTypeChange_DownInverse(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			reverseDiff := typeChangeDiff("posts", "user_id", "BIGINT -> INTEGER")
			// Shaped like dbschematogo.ConvertDBSchemaToGoSchema output: the FK is
			// a field-level reference whose ForeignKeyName is the real DB name.
			dbAsGoSchema := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "Users"},
					{Name: "posts", StructName: "Posts"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "INTEGER", StructName: "Users", Primary: true},
					{
						Name: "user_id", Type: "INTEGER", StructName: "Posts", Nullable: false,
						Foreign: "users(id)", ForeignKeyName: "fk_posts_user_id", OnDelete: "CASCADE",
					},
				},
			}

			sql := renderMySQLFamily(c, dialect, reverseDiff, dbAsGoSchema)

			assertContainsBefore(c, sql,
				"ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_id",
				"ALTER TABLE posts MODIFY COLUMN user_id INTEGER NOT NULL;")
			assertContainsBefore(c, sql,
				"ALTER TABLE posts MODIFY COLUMN user_id INTEGER NOT NULL;",
				"ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;")
		})
	}
}

// TestPlanner_ColumnTypeChange_CoincidentFKDefinitionChange covers a column type
// change that coincides with a foreign-key definition change (an ON DELETE
// change) on the same existing key. The drop must be emitted ONCE, before the
// MODIFY (owned by the column-type bracketing at step 4), and the re-add ONCE,
// after the MODIFY, carrying the new definition (owned by the constraint
// machinery). A bare MODIFY before the drop would be rejected by the server, and
// a second, unguarded drop would abort the migration (MySQL has no IF EXISTS on
// constraint drops). This is the regression guard for blocker 1.
func TestPlanner_ColumnTypeChange_CoincidentFKDefinitionChange(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "posts", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "user_id", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
					}},
				},
				ConstraintsAdded:   []string{"fk_posts_user_id"},
				ConstraintsRemoved: []string{"fk_posts_user_id"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name: "fk_posts_user_id", TableName: "posts", Type: "FOREIGN KEY",
					Columns: []string{"user_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "SET NULL",
				}},
				ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
					Name: "fk_posts_user_id", TableName: "posts", Type: "FOREIGN KEY",
				}},
			}
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
					{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: true, Foreign: "users(id)", ForeignKeyName: "fk_posts_user_id", OnDelete: "SET NULL"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			// Exactly one drop and one re-add.
			c.Assert(strings.Count(sql, "DROP FOREIGN KEY fk_posts_user_id"), qt.Equals, 1,
				qt.Commentf("must not double-drop; got:\n%s", sql))
			c.Assert(strings.Count(sql, "ADD CONSTRAINT fk_posts_user_id"), qt.Equals, 1,
				qt.Commentf("must not double-add; got:\n%s", sql))

			// The drop precedes the MODIFY, the re-add follows it, and the re-add
			// carries the new ON DELETE SET NULL definition.
			assertContainsBefore(c, sql,
				"ALTER TABLE posts DROP FOREIGN KEY fk_posts_user_id",
				"ALTER TABLE posts MODIFY COLUMN user_id BIGINT;")
			assertContainsBefore(c, sql,
				"ALTER TABLE posts MODIFY COLUMN user_id BIGINT;",
				"ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL;")
		})
	}
}

// TestPlanner_ColumnTypeChange_CoHostedSharedFKName covers a foreign-key name
// shared across two tables where only one host's definition changes: posts is a
// modification (in the constraint diff) and comments is a pure column widening
// (not in the constraint diff). Both widen user_id, so BOTH keys must be dropped
// before the modifications and recreated — comments by the bracketing, posts by
// the constraint machinery. Keying on the bare name would leave comments' key
// undropped (its MODIFY would fail) and unrecreated. This is the regression
// guard for blocker 2.
func TestPlanner_ColumnTypeChange_CoHostedSharedFKName(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "posts", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "user_id", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
					}},
					{TableName: "comments", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "user_id", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
					}},
				},
				ConstraintsAdded:   []string{"fk_shared"},
				ConstraintsRemoved: []string{"fk_shared"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name: "fk_shared", TableName: "posts", Type: "FOREIGN KEY",
					Columns: []string{"user_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "SET NULL",
				}},
				ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
					Name: "fk_shared", TableName: "posts", Type: "FOREIGN KEY",
				}},
			}
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
					{Name: "comments", StructName: "Comment"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
					{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: true, Foreign: "users(id)", ForeignKeyName: "fk_shared", OnDelete: "SET NULL"},
					{Name: "user_id", Type: "BIGINT", StructName: "Comment", Nullable: false, Foreign: "users(id)", ForeignKeyName: "fk_shared"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			// Both hosts dropped and recreated exactly once each.
			c.Assert(strings.Count(sql, "ALTER TABLE posts DROP FOREIGN KEY fk_shared"), qt.Equals, 1,
				qt.Commentf("posts key must be dropped once; got:\n%s", sql))
			c.Assert(strings.Count(sql, "ALTER TABLE comments DROP FOREIGN KEY fk_shared"), qt.Equals, 1,
				qt.Commentf("comments key must be dropped once (blocker 2); got:\n%s", sql))
			c.Assert(strings.Count(sql, "ALTER TABLE posts ADD CONSTRAINT fk_shared"), qt.Equals, 1,
				qt.Commentf("posts key must be recreated once; got:\n%s", sql))
			c.Assert(strings.Count(sql, "ALTER TABLE comments ADD CONSTRAINT fk_shared"), qt.Equals, 1,
				qt.Commentf("comments key must be recreated once (blocker 2); got:\n%s", sql))

			// Each host's drop precedes its MODIFY and its re-add follows it.
			assertContainsBefore(c, sql, "ALTER TABLE comments DROP FOREIGN KEY fk_shared", "ALTER TABLE comments MODIFY COLUMN user_id BIGINT")
			assertContainsBefore(c, sql, "ALTER TABLE posts DROP FOREIGN KEY fk_shared", "ALTER TABLE posts MODIFY COLUMN user_id BIGINT")
			assertContainsBefore(c, sql, "ALTER TABLE comments MODIFY COLUMN user_id BIGINT", "ALTER TABLE comments ADD CONSTRAINT fk_shared")
			assertContainsBefore(c, sql, "ALTER TABLE posts MODIFY COLUMN user_id BIGINT", "ALTER TABLE posts ADD CONSTRAINT fk_shared")
		})
	}
}

// TestPlanner_ColumnTypeChange_RemovedOnlyForeignKey covers a foreign key that
// is being dropped (removed-only, not in the additions) whose owning table also
// has a column-type change. The key still exists in the database, so its bare
// MODIFY would be rejected; it must be pre-dropped before the modification. It
// has no re-add, and removeConstraints must not emit a second drop.
func TestPlanner_ColumnTypeChange_RemovedOnlyForeignKey(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "posts", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "author_id", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
					}},
				},
				ConstraintsRemoved: []string{"fk_posts_author"},
				ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
					{Name: "fk_posts_author", TableName: "posts", Type: "FOREIGN KEY"},
				},
			}
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
					{Name: "author_id", Type: "BIGINT", StructName: "Post", Nullable: true},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			// Dropped exactly once, before the MODIFY; never re-added.
			c.Assert(strings.Count(sql, "DROP FOREIGN KEY fk_posts_author"), qt.Equals, 1,
				qt.Commentf("removed-only FK must be dropped exactly once; got:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT fk_posts_author")
			assertContainsBefore(c, sql,
				"ALTER TABLE posts DROP FOREIGN KEY fk_posts_author",
				"ALTER TABLE posts MODIFY COLUMN author_id BIGINT;")
		})
	}
}

func TestPlanner_ColumnTypeChange_NonTypeChangeKeepsForeignKey(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			// A nullability-only change does not disturb the referential type
			// match, so no foreign key is dropped.
			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "posts", ColumnsModified: []types.ColumnDiff{
						{ColumnName: "user_id", Changes: map[string]string{"nullable": "false -> true"}},
					}},
				},
			}
			generated := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
					{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: true, Foreign: "users(id)"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			c.Assert(sql, qt.Not(qt.Contains), "DROP FOREIGN KEY")
			c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT fk_posts_user_id")
			c.Assert(sql, qt.Contains, "ALTER TABLE posts MODIFY COLUMN user_id BIGINT;")
		})
	}
}

func TestPlanner_ColumnTypeChange_NoForeignKeyLeavesBareModify(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := typeChangeDiff("users", "age", "INTEGER -> SMALLINT")
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "users", StructName: "User"}},
				Fields: []goschema.Field{
					{Name: "age", Type: "SMALLINT", StructName: "User", Nullable: true},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			c.Assert(sql, qt.Not(qt.Contains), "DROP FOREIGN KEY")
			c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT")
			c.Assert(sql, qt.Contains, "ALTER TABLE users MODIFY COLUMN age SMALLINT;")
		})
	}
}

// TestPlanner_ColumnTypeChange_MariaDBGuardsDrop confirms the MariaDB capability
// preset renders the drop with its IF EXISTS guard, matching the rest of the
// planner's MariaDB constraint drops.
func TestPlanner_ColumnTypeChange_MariaDBGuardsDrop(t *testing.T) {
	c := qt.New(t)

	diff := typeChangeDiff("posts", "user_id", "INTEGER -> BIGINT")
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
			{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: false, Foreign: "users(id)"},
		},
	}

	nodes, err := mysql.NewWithCapabilities(capability.MariaDB1011()).GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities("mariadb", capability.MariaDB1011(), nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user_id;")
}
