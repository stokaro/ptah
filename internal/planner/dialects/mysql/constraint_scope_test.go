package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// mysqlFamilyDialects renders the same planner output through both renderers
// that consume the MySQL planner, so every scenario is asserted for the full
// MySQL family. The suite deliberately uses the strict MySQL capability
// preset (mysql.New()) for BOTH renderers: the (table,name) ownership
// discipline it pins (issue #207) is capability-independent and must hold
// even without IF EXISTS guards. The production mariadb configuration —
// GetPlanner("mariadb"), which adds guard intent via the MariaDB preset — is
// covered in capability_gating_test.go and the planner-level wiring test.
var mysqlFamilyDialects = []string{"mysql", "mariadb"}

// renderMySQLFamily generates the migration AST once per invocation and
// renders it with the given dialect.
// renderMySQLFamily plans and renders a diff, completing any column
// modification whose operand the fixture left out.
//
// A ColumnDiff carries the column the plan renders from, and a comparison
// always fills it. A hand-built fixture states the CHANGE -- which is what
// each of these tests is about -- and leaving it to also restate the column
// would put the same declaration in two places, where a reader has to check
// they agree. This resolves it from the declaration the plan is applied
// against, which is what the comparison does and the only answer that can be
// right.
func renderMySQLFamily(c *qt.C, dialect string, diff *difftypes.SchemaDiff, desired *schemamodel.Database) string {
	diff = withDeclaredObjects(diff, desired)
	nodes, err := mysql.New().GenerateMigrationAST(withDeclaredObjects(diff, desired), desired)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(dialect, nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	return sql
}

func assertContainsBefore(c *qt.C, sql, earlier, later string) {
	earlierIndex := strings.Index(sql, earlier)
	laterIndex := strings.Index(sql, later)
	c.Assert(earlierIndex >= 0, qt.IsTrue, qt.Commentf("%q not found in SQL:\n%s", earlier, sql))
	c.Assert(laterIndex >= 0, qt.IsTrue, qt.Commentf("%q not found in SQL:\n%s", later, sql))
	c.Assert(earlierIndex < laterIndex, qt.IsTrue, qt.Commentf("%q must appear before %q:\n%s", earlier, later, sql))
}

func TestPlanner_GenerateMigrationAST_CompositeForeignKeyAddition(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"fk_orders_accounts"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{
						Name:           "fk_orders_accounts",
						TableName:      "orders",
						Type:           "FOREIGN KEY",
						Columns:        []string{"tenant_id", "owner_id"},
						ForeignTable:   "accounts",
						ForeignColumn:  "tenant_id",
						ForeignColumns: []string{"tenant_id", "id"},
						OnDelete:       "CASCADE",
					},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})

			c.Assert(sql, qt.Contains, "ALTER TABLE orders ADD CONSTRAINT fk_orders_accounts FOREIGN KEY (tenant_id, owner_id) REFERENCES accounts(tenant_id, id) ON DELETE CASCADE;",
				qt.Commentf("composite FK addition must preserve all referenced columns; got:\n%s", sql))
		})
	}
}

func TestPlanner_GenerateMigrationAST_ForeignKeyIndexesDropAfterConstraints(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"fk_users_account_id", "fk_users_manager_id"},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
					{Name: "fk_users_account_id", TableName: "users", Type: "FOREIGN KEY"},
					{Name: "fk_users_manager_id", TableName: "users", Type: "FOREIGN KEY"},
				},
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "fk_users_account_id", TableName: "users"},
					{Name: "fk_users_manager_id", TableName: "users"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})

			assertContainsBefore(
				c,
				sql,
				"ALTER TABLE users DROP FOREIGN KEY fk_users_account_id;",
				"DROP INDEX fk_users_account_id ON users;",
			)
			assertContainsBefore(
				c,
				sql,
				"ALTER TABLE users DROP FOREIGN KEY fk_users_manager_id;",
				"DROP INDEX fk_users_manager_id ON users;",
			)
		})
	}
}

func TestPlanner_GenerateMigrationAST_TableQualifiedCheckAndUniqueAdditions(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		wantDrop string
		wantSQL  string
	}{
		{
			name: "unique to check",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"products_quantity_guard"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
					Name:            "products_quantity_guard",
					TableName:       "products",
					Type:            "CHECK",
					CheckExpression: "quantity > 10",
				}},
				ConstraintsRemoved: []string{"products_quantity_guard"},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
					Name:      "products_quantity_guard",
					TableName: "products",
					Type:      "UNIQUE",
				}},
			},
			wantDrop: "ALTER TABLE products DROP INDEX products_quantity_guard;",
			wantSQL:  "ALTER TABLE products ADD CONSTRAINT products_quantity_guard CHECK (quantity > 10);",
		},
		{
			name: "check to unique",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"accounts_identity"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
					Name:      "accounts_identity",
					TableName: "accounts",
					Type:      "UNIQUE",
					Columns:   []string{"email", "region"},
				}},
				ConstraintsRemoved: []string{"accounts_identity"},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
					Name:      "accounts_identity",
					TableName: "accounts",
					Type:      "CHECK",
				}},
			},
			wantDrop: "ALTER TABLE accounts DROP CONSTRAINT accounts_identity;",
			wantSQL:  "ALTER TABLE accounts ADD CONSTRAINT accounts_identity UNIQUE (email, region);",
		},
	}

	for _, dialect := range mysqlFamilyDialects {
		for _, tt := range tests {
			t.Run(dialect+"/"+tt.name, func(t *testing.T) {
				c := qt.New(t)

				sql := renderMySQLFamily(c, dialect, tt.diff, &schemamodel.Database{})

				assertContainsBefore(c, sql, tt.wantDrop, tt.wantSQL)
				c.Assert(sql, qt.Contains, tt.wantSQL)
				c.Assert(strings.Count(sql, tt.wantSQL), qt.Equals, 1)
			})
		}
	}
}

func TestPlanner_GenerateMigrationAST_DropsFKBeforeRemovingItsTable(t *testing.T) {
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{"tasks", "projects", "accounts"},
		ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
			{Name: "fk_tasks_project", TableName: "tasks", Type: "FOREIGN KEY"},
			{Name: "fk_projects_account", TableName: "projects", Type: "FOREIGN KEY"},
		},
	}

	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})

			assertContainsBefore(c, sql, "ALTER TABLE tasks DROP FOREIGN KEY fk_tasks_project;", "DROP TABLE IF EXISTS tasks;")
			assertContainsBefore(c, sql, "ALTER TABLE projects DROP FOREIGN KEY fk_projects_account;", "DROP TABLE IF EXISTS projects;")
		})
	}
}

// TestPlanner_GenerateMigrationAST_SharedConstraintName_ModifiedOnOneTablePurelyRemovedOnAnother
// guards issue #207 — the MySQL-family sibling of the postgres issue #206. A
// single constraint name is shared across two tables: it is MODIFIED on table
// A (the name lands in ConstraintsAdded) and PURELY REMOVED on table B (B has
// no addition).
//
// The buggy implementation failed on BOTH sides:
//   - add side: the modify pre-drop resolved its host via a name-keyed
//     single-winner map (last removal entry wins), so with two removal hosts it
//     could drop B (the wrong, pure-removal host) and leave A's stale
//     constraint in place — A's re-ADD then collides (errno 1826/3822);
//   - remove side: the modify-skip was keyed on the bare name, so B's pure
//     removal was treated as "owned by addNewConstraints" and skipped — B's
//     stale constraint would survive forever.
//
// The fix keys both sides on (table, name): A is dropped-then-re-added by the
// add side, B is dropped exactly once by removeConstraints, and no statement
// needs the (MySQL-unsupported) IF EXISTS guard. Removal-entry order must not
// matter, so every subtest runs with both orderings.
func TestPlanner_GenerateMigrationAST_SharedConstraintName_ModifiedOnOneTablePurelyRemovedOnAnother(t *testing.T) {
	t.Run("foreign key", func(t *testing.T) {
		orderings := map[string][]difftypes.ConstraintRemovalInfo{
			"modified host listed first": {
				{Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY"},
				{Name: "shared_fk", TableName: "pages", Type: "FOREIGN KEY"},
			},
			"purely-removed host listed first": {
				{Name: "shared_fk", TableName: "pages", Type: "FOREIGN KEY"},
				{Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY"},
			},
		}
		for orderName, removals := range orderings {
			for _, dialect := range mysqlFamilyDialects {
				t.Run(dialect+"/"+orderName, func(t *testing.T) {
					c := qt.New(t)

					diff := &difftypes.SchemaDiff{
						ConstraintsAdded:   []string{"shared_fk"},
						ConstraintsRemoved: []string{"shared_fk"},
						ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
							{
								Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY",
								Columns: []string{"author_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "CASCADE",
							},
						},
						ConstraintsRemovedWithTables: removals,
					}

					sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})

					// The modified host is dropped exactly once with FK syntax and
					// re-added exactly once, drop before add.
					c.Assert(strings.Count(sql, "ALTER TABLE articles DROP FOREIGN KEY shared_fk;"), qt.Equals, 1,
						qt.Commentf("modified host must be dropped exactly once from its own table; got:\n%s", sql))
					c.Assert(strings.Count(sql, "ALTER TABLE articles ADD CONSTRAINT shared_fk FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE;"), qt.Equals, 1,
						qt.Commentf("modified FK must be re-added on its host; got:\n%s", sql))
					c.Assert(strings.Count(sql, "ADD CONSTRAINT shared_fk"), qt.Equals, 1,
						qt.Commentf("only the modified host may be re-added; got:\n%s", sql))

					// The purely-removed host is dropped exactly once and never
					// re-added. This is the assertion that fails against the
					// bare-name remove-side skip.
					c.Assert(strings.Count(sql, "ALTER TABLE pages DROP FOREIGN KEY shared_fk;"), qt.Equals, 1,
						qt.Commentf("purely-removed host must be dropped exactly once, not skipped; got:\n%s", sql))
					c.Assert(strings.Count(sql, "DROP FOREIGN KEY shared_fk;"), qt.Equals, 2,
						qt.Commentf("exactly one drop per host, no more; got:\n%s", sql))

					// MySQL 8 accepts no IF EXISTS on constraint drops — the plan
					// must be valid without it.
					c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS",
						qt.Commentf("MySQL-family constraint scoping must not lean on IF EXISTS; got:\n%s", sql))

					// Ordering: the modified host's drop precedes its re-add; the
					// pure removal is owned by removeConstraints and lands after.
					articlesDrop := strings.Index(sql, "ALTER TABLE articles DROP FOREIGN KEY shared_fk")
					articlesAdd := strings.Index(sql, "ALTER TABLE articles ADD CONSTRAINT shared_fk")
					pagesDrop := strings.Index(sql, "ALTER TABLE pages DROP FOREIGN KEY shared_fk")
					c.Assert(articlesDrop >= 0 && articlesAdd >= 0 && pagesDrop >= 0, qt.IsTrue)
					c.Assert(articlesDrop < articlesAdd, qt.IsTrue,
						qt.Commentf("modified host's drop must precede its re-add; got:\n%s", sql))
					c.Assert(pagesDrop > articlesAdd, qt.IsTrue,
						qt.Commentf("pure removal must come from removeConstraints (after the re-add); got:\n%s", sql))
				})
			}
		}
	})

	t.Run("check constraint", func(t *testing.T) {
		orderings := map[string][]difftypes.ConstraintRemovalInfo{
			"modified host listed first": {
				{Name: "shared_check", TableName: "articles", Type: "CHECK"},
				{Name: "shared_check", TableName: "pages", Type: "CHECK"},
			},
			"purely-removed host listed first": {
				{Name: "shared_check", TableName: "pages", Type: "CHECK"},
				{Name: "shared_check", TableName: "articles", Type: "CHECK"},
			},
		}
		for orderName, removals := range orderings {
			for _, dialect := range mysqlFamilyDialects {
				t.Run(dialect+"/"+orderName, func(t *testing.T) {
					c := qt.New(t)

					diff := &difftypes.SchemaDiff{
						ConstraintsAdded:   []string{"shared_check"},
						ConstraintsRemoved: []string{"shared_check"},
						ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
							// The body a comparison carries. The assertions here are
							// about statement ORDER, but a record with no expression
							// describes no constraint (stokaro/ptah#2315).
							{
								Name: "shared_check", TableName: "articles", Type: "CHECK",
								CheckExpression: "status IN ('draft', 'published')",
							},
						},
						ConstraintsRemovedWithTables: removals,
					}
					desired := &schemamodel.Database{
						Constraints: []schemamodel.Constraint{
							{StructName: "Article", Name: "shared_check", Type: "CHECK", Table: "articles", CheckExpression: "status IN ('draft', 'published')"},
						},
					}

					sql := renderMySQLFamily(c, dialect, diff, desired)

					// Modified host: dropped exactly once from ITS table (the
					// name-keyed single-winner map could drop pages instead and
					// leave this collision in place), re-added exactly once.
					c.Assert(strings.Count(sql, "ALTER TABLE articles DROP CONSTRAINT shared_check;"), qt.Equals, 1,
						qt.Commentf("modified host must be dropped exactly once from its own table; got:\n%s", sql))
					c.Assert(strings.Count(sql, "ALTER TABLE articles ADD CONSTRAINT shared_check CHECK (status IN ('draft', 'published'));"), qt.Equals, 1,
						qt.Commentf("modified CHECK must be re-added on its host; got:\n%s", sql))
					c.Assert(strings.Count(sql, "ADD CONSTRAINT shared_check"), qt.Equals, 1,
						qt.Commentf("only the modified host may be re-added; got:\n%s", sql))

					// Pure removal: dropped exactly once, by removeConstraints.
					c.Assert(strings.Count(sql, "ALTER TABLE pages DROP CONSTRAINT shared_check;"), qt.Equals, 1,
						qt.Commentf("purely-removed host must be dropped exactly once; got:\n%s", sql))
					c.Assert(strings.Count(sql, "DROP CONSTRAINT shared_check;"), qt.Equals, 2,
						qt.Commentf("exactly one drop per host, no more; got:\n%s", sql))
					c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS",
						qt.Commentf("MySQL-family constraint scoping must not lean on IF EXISTS; got:\n%s", sql))

					articlesDrop := strings.Index(sql, "ALTER TABLE articles DROP CONSTRAINT shared_check")
					articlesAdd := strings.Index(sql, "ALTER TABLE articles ADD CONSTRAINT shared_check")
					pagesDrop := strings.Index(sql, "ALTER TABLE pages DROP CONSTRAINT shared_check")
					c.Assert(articlesDrop >= 0 && articlesAdd >= 0 && pagesDrop >= 0, qt.IsTrue)
					c.Assert(articlesDrop < articlesAdd, qt.IsTrue,
						qt.Commentf("modified host's drop must precede its re-add; got:\n%s", sql))
					c.Assert(pagesDrop > articlesAdd, qt.IsTrue,
						qt.Commentf("pure removal must come from removeConstraints (after the re-add); got:\n%s", sql))
				})
			}
		}
	})
}

// TestPlanner_GenerateMigrationAST_ModifiedFK_EveryHostDroppedAndReadded covers
// the multi-host modify (the issue #197 mixin shape, both hosts drifted): each
// host must get its own table-qualified DROP FOREIGN KEY + re-ADD pair, with
// each drop preceding its own re-add. A single-host modify (the issue #189
// action-drift shape) is the degenerate case and is asserted too.
func TestPlanner_GenerateMigrationAST_ModifiedFK_EveryHostDroppedAndReadded(t *testing.T) {
	t.Run("two hosts, distinct actions", func(t *testing.T) {
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &difftypes.SchemaDiff{
					ConstraintsAdded:   []string{"fk_customer"},
					ConstraintsRemoved: []string{"fk_customer"},
					ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
						{
							Name: "fk_customer", TableName: "orders", Type: "FOREIGN KEY",
							Columns: []string{"customer_id"}, ForeignTable: "customers", ForeignColumn: "id", OnDelete: "CASCADE",
						},
						{
							Name: "fk_customer", TableName: "invoices", Type: "FOREIGN KEY",
							Columns: []string{"customer_id"}, ForeignTable: "customers", ForeignColumn: "id", OnDelete: "SET NULL",
						},
					},
					ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
						{Name: "fk_customer", TableName: "orders", Type: "FOREIGN KEY"},
						{Name: "fk_customer", TableName: "invoices", Type: "FOREIGN KEY"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})

				c.Assert(strings.Count(sql, "ALTER TABLE orders DROP FOREIGN KEY fk_customer;"), qt.Equals, 1,
					qt.Commentf("orders host dropped exactly once; got:\n%s", sql))
				c.Assert(strings.Count(sql, "ALTER TABLE invoices DROP FOREIGN KEY fk_customer;"), qt.Equals, 1,
					qt.Commentf("invoices host dropped exactly once; got:\n%s", sql))
				c.Assert(sql, qt.Contains, "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE;",
					qt.Commentf("orders re-added with its own action; got:\n%s", sql))
				c.Assert(sql, qt.Contains, "ALTER TABLE invoices ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE SET NULL;",
					qt.Commentf("invoices re-added with its own action; got:\n%s", sql))
				c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS")

				for _, host := range []string{"orders", "invoices"} {
					dropIdx := strings.Index(sql, "ALTER TABLE "+host+" DROP FOREIGN KEY fk_customer")
					addIdx := strings.Index(sql, "ALTER TABLE "+host+" ADD CONSTRAINT fk_customer")
					c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
						qt.Commentf("%s: drop must precede its re-add; drop=%d add=%d; got:\n%s", host, dropIdx, addIdx, sql))
				}
			})
		}
	})

	t.Run("single host (issue #189 parity)", func(t *testing.T) {
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &difftypes.SchemaDiff{
					ConstraintsAdded:   []string{"fk_post_owner"},
					ConstraintsRemoved: []string{"fk_post_owner"},
					ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
						{
							Name: "fk_post_owner", TableName: "posts", Type: "FOREIGN KEY",
							Columns: []string{"owner_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "SET NULL",
						},
					},
					ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
						{Name: "fk_post_owner", TableName: "posts", Type: "FOREIGN KEY"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})

				c.Assert(strings.Count(sql, "ALTER TABLE posts DROP FOREIGN KEY fk_post_owner;"), qt.Equals, 1,
					qt.Commentf("exactly one drop; got:\n%s", sql))
				c.Assert(strings.Count(sql, "ALTER TABLE posts ADD CONSTRAINT fk_post_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE SET NULL;"), qt.Equals, 1,
					qt.Commentf("exactly one re-add; got:\n%s", sql))
				dropIdx := strings.Index(sql, "DROP FOREIGN KEY fk_post_owner")
				addIdx := strings.Index(sql, "ADD CONSTRAINT fk_post_owner")
				c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
					qt.Commentf("drop must precede re-add; got:\n%s", sql))
			})
		}
	})
}

// TestPlanner_GenerateMigrationAST_ModifyDrop_HostScopedWhenAddedHostsAbsent
// guards the reverse/down shape: ConstraintsAdded carries the name but
// ConstraintsAddedWithTables is EMPTY (reverseConstraintAdditions restores only
// FOREIGN KEYs, and nothing at all when the introspected schema is absent). The
// add side must drop every recorded removal host, and removeConstraints must
// then skip the name entirely — MySQL has no IF EXISTS on constraint drops, so
// a second drop of the same (table, name) would abort the whole migration.
// This is exactly the failure mode that sank the naive remove-side-only port of
// the postgres #206 fix (see issue #207).
func TestPlanner_GenerateMigrationAST_ModifyDrop_HostScopedWhenAddedHostsAbsent(t *testing.T) {
	t.Run("check constraint", func(t *testing.T) {
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &difftypes.SchemaDiff{
					ConstraintsAdded:   []string{"chk_down"},
					ConstraintsRemoved: []string{"chk_down"},
					ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
						{Name: "chk_down", TableName: "things", Type: "CHECK"},
					},
				}
				desired := &schemamodel.Database{
					Constraints: []schemamodel.Constraint{
						{StructName: "Thing", Name: "chk_down", Type: "CHECK", Table: "things", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, desired)

				// Exactly ONE drop in the whole plan: the add side owns it and
				// removeConstraints must not emit a second, unguarded one.
				c.Assert(strings.Count(sql, "ALTER TABLE things DROP CONSTRAINT chk_down;"), qt.Equals, 1,
					qt.Commentf("the drop must be emitted exactly once across both planner phases; got:\n%s", sql))
				c.Assert(sql, qt.Contains, "ALTER TABLE things ADD CONSTRAINT chk_down CHECK (qty >= 0);",
					qt.Commentf("modified constraint must still be re-added; got:\n%s", sql))
				dropIdx := strings.Index(sql, "ALTER TABLE things DROP CONSTRAINT chk_down")
				addIdx := strings.Index(sql, "ALTER TABLE things ADD CONSTRAINT chk_down")
				c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
					qt.Commentf("drop must precede the re-add; got:\n%s", sql))
			})
		}
	})

	t.Run("check constraint, two removal hosts", func(t *testing.T) {
		// The down migration of a multi-host non-FK constraint modify arrives in
		// this shape: the name once per host, and a record per host on both
		// sides -- reverseConstraintAdditions reconstructs a CHECK from the
		// pre-change database.
		//
		// Every recorded host must be dropped exactly once, deduped across the
		// duplicated names, and removeConstraints must emit nothing on top. A
		// regression that drops only the first host would leave the second
		// host's stale constraint in place with the whole suite green, which is
		// why the per-host counts below are load-bearing.
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &difftypes.SchemaDiff{
					ConstraintsAdded:   []string{"shared_check", "shared_check"},
					ConstraintsRemoved: []string{"shared_check", "shared_check"},
					// A record per host. The reversal reconstructs a CHECK from the
					// pre-change database, so this is the shape a down migration
					// arrives in (stokaro/ptah#2315).
					ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
						{Name: "shared_check", TableName: "articles", Type: "CHECK", CheckExpression: "qty >= 0"},
						{Name: "shared_check", TableName: "pages", Type: "CHECK", CheckExpression: "qty >= 0"},
					},
					ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
						{Name: "shared_check", TableName: "articles", Type: "CHECK"},
						{Name: "shared_check", TableName: "pages", Type: "CHECK"},
					},
				}
				desired := &schemamodel.Database{
					Constraints: []schemamodel.Constraint{
						{StructName: "Article", Name: "shared_check", Type: "CHECK", Table: "articles", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, desired)

				c.Assert(strings.Count(sql, "ALTER TABLE articles DROP CONSTRAINT shared_check;"), qt.Equals, 1,
					qt.Commentf("first removal host must be dropped exactly once; got:\n%s", sql))
				c.Assert(strings.Count(sql, "ALTER TABLE pages DROP CONSTRAINT shared_check;"), qt.Equals, 1,
					qt.Commentf("second removal host must be dropped exactly once, not skipped and not doubled; got:\n%s", sql))
				c.Assert(strings.Count(sql, "DROP CONSTRAINT shared_check;"), qt.Equals, 2,
					qt.Commentf("exactly one drop per recorded host across BOTH planner phases; got:\n%s", sql))
				c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS")

				// Each host's drop precedes ITS OWN re-add. The older assertion put
				// both drops before a single re-add, which is the shape one bare
				// name standing in for two hosts produced; with a record per host
				// the plan pairs them, and pairing is the property that matters --
				// a host re-added before its own drop collides.
				for _, host := range []string{"articles", "pages"} {
					drop := strings.Index(sql, "ALTER TABLE "+host+" DROP CONSTRAINT shared_check")
					add := strings.Index(sql, "ALTER TABLE "+host+" ADD CONSTRAINT shared_check")
					c.Assert(drop >= 0, qt.IsTrue, qt.Commentf("%s is dropped; got:\n%s", host, sql))
					c.Assert(add >= 0, qt.IsTrue, qt.Commentf("%s is re-added; got:\n%s", host, sql))
					c.Assert(drop < add, qt.IsTrue,
						qt.Commentf("%s must be dropped before it is re-added; got:\n%s", host, sql))
				}
			})
		}
	})

	t.Run("empty-TableName removal entry emits nothing", func(t *testing.T) {
		// Design decision: a removal entry with no recorded host cannot be
		// dropped on MySQL — there is no valid table-qualified ALTER TABLE to
		// emit and no runtime name-only fallback (no anonymous-block
		// equivalent of the postgres information_schema DO block). Both new
		// guards (emitModifyDropForName and removeConstraints) must skip the
		// entry silently: no malformed statement with an empty table name, no
		// abort. The re-add still proceeds alone.
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &difftypes.SchemaDiff{
					ConstraintsAdded:   []string{"chk_hostless"},
					ConstraintsRemoved: []string{"chk_hostless"},
					ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
						{Name: "chk_hostless", TableName: "", Type: "CHECK"},
					},
				}
				desired := &schemamodel.Database{
					Constraints: []schemamodel.Constraint{
						{StructName: "Thing", Name: "chk_hostless", Type: "CHECK", Table: "things", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, desired)

				c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT chk_hostless",
					qt.Commentf("a hostless removal entry must be skipped, not dropped; got:\n%s", sql))
				c.Assert(sql, qt.Not(qt.Contains), "ALTER TABLE  DROP",
					qt.Commentf("no malformed empty-table ALTER may be emitted; got:\n%s", sql))
				c.Assert(strings.Count(sql, "ALTER TABLE things ADD CONSTRAINT chk_hostless CHECK (qty >= 0);"), qt.Equals, 1,
					qt.Commentf("the re-add still proceeds alone; got:\n%s", sql))
			})
		}
	})

	t.Run("empty-TableName addition entry is treated as hostless", func(t *testing.T) {
		// A ConstraintsAddedWithTables entry with no recorded host must not
		// count as a recorded addition host on either side. If it did, the
		// add side would see a non-empty addedHosts set containing only ""
		// (matching no real removal host) and skip the required pre-drop,
		// while removeConstraints would see addedHostCounts > 0, disengage
		// its hostless-re-add rule, and emit the drop AFTER the re-add —
		// killing the freshly added constraint. With the guard, the name
		// behaves exactly like a hostless re-add: one pre-drop from the add
		// side, then the re-add, nothing from removeConstraints.
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &difftypes.SchemaDiff{
					ConstraintsAdded:   []string{"chk_ghost"},
					ConstraintsRemoved: []string{"chk_ghost"},
					ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
						// The host and the body a comparison resolves. The record used
						// to carry neither and the planner recovered both from the
						// declaration; that route is withdrawn (stokaro/ptah#2315).
						{Name: "chk_ghost", TableName: "things", Type: "CHECK", CheckExpression: "qty >= 0"},
					},
					ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
						{Name: "chk_ghost", TableName: "things", Type: "CHECK"},
					},
				}
				desired := &schemamodel.Database{
					Constraints: []schemamodel.Constraint{
						{StructName: "Thing", Name: "chk_ghost", Type: "CHECK", Table: "things", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, desired)

				c.Assert(strings.Count(sql, "ALTER TABLE things DROP CONSTRAINT chk_ghost;"), qt.Equals, 1,
					qt.Commentf("the recorded removal host must be dropped exactly once; got:\n%s", sql))
				c.Assert(strings.Count(sql, "ALTER TABLE things ADD CONSTRAINT chk_ghost CHECK (qty >= 0);"), qt.Equals, 1,
					qt.Commentf("the re-add must still be emitted; got:\n%s", sql))
				dropIdx := strings.Index(sql, "ALTER TABLE things DROP CONSTRAINT chk_ghost")
				addIdx := strings.Index(sql, "ALTER TABLE things ADD CONSTRAINT chk_ghost")
				c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
					qt.Commentf("the drop must be the add-side pre-drop (before the re-add), not a removeConstraints drop after it; got:\n%s", sql))
			})
		}
	})

	t.Run("field-level foreign key", func(t *testing.T) {
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &difftypes.SchemaDiff{
					ConstraintsAdded:   []string{"fk_post_owner"},
					ConstraintsRemoved: []string{"fk_post_owner"},
					// The record a comparison carries for a key synthesized from a
					// field: it folds the synthesis into the same map before it
					// compares, so this reaches a diff like any other addition
					// (stokaro/ptah#2315).
					ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
						Name: "fk_post_owner", TableName: "posts", Type: "FOREIGN KEY",
						Columns: []string{"owner_id"}, ForeignTable: "users",
						ForeignColumn: "id", ForeignColumns: []string{"id"},
						OnDelete: "CASCADE",
					}},
					ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
						{Name: "fk_post_owner", TableName: "posts", Type: "FOREIGN KEY"},
					},
				}
				desired := &schemamodel.Database{
					Tables: []schemamodel.Table{
						{StructName: "User", Name: "users"},
						{StructName: "Post", Name: "posts"},
					},
					Fields: []schemamodel.Field{
						{
							StructName:     "Post",
							Name:           "owner_id",
							Type:           "INT",
							Foreign:        "users(id)",
							ForeignKeyName: "fk_post_owner",
							OnDelete:       "CASCADE",
						},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, desired)

				c.Assert(strings.Count(sql, "ALTER TABLE posts DROP FOREIGN KEY fk_post_owner;"), qt.Equals, 1,
					qt.Commentf("the drop must be emitted exactly once across both planner phases; got:\n%s", sql))
				c.Assert(strings.Count(sql, "ALTER TABLE posts ADD CONSTRAINT fk_post_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE;"), qt.Equals, 1,
					qt.Commentf("field-level FK must be re-added via the synthesis fallback; got:\n%s", sql))
				dropIdx := strings.Index(sql, "DROP FOREIGN KEY fk_post_owner")
				addIdx := strings.Index(sql, "ADD CONSTRAINT fk_post_owner")
				c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
					qt.Commentf("drop must precede the re-add; got:\n%s", sql))
			})
		}
	})
}

// TestPlanner_GenerateMigrationAST_PureConstraintRemovals_TableQualified locks
// the pure-removal path: every removal with a known host is dropped exactly
// once with the type-correct syntax; non-FK constraints on tables that are
// themselves being dropped are skipped; a duplicate removal entry for the same
// (table, name) is deduped — MySQL would abort on the second, unguarded drop
// otherwise.
func TestPlanner_GenerateMigrationAST_PureConstraintRemovals_TableQualified(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{
				TablesRemoved: []string{"obsolete"},
				ConstraintsRemoved: []string{
					"fk_orders_customer", "chk_qty", "pk_legacy", "chk_on_obsolete", "fk_orders_customer", "chk_orphan",
				},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
					{Name: "fk_orders_customer", TableName: "orders", Type: "FOREIGN KEY"},
					{Name: "chk_qty", TableName: "things", Type: "CHECK"},
					{Name: "pk_legacy", TableName: "legacy", Type: "PRIMARY KEY"},
					{Name: "chk_on_obsolete", TableName: "obsolete", Type: "CHECK"},
					// Duplicate entry for an already-listed host: must be deduped.
					{Name: "fk_orders_customer", TableName: "orders", Type: "FOREIGN KEY"},
					// Entry with no recorded host: must be skipped silently —
					// MySQL has no name-only runtime fallback, and an empty
					// table name would render a malformed ALTER.
					{Name: "chk_orphan", TableName: "", Type: "CHECK"},
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})

			c.Assert(strings.Count(sql, "ALTER TABLE orders DROP FOREIGN KEY fk_orders_customer;"), qt.Equals, 1,
				qt.Commentf("FK removal must be dropped exactly once (deduped) with FK syntax; got:\n%s", sql))
			c.Assert(strings.Count(sql, "ALTER TABLE things DROP CONSTRAINT chk_qty;"), qt.Equals, 1,
				qt.Commentf("CHECK removal must be dropped exactly once; got:\n%s", sql))
			c.Assert(strings.Count(sql, "ALTER TABLE legacy DROP PRIMARY KEY;"), qt.Equals, 1,
				qt.Commentf("PRIMARY KEY removal must use the dedicated MySQL-family spelling; got:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT chk_on_obsolete",
				qt.Commentf("constraints on dropped tables are cascaded by DROP TABLE, not dropped explicitly; got:\n%s", sql))
			c.Assert(sql, qt.Contains, "DROP TABLE IF EXISTS obsolete",
				qt.Commentf("the dropped table itself is still removed; got:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "chk_orphan",
				qt.Commentf("a removal entry with no recorded host must be skipped silently; got:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "ALTER TABLE  DROP",
				qt.Commentf("no malformed empty-table ALTER may be emitted; got:\n%s", sql))
		})
	}
}

func TestPlanner_GenerateMigrationAST_TableQualifiedPrimaryKeyAddition(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName: "memberships",
					ColumnsModified: []difftypes.ColumnDiff{
						{ColumnName: "org_id", Changes: map[string]string{"primary_key": "false -> true"}},
						{ColumnName: "user_id", Changes: map[string]string{"primary_key": "false -> true"}},
					},
				}},
				ConstraintsAdded: []string{"PRIMARY"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
					Name:      "PRIMARY",
					TableName: "memberships",
					Type:      "PRIMARY KEY",
					Columns:   []string{"org_id", "user_id"},
				}},
			}

			sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})
			c.Assert(sql, qt.Contains, "ALTER TABLE memberships ADD PRIMARY KEY (org_id, user_id);")
			c.Assert(sql, qt.Not(qt.Contains), "MODIFY COLUMN org_id")
			c.Assert(sql, qt.Not(qt.Contains), "MODIFY COLUMN user_id")
		})
	}
}

func TestPlanner_GenerateMigrationAST_TableQualifiedPrimaryKeyRemovalSuppressesColumnModify(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName: "memberships",
					ColumnsModified: []difftypes.ColumnDiff{
						{ColumnName: "org_id", Changes: map[string]string{"primary_key": "true -> false"}},
						{ColumnName: "user_id", Changes: map[string]string{"primary_key": "true -> false"}},
					},
				}},
				ConstraintsRemoved: []string{"PRIMARY"},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
					Name:      "PRIMARY",
					TableName: "memberships",
					Type:      "PRIMARY KEY",
				}},
			}

			sql := renderMySQLFamily(c, dialect, diff, &schemamodel.Database{})
			c.Assert(sql, qt.Contains, "ALTER TABLE memberships DROP PRIMARY KEY;")
			c.Assert(sql, qt.Not(qt.Contains), "MODIFY COLUMN org_id")
			c.Assert(sql, qt.Not(qt.Contains), "MODIFY COLUMN user_id")
		})
	}
}

// withDeclaredObjects fills a fixture diff's carries from the declaration the
// plan is applied against: each column modification's operand, and the declared
// table list a constraint synthesis indexes by struct name.
//
// Both are things a comparison fills on every run, and a fixture states the
// CHANGE.
//
// It resolves the way the comparison does: the table's struct, then that
// struct's fields with embedded structs folded in. A column desired does not
// declare is left with no operand, so a fixture testing that case still reaches
// the planner's report.
func withDeclaredObjects(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
) *difftypes.SchemaDiff {
	diff = withConstraintRecords(diff, desired)
	completed := *diff
	completed.TablesModified = make([]difftypes.TableDiff, len(diff.TablesModified))
	copy(completed.TablesModified, diff.TablesModified)
	for tableIndex, tableDiff := range completed.TablesModified {
		columns := make([]difftypes.ColumnDiff, len(tableDiff.ColumnsModified))
		copy(columns, tableDiff.ColumnsModified)
		for columnIndex, colDiff := range columns {
			if colDiff.Desired.Name != "" {
				continue
			}
			columns[columnIndex].Desired = declaredColumn(desired, tableDiff.TableName, colDiff.ColumnName)
		}
		completed.TablesModified[tableIndex].ColumnsModified = columns
	}
	if len(completed.DeclaredTables) == 0 {
		completed.DeclaredTables = desired.Tables
	}
	if len(completed.DeclaredIndexes) == 0 {
		completed.DeclaredIndexes = difftypes.IndexDeclarationsOf(desired)
	}
	if len(completed.DeclaredTableDependencies) == 0 {
		completed.DeclaredTableDependencies = deporder.GeneratedTableDependencies(desired)
	}
	if len(completed.DeclaredTableDependencies) == 0 {
		completed.DeclaredTableDependencies = deporder.GeneratedTableDependencies(desired)
	}
	if len(completed.DeclaredForeignKeys) == 0 {
		completed.DeclaredForeignKeys = difftypes.ForeignKeyDeclarationsOf(desired)
	}
	return &completed
}

func declaredColumn(desired *schemamodel.Database, tableName, columnName string) schemamodel.Field {
	for _, table := range desired.Tables {
		if table.Name != tableName && table.QualifiedName() != tableName {
			continue
		}
		for _, field := range fromschema.ProcessEmbeddedFields(desired.EmbeddedFields, desired.Fields) {
			if field.StructName == table.StructName && field.Name == columnName {
				return field
			}
		}
	}
	return schemamodel.Field{}
}

// withConstraintRecords fills a fixture diff's constraint additions from the
// declaration, the way a comparison does.
//
// A comparison describes every constraint it adds: it resolves the host table,
// folds in the ones synthesized from a field, and carries the body. A fixture
// that states only names is standing in for that, so this does the same
// resolution once rather than each fixture spelling the record out
// (stokaro/ptah#2315).
//
// A name the declaration does not describe is left without a record, which is
// what a test about a diff naming something undeclared needs.
func withConstraintRecords(diff *difftypes.SchemaDiff, desired *schemamodel.Database) *difftypes.SchemaDiff {
	if diff == nil || desired == nil || len(diff.ConstraintsAdded) == 0 {
		return diff
	}
	completed := *diff
	records := append([]difftypes.ConstraintAdditionInfo(nil), diff.ConstraintsAddedWithTables...)
	described := make(map[string]bool, len(records))
	for _, record := range records {
		described[record.Name] = true
	}
	for _, name := range diff.ConstraintsAdded {
		if described[name] {
			continue
		}
		declared, ok := declaredConstraintNamed(desired, name)
		if !ok {
			declared, ok = synthesizedFieldCheck(desired, name)
		}
		if !ok {
			continue
		}
		described[name] = true
		records = append(records, difftypes.ConstraintAdditionInfo{
			Name:            declared.Name,
			TableName:       constraintHostTable(desired, declared),
			Type:            declared.Type,
			Columns:         append([]string(nil), declared.Columns...),
			IncludeColumns:  append([]string(nil), declared.IncludeColumns...),
			CheckExpression: declared.CheckExpression,
			ForeignTable:    declared.ForeignTable,
			ForeignColumn:   declared.ForeignColumn,
			ForeignColumns:  append([]string(nil), declared.ForeignColumns...),
			OnDelete:        declared.OnDelete,
			OnUpdate:        declared.OnUpdate,
			Deferrable:      declared.Deferrable,
			Initially:       declared.Initially,
			UsingMethod:     declared.UsingMethod,
			ExcludeElements: declared.ExcludeElements,
			WhereCondition:  declared.WhereCondition,
		})
	}
	completed.ConstraintsAddedWithTables = records
	return &completed
}

func declaredConstraintNamed(desired *schemamodel.Database, name string) (schemamodel.Constraint, bool) {
	for _, constraint := range desired.Constraints {
		if constraint.Name == name {
			return constraint, true
		}
	}
	return schemamodel.Constraint{}, false
}

// constraintHostTable resolves the table a declared constraint belongs to: the
// one it names, or the one its struct declares.
func constraintHostTable(desired *schemamodel.Database, constraint schemamodel.Constraint) string {
	if constraint.Table != "" {
		return constraint.Table
	}
	for _, table := range desired.Tables {
		if table.StructName == constraint.StructName {
			return table.QualifiedName()
		}
	}
	return ""
}

// synthesizedFieldCheck rebuilds the constraint a comparison synthesizes from a
// field's `check=`.
//
// The comparison folds these into the same map as the declared ones before it
// compares, so they reach a diff as ordinary records. A fixture that names one
// is standing in for that.
func synthesizedFieldCheck(desired *schemamodel.Database, name string) (schemamodel.Constraint, bool) {
	for _, field := range desired.Fields {
		if field.Check == "" {
			continue
		}
		table := ""
		for _, candidate := range desired.Tables {
			if candidate.StructName == field.StructName {
				table = candidate.QualifiedName()
			}
		}
		synthesized := field.CheckName
		if synthesized == "" {
			synthesized = table + "_" + field.Name + "_check"
		}
		if synthesized != name {
			continue
		}
		return schemamodel.Constraint{
			StructName:      field.StructName,
			Name:            synthesized,
			Type:            "CHECK",
			Table:           table,
			CheckExpression: field.Check,
		}, true
	}
	return schemamodel.Constraint{}, false
}

// TestPlanner_ModifiedPrimaryKeyIsDroppedThenReadded covers the one addition
// kind whose pass emitted nothing for a modification.
//
// A PRIMARY KEY change reaches the planner as a removal and an addition sharing
// one (table, name). The pass that owns primary keys skipped such a record
// outright, and the re-ADD arrived from the name-resolving route instead — so
// withdrawing that route left the drop with nothing after it, and the name it
// never marked as handled reached the refusal for a constraint the diff does
// not describe. It describes this one completely.
//
// The PostgreSQL planner already emitted the pair from the record
// (stokaro/ptah#2199); this is the same shape, and the SQL is byte-identical to
// what the withdrawn route produced.
func TestPlanner_ModifiedPrimaryKeyIsDroppedThenReadded(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded:   []string{"pk_users"},
		ConstraintsRemoved: []string{"pk_users"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name: "pk_users", TableName: "users", Type: "PRIMARY KEY",
			Columns: []string{"id", "tenant"},
		}},
		ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
			Name: "pk_users", TableName: "users", Type: "PRIMARY KEY",
		}},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "User", Name: "pk_users", Type: "PRIMARY KEY",
			Table: "users", Columns: []string{"id", "tenant"},
		}},
	}

	nodes, err := mysql.New().GenerateMigrationAST(diff, desired)

	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	drop := strings.Index(sql, "ALTER TABLE `users` DROP PRIMARY KEY;")
	add := strings.Index(sql, "ALTER TABLE `users` ADD PRIMARY KEY (`id`, `tenant`);")
	c.Assert(drop >= 0, qt.IsTrue)
	c.Assert(add >= 0, qt.IsTrue)
	// The order is the property: MySQL 9.7.1 answers
	// `ERROR 1068 (42000): Multiple primary key defined` to an ADD that
	// precedes the DROP of the key it replaces.
	c.Assert(drop < add, qt.IsTrue)
	c.Assert(strings.Count(sql, "DROP PRIMARY KEY"), qt.Equals, 1)
}
