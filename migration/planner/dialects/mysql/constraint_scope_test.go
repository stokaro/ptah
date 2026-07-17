package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/schemadiff/types"
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
func renderMySQLFamily(c *qt.C, dialect string, diff *types.SchemaDiff, generated *goschema.Database) string {
	nodes := mysql.New().GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL(dialect, nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

func TestPlanner_GenerateMigrationAST_CompositeForeignKeyAddition(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &types.SchemaDiff{
				ConstraintsAdded: []string{"fk_orders_accounts"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
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

			sql := renderMySQLFamily(c, dialect, diff, &goschema.Database{})

			c.Assert(sql, qt.Contains, "ALTER TABLE orders ADD CONSTRAINT fk_orders_accounts FOREIGN KEY (tenant_id, owner_id) REFERENCES accounts(tenant_id, id) ON DELETE CASCADE;",
				qt.Commentf("composite FK addition must preserve all referenced columns; got:\n%s", sql))
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
		orderings := map[string][]types.ConstraintRemovalInfo{
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

					diff := &types.SchemaDiff{
						ConstraintsAdded:   []string{"shared_fk"},
						ConstraintsRemoved: []string{"shared_fk"},
						ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
							{
								Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY",
								Columns: []string{"author_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "CASCADE",
							},
						},
						ConstraintsRemovedWithTables: removals,
					}

					sql := renderMySQLFamily(c, dialect, diff, &goschema.Database{})

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
		orderings := map[string][]types.ConstraintRemovalInfo{
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

					diff := &types.SchemaDiff{
						ConstraintsAdded:   []string{"shared_check"},
						ConstraintsRemoved: []string{"shared_check"},
						ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
							{Name: "shared_check", TableName: "articles", Type: "CHECK"},
						},
						ConstraintsRemovedWithTables: removals,
					}
					generated := &goschema.Database{
						Constraints: []goschema.Constraint{
							{StructName: "Article", Name: "shared_check", Type: "CHECK", Table: "articles", CheckExpression: "status IN ('draft', 'published')"},
						},
					}

					sql := renderMySQLFamily(c, dialect, diff, generated)

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

				diff := &types.SchemaDiff{
					ConstraintsAdded:   []string{"fk_customer"},
					ConstraintsRemoved: []string{"fk_customer"},
					ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
						{
							Name: "fk_customer", TableName: "orders", Type: "FOREIGN KEY",
							Columns: []string{"customer_id"}, ForeignTable: "customers", ForeignColumn: "id", OnDelete: "CASCADE",
						},
						{
							Name: "fk_customer", TableName: "invoices", Type: "FOREIGN KEY",
							Columns: []string{"customer_id"}, ForeignTable: "customers", ForeignColumn: "id", OnDelete: "SET NULL",
						},
					},
					ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
						{Name: "fk_customer", TableName: "orders", Type: "FOREIGN KEY"},
						{Name: "fk_customer", TableName: "invoices", Type: "FOREIGN KEY"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, &goschema.Database{})

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

				diff := &types.SchemaDiff{
					ConstraintsAdded:   []string{"fk_post_owner"},
					ConstraintsRemoved: []string{"fk_post_owner"},
					ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
						{
							Name: "fk_post_owner", TableName: "posts", Type: "FOREIGN KEY",
							Columns: []string{"owner_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "SET NULL",
						},
					},
					ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
						{Name: "fk_post_owner", TableName: "posts", Type: "FOREIGN KEY"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, &goschema.Database{})

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

				diff := &types.SchemaDiff{
					ConstraintsAdded:   []string{"chk_down"},
					ConstraintsRemoved: []string{"chk_down"},
					ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
						{Name: "chk_down", TableName: "things", Type: "CHECK"},
					},
				}
				generated := &goschema.Database{
					Constraints: []goschema.Constraint{
						{StructName: "Thing", Name: "chk_down", Type: "CHECK", Table: "things", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, generated)

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
		// The down migration of a multi-host non-FK constraint modify arrives
		// exactly in this shape: reverseSchemaDiffWithSchema copies the bare
		// name into ConstraintsAdded once per host (duplicated), fills
		// ConstraintsRemovedWithTables with EVERY host, and leaves
		// ConstraintsAddedWithTables empty (reverseConstraintAdditions
		// restores FOREIGN KEYs only). emitModifyDropForName must then drop
		// every recorded host exactly once — deduped across the duplicated
		// bare names — and removeConstraints must emit nothing (hostless
		// re-add rule). A regression that drops only the first host would
		// leave the second host's stale constraint in place with the whole
		// suite green, which is why the per-host counts below are load-bearing.
		//
		// The ADD side is deliberately NOT count-asserted: the multi-host
		// non-FK re-add resolves by name to a single definition (a
		// pre-existing limitation explicitly deferred in issue #207 notes).
		for _, dialect := range mysqlFamilyDialects {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)

				diff := &types.SchemaDiff{
					ConstraintsAdded:   []string{"shared_check", "shared_check"},
					ConstraintsRemoved: []string{"shared_check", "shared_check"},
					ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
						{Name: "shared_check", TableName: "articles", Type: "CHECK"},
						{Name: "shared_check", TableName: "pages", Type: "CHECK"},
					},
				}
				generated := &goschema.Database{
					Constraints: []goschema.Constraint{
						{StructName: "Article", Name: "shared_check", Type: "CHECK", Table: "articles", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, generated)

				c.Assert(strings.Count(sql, "ALTER TABLE articles DROP CONSTRAINT shared_check;"), qt.Equals, 1,
					qt.Commentf("first removal host must be dropped exactly once; got:\n%s", sql))
				c.Assert(strings.Count(sql, "ALTER TABLE pages DROP CONSTRAINT shared_check;"), qt.Equals, 1,
					qt.Commentf("second removal host must be dropped exactly once, not skipped and not doubled; got:\n%s", sql))
				c.Assert(strings.Count(sql, "DROP CONSTRAINT shared_check;"), qt.Equals, 2,
					qt.Commentf("exactly one drop per recorded host across BOTH planner phases; got:\n%s", sql))
				c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS")

				articlesDrop := strings.Index(sql, "ALTER TABLE articles DROP CONSTRAINT shared_check")
				pagesDrop := strings.Index(sql, "ALTER TABLE pages DROP CONSTRAINT shared_check")
				firstAdd := strings.Index(sql, "ADD CONSTRAINT shared_check")
				c.Assert(articlesDrop >= 0 && pagesDrop >= 0 && firstAdd >= 0, qt.IsTrue)
				c.Assert(articlesDrop < firstAdd && pagesDrop < firstAdd, qt.IsTrue,
					qt.Commentf("both host drops are owned by the add side and must precede the re-add; got:\n%s", sql))
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

				diff := &types.SchemaDiff{
					ConstraintsAdded:   []string{"chk_hostless"},
					ConstraintsRemoved: []string{"chk_hostless"},
					ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
						{Name: "chk_hostless", TableName: "", Type: "CHECK"},
					},
				}
				generated := &goschema.Database{
					Constraints: []goschema.Constraint{
						{StructName: "Thing", Name: "chk_hostless", Type: "CHECK", Table: "things", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, generated)

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

				diff := &types.SchemaDiff{
					ConstraintsAdded:   []string{"chk_ghost"},
					ConstraintsRemoved: []string{"chk_ghost"},
					ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
						{Name: "chk_ghost", TableName: "", Type: "CHECK"},
					},
					ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
						{Name: "chk_ghost", TableName: "things", Type: "CHECK"},
					},
				}
				generated := &goschema.Database{
					Constraints: []goschema.Constraint{
						{StructName: "Thing", Name: "chk_ghost", Type: "CHECK", Table: "things", CheckExpression: "qty >= 0"},
					},
				}

				sql := renderMySQLFamily(c, dialect, diff, generated)

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

				diff := &types.SchemaDiff{
					ConstraintsAdded:   []string{"fk_post_owner"},
					ConstraintsRemoved: []string{"fk_post_owner"},
					ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
						{Name: "fk_post_owner", TableName: "posts", Type: "FOREIGN KEY"},
					},
				}
				generated := &goschema.Database{
					Tables: []goschema.Table{
						{StructName: "User", Name: "users"},
						{StructName: "Post", Name: "posts"},
					},
					Fields: []goschema.Field{
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

				sql := renderMySQLFamily(c, dialect, diff, generated)

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
// once with the type-correct syntax; PRIMARY KEY removals and constraints on
// tables that are themselves being dropped are skipped; a duplicate removal
// entry for the same (table, name) is deduped — MySQL would abort on the
// second, unguarded drop otherwise.
func TestPlanner_GenerateMigrationAST_PureConstraintRemovals_TableQualified(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := &types.SchemaDiff{
				TablesRemoved: []string{"obsolete"},
				ConstraintsRemoved: []string{
					"fk_orders_customer", "chk_qty", "pk_legacy", "chk_on_obsolete", "fk_orders_customer", "chk_orphan",
				},
				ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
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

			sql := renderMySQLFamily(c, dialect, diff, &goschema.Database{})

			c.Assert(strings.Count(sql, "ALTER TABLE orders DROP FOREIGN KEY fk_orders_customer;"), qt.Equals, 1,
				qt.Commentf("FK removal must be dropped exactly once (deduped) with FK syntax; got:\n%s", sql))
			c.Assert(strings.Count(sql, "ALTER TABLE things DROP CONSTRAINT chk_qty;"), qt.Equals, 1,
				qt.Commentf("CHECK removal must be dropped exactly once; got:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "pk_legacy",
				qt.Commentf("PRIMARY KEY removals must be skipped; got:\n%s", sql))
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
