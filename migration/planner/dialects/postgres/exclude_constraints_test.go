package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_CompositeForeignKeyAddition(t *testing.T) {
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

	nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, "ALTER TABLE orders ADD CONSTRAINT fk_orders_accounts FOREIGN KEY (tenant_id, owner_id) REFERENCES accounts(tenant_id, id) ON DELETE CASCADE;",
		qt.Commentf("composite FK addition must preserve all referenced columns; got:\n%s", sql))
}

func TestPlanner_GenerateMigrationAST_ConstraintsAdded(t *testing.T) {
	tests := []struct {
		name        string
		diff        *types.SchemaDiff
		generated   *goschema.Database
		expectedSQL []string
	}{
		{
			name: "EXCLUDE constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Booking",
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, during WITH &&",
						WhereCondition:  "is_active = true",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true);",
			},
		},
		{
			name: "EXCLUDE constraint without WHERE clause",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"unique_locations"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Location",
						Name:            "unique_locations",
						Type:            "EXCLUDE",
						Table:           "locations",
						UsingMethod:     "gist",
						ExcludeElements: "location WITH &&",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE locations ADD CONSTRAINT unique_locations EXCLUDE USING gist (location WITH &&);",
			},
		},
		{
			name: "CHECK constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"positive_price"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price > 0",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
			},
		},
		{
			name: "UNIQUE constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"unique_user_email"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName: "User",
						Name:       "unique_user_email",
						Type:       "UNIQUE",
						Table:      "users",
						Columns:    []string{"user_id", "email"},
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE users ADD CONSTRAINT unique_user_email UNIQUE (user_id, email);",
			},
		},
		{
			name: "FOREIGN KEY constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"fk_user"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:    "Order",
						Name:          "fk_user",
						Type:          "FOREIGN KEY",
						Table:         "orders",
						Columns:       []string{"user_id"},
						ForeignTable:  "users",
						ForeignColumn: "id",
						OnDelete:      "CASCADE",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;",
			},
		},
		{
			name: "Multiple constraints added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings", "positive_price"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Booking",
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, during WITH &&",
					},
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price > 0",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&);",
				"ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := postgres.New()
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			// Convert AST nodes to SQL for verification
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)

			// Remove header comments and split into statements
			lines := strings.Split(sql, "\n")
			var sqlStatements []string
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "--") {
					sqlStatements = append(sqlStatements, line)
				}
			}

			c.Assert(sqlStatements, qt.HasLen, len(tt.expectedSQL))
			for i, expected := range tt.expectedSQL {
				c.Assert(sqlStatements[i], qt.Equals, expected)
			}
		})
	}
}

// TestPlanner_GenerateMigrationAST_ModifiedFK_ScopesDropToHostTable guards the
// issue #199 fix: a MODIFIED field-level FK (dropped + re-added) must have its
// DROP scoped to the concrete host table that the comparator recorded, never the
// name-only information_schema DO block. PostgreSQL constraint names are unique
// per table, so when the same name exists on two tables and only one is modified,
// the name-only `... WHERE constraint_name = X ... LIMIT 1` lookup could resolve
// and drop the constraint on the WRONG table. The planner already knows the host
// (ConstraintAdditionInfo.TableName), so it emits a direct table-qualified drop.
func TestPlanner_GenerateMigrationAST_ModifiedFK_ScopesDropToHostTable(t *testing.T) {
	t.Run("same constraint name on two tables, only one modified, drops only the intended host", func(t *testing.T) {
		c := qt.New(t)

		// Both `orders` and `invoices` carry an FK literally named `fk_customer`
		// (e.g. via an embedded inline-relation mixin). Only the one on `orders`
		// drifted (its on_delete changed), so the comparator reports a modify for
		// orders alone: orders appears in BOTH the additions and removals, while
		// invoices appears nowhere.
		diff := &types.SchemaDiff{
			ConstraintsAdded:   []string{"fk_customer"},
			ConstraintsRemoved: []string{"fk_customer"},
			ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
				{
					Name:          "fk_customer",
					TableName:     "orders",
					Type:          "FOREIGN KEY",
					Columns:       []string{"customer_id"},
					ForeignTable:  "customers",
					ForeignColumn: "id",
					OnDelete:      "CASCADE",
				},
			},
			ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
				{Name: "fk_customer", TableName: "orders", Type: "FOREIGN KEY"},
			},
		}

		nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		// The DROP is scoped to the intended host with a direct ALTER TABLE.
		c.Assert(sql, qt.Contains, "ALTER TABLE orders DROP CONSTRAINT IF EXISTS fk_customer;",
			qt.Commentf("modified FK must be dropped from its known host table; got:\n%s", sql))

		// The unsafe name-only DO block resolution must NOT be used for a
		// known-host modify — that is what could hit the wrong table.
		c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
			qt.Commentf("known-host modify must not use the name-only DO block; got:\n%s", sql))

		// The unrelated host (invoices) must be left completely alone: no drop,
		// no add.
		c.Assert(sql, qt.Not(qt.Contains), "invoices",
			qt.Commentf("the unmodified host with the same constraint name must not be touched; got:\n%s", sql))

		// The re-ADD targets the same host, and the DROP precedes it.
		c.Assert(sql, qt.Contains, "ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE;",
			qt.Commentf("modified FK must be re-added on its host; got:\n%s", sql))
		dropIdx := strings.Index(sql, "DROP CONSTRAINT IF EXISTS fk_customer")
		addIdx := strings.Index(sql, "ADD CONSTRAINT fk_customer")
		c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
			qt.Commentf("DROP must precede the re-ADD; drop=%d add=%d sql:\n%s", dropIdx, addIdx, sql))
	})

	t.Run("both hosts modified each get their own table-qualified drop and add", func(t *testing.T) {
		c := qt.New(t)

		// Both hosts drifted: each must be dropped from its own table and
		// re-added with its own (distinct) action. Neither may rely on a
		// name-only resolution that can only reach one table.
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

		nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Count(sql, "ALTER TABLE orders DROP CONSTRAINT IF EXISTS fk_customer;"), qt.Equals, 1,
			qt.Commentf("orders host dropped exactly once; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ALTER TABLE invoices DROP CONSTRAINT IF EXISTS fk_customer;"), qt.Equals, 1,
			qt.Commentf("invoices host dropped exactly once; got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
			qt.Commentf("multi-host modify must not use the name-only DO block; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "ON DELETE CASCADE")
		c.Assert(sql, qt.Contains, "ON DELETE SET NULL")
	})
}

// TestPlanner_GenerateMigrationAST_ModifiedNonFKConstraint_ScopesDropToHostTable
// extends the issue #199 fix to the NON-FK modify path. A modified table-level
// UNIQUE / CHECK constraint is reached via the bare ConstraintsAdded loop (FK
// modifies go through the ConstraintsAddedWithTables loop instead), which used
// to emit the name-only information_schema DO block for the DROP. Because the
// comparator records the host in ConstraintsRemovedWithTables in lockstep, the
// planner now scopes that DROP to the concrete host table — so a constraint name
// reused across two tables can no longer be dropped from the wrong one.
func TestPlanner_GenerateMigrationAST_ModifiedNonFKConstraint_ScopesDropToHostTable(t *testing.T) {
	t.Run("UNIQUE constraint name reused on two tables, only one modified, drops only that host", func(t *testing.T) {
		c := qt.New(t)

		// Both `articles` and `pages` carry a UNIQUE constraint literally named
		// `uq_slug`. Only the one on `articles` drifted (a column was added), so
		// the comparator reports a modify for articles alone.
		diff := &types.SchemaDiff{
			ConstraintsAdded:   []string{"uq_slug"},
			ConstraintsRemoved: []string{"uq_slug"},
			ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
				{Name: "uq_slug", TableName: "articles", Type: "UNIQUE", Columns: []string{"slug", "locale"}},
			},
			ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
				{Name: "uq_slug", TableName: "articles", Type: "UNIQUE"},
			},
		}
		generated := &goschema.Database{
			Constraints: []goschema.Constraint{
				{StructName: "Article", Name: "uq_slug", Type: "UNIQUE", Table: "articles", Columns: []string{"slug", "locale"}},
			},
		}

		nodes := postgres.New().GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		// The DROP is scoped to the intended host with a direct ALTER TABLE.
		c.Assert(sql, qt.Contains, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS uq_slug;",
			qt.Commentf("modified non-FK constraint must be dropped from its known host table; got:\n%s", sql))
		// The unsafe name-only DO block must NOT be used when the host is known.
		c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
			qt.Commentf("known-host non-FK modify must not use the name-only DO block; got:\n%s", sql))
		// The unrelated host (pages) with the same constraint name is untouched.
		c.Assert(sql, qt.Not(qt.Contains), "pages",
			qt.Commentf("the unmodified host with the same constraint name must not be touched; got:\n%s", sql))
		// The re-ADD targets the same host, and the DROP precedes it.
		c.Assert(sql, qt.Contains, "ALTER TABLE articles ADD CONSTRAINT uq_slug UNIQUE (slug, locale);",
			qt.Commentf("modified constraint must be re-added on its host; got:\n%s", sql))
		dropIdx := strings.Index(sql, "DROP CONSTRAINT IF EXISTS uq_slug")
		addIdx := strings.Index(sql, "ADD CONSTRAINT uq_slug")
		c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
			qt.Commentf("DROP must precede the re-ADD; drop=%d add=%d sql:\n%s", dropIdx, addIdx, sql))
	})

	t.Run("synthetic diff without a recorded host falls back to the name-only DO block", func(t *testing.T) {
		c := qt.New(t)

		// Defensive fallback: a hand-built diff that names a modified constraint
		// but carries no ConstraintsRemovedWithTables entry has genuinely no host
		// to scope by, so the runtime information_schema DO block remains the
		// only option. The real comparator never produces this shape (it fills
		// the WithTables lists in lockstep), but the planner must not panic or
		// silently drop the work.
		diff := &types.SchemaDiff{
			ConstraintsAdded:   []string{"legacy_check"},
			ConstraintsRemoved: []string{"legacy_check"},
		}
		generated := &goschema.Database{
			Constraints: []goschema.Constraint{
				{StructName: "Thing", Name: "legacy_check", Type: "CHECK", Table: "things", CheckExpression: "x > 0"},
			},
		}

		nodes := postgres.New().GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(sql, qt.Contains, "information_schema.table_constraints",
			qt.Commentf("with no recorded host the planner must fall back to the DO block; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "constraint_name = 'legacy_check'",
			qt.Commentf("fallback DO block must target the constraint by name; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "ALTER TABLE things ADD CONSTRAINT legacy_check CHECK (x > 0);",
			qt.Commentf("modified constraint must still be re-added; got:\n%s", sql))
		dropIdx := strings.Index(sql, "DO $ptah$")
		addIdx := strings.Index(sql, "ADD CONSTRAINT legacy_check")
		c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
			qt.Commentf("DROP must precede the re-ADD; drop=%d add=%d sql:\n%s", dropIdx, addIdx, sql))
	})
}

// TestPlanner_GenerateMigrationAST_SharedConstraintName_ModifiedOnOneTablePurelyRemovedOnAnother
// guards issue #206. A single constraint name is shared across two tables: it is
// MODIFIED on table A (so the name also lands in ConstraintsAdded) and PURELY
// REMOVED on table B (B has no addition). The buggy implementation keyed the
// modify-skip on the bare name, so B's removal was treated as a modify owned by
// addNewConstraints and skipped — leaving the stale constraint on B. The fix
// keys the skip on (table, name), so B is dropped table-qualified, A is dropped
// and re-added, and no name-only DO block is used.
//
// The bug only reproduces for the FOREIGN KEY shape: an FK modify on A is dropped
// by the ConstraintsAddedWithTables loop (which never touches B), so a buggy
// bare-name skip in removeConstraints drops B zero times. The CHECK shape is a
// weaker guard — the old add-side modify-drop already dropped B as a phantom
// pre-drop, so the bug is masked there except for statement ordering. We cover
// both: the FK subtest is the load-bearing #206 regression guard.
func TestPlanner_GenerateMigrationAST_SharedConstraintName_ModifiedOnOneTablePurelyRemovedOnAnother(t *testing.T) {
	t.Run("foreign key (the load-bearing #206 guard)", func(t *testing.T) {
		c := qt.New(t)

		// `shared_fk` exists on both `articles` and `pages`. It drifted on
		// `articles` (a modify: present in both additions and removals) and was
		// removed outright on `pages` (removal only). The FK modify on articles is
		// owned by the ConstraintsAddedWithTables loop, so removeConstraints alone
		// is responsible for dropping pages — which the bare-name skip got wrong.
		diff := &types.SchemaDiff{
			ConstraintsAdded:   []string{"shared_fk"},
			ConstraintsRemoved: []string{"shared_fk"},
			ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
				{Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY", Columns: []string{"author_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "CASCADE"},
			},
			ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
				{Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY"},
				{Name: "shared_fk", TableName: "pages", Type: "FOREIGN KEY"},
			},
		}

		nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		// The pure removal on B (pages) is dropped table-qualified and NOT skipped.
		// This is the assertion that fails against the pre-fix bare-name skip.
		c.Assert(sql, qt.Contains, "ALTER TABLE pages DROP CONSTRAINT IF EXISTS shared_fk;",
			qt.Commentf("purely-removed host must be dropped table-qualified, not skipped; got:\n%s", sql))
		// The modify on A (articles) is dropped table-qualified and re-added once.
		c.Assert(sql, qt.Contains, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS shared_fk;",
			qt.Commentf("modified host must be dropped from its known table; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "ALTER TABLE articles ADD CONSTRAINT shared_fk FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE;",
			qt.Commentf("modified FK must be re-added on its host; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ADD CONSTRAINT shared_fk"), qt.Equals, 1,
			qt.Commentf("only the modified host may be re-added; got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
			qt.Commentf("must not use the name-only DO block when hosts are known; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS shared_fk;"), qt.Equals, 1)
		c.Assert(strings.Count(sql, "ALTER TABLE pages DROP CONSTRAINT IF EXISTS shared_fk;"), qt.Equals, 1)

		dropIdx := strings.Index(sql, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS shared_fk")
		addIdx := strings.Index(sql, "ALTER TABLE articles ADD CONSTRAINT shared_fk")
		c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
			qt.Commentf("the modified host's drop must precede its re-add; drop=%d add=%d; got:\n%s", dropIdx, addIdx, sql))
	})

	t.Run("check constraint", func(t *testing.T) {
		c := qt.New(t)

		// Same mixed shape with a CHECK. The non-FK modify routes through the bare
		// ConstraintsAdded loop / emitModifyDropForName, which now drops ONLY the
		// re-added host (articles); pages is left to removeConstraints. The
		// discriminating assertion is the ORDER: the fix emits the pages-drop AFTER
		// the articles re-add (removeConstraints runs later), whereas the pre-fix
		// add-side phantom pre-drop emitted pages BEFORE the re-add.
		diff := &types.SchemaDiff{
			ConstraintsAdded:   []string{"shared_check"},
			ConstraintsRemoved: []string{"shared_check"},
			ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
				{Name: "shared_check", TableName: "articles", Type: "CHECK"},
			},
			ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
				{Name: "shared_check", TableName: "articles", Type: "CHECK"},
				{Name: "shared_check", TableName: "pages", Type: "CHECK"},
			},
		}
		generated := &goschema.Database{
			Constraints: []goschema.Constraint{
				{StructName: "Article", Name: "shared_check", Type: "CHECK", Table: "articles", CheckExpression: "status IN ('draft', 'published')"},
			},
		}

		nodes := postgres.New().GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(sql, qt.Contains, "ALTER TABLE pages DROP CONSTRAINT IF EXISTS shared_check;",
			qt.Commentf("purely-removed host must be dropped table-qualified; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS shared_check;",
			qt.Commentf("modified host must be dropped from its known table; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "ALTER TABLE articles ADD CONSTRAINT shared_check CHECK (status IN ('draft', 'published'));",
			qt.Commentf("modified constraint must be re-added on its host; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ADD CONSTRAINT shared_check"), qt.Equals, 1,
			qt.Commentf("only the modified host may be re-added; got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
			qt.Commentf("must not use the name-only DO block when hosts are known; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ALTER TABLE pages DROP CONSTRAINT IF EXISTS shared_check;"), qt.Equals, 1)

		articlesDropIdx := strings.Index(sql, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS shared_check")
		articlesAddIdx := strings.Index(sql, "ALTER TABLE articles ADD CONSTRAINT shared_check")
		pagesDropIdx := strings.Index(sql, "ALTER TABLE pages DROP CONSTRAINT IF EXISTS shared_check")
		c.Assert(articlesDropIdx >= 0 && articlesAddIdx >= 0 && pagesDropIdx >= 0, qt.IsTrue)
		c.Assert(articlesDropIdx < articlesAddIdx, qt.IsTrue,
			qt.Commentf("modified host drop must precede its re-add; got:\n%s", sql))
		// The pure-removal host's drop is owned by removeConstraints and lands after
		// the add-side re-add — the pre-fix add-side phantom pre-drop placed it before.
		c.Assert(pagesDropIdx > articlesAddIdx, qt.IsTrue,
			qt.Commentf("pure-removal drop must come from removeConstraints (after the re-add), not the add-side; got:\n%s", sql))
	})
}

// TestPlanner_GenerateMigrationAST_ModifyDrop_ScopesToHostWhenAddedHostsAbsent
// guards the reverse/down direction. reverseConstraintAdditions only restores
// FOREIGN KEY additions (and nothing when the schema context is absent), so a
// non-FK modify in a down migration carries ConstraintsRemovedWithTables but an
// EMPTY ConstraintsAddedWithTables. emitModifyDropForName must still scope the
// pre-drop to the recorded removal host — NOT fall back to the name-only
// information_schema DO block, which would regress the table-scoped drop #205
// already emitted and contradict #206's own acceptance criterion.
func TestPlanner_GenerateMigrationAST_ModifyDrop_ScopesToHostWhenAddedHostsAbsent(t *testing.T) {
	c := qt.New(t)

	// A non-FK modify shaped like a reverse/down diff: the removal host is known
	// (ConstraintsRemovedWithTables) but ConstraintsAddedWithTables is empty.
	diff := &types.SchemaDiff{
		ConstraintsAdded:           []string{"chk_down"},
		ConstraintsRemoved:         []string{"chk_down"},
		ConstraintsAddedWithTables: nil,
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "chk_down", TableName: "things", Type: "CHECK"},
		},
	}
	generated := &goschema.Database{
		Constraints: []goschema.Constraint{
			{StructName: "Thing", Name: "chk_down", Type: "CHECK", Table: "things", CheckExpression: "qty >= 0"},
		},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	// Table-scoped drop, not the name-only DO block.
	c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_down;",
		qt.Commentf("modify drop must be scoped to the known removal host even with no addedHosts; got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
		qt.Commentf("must not regress to the name-only DO block when the removal host is known; got:\n%s", sql))
	c.Assert(sql, qt.Contains, "ALTER TABLE things ADD CONSTRAINT chk_down CHECK (qty >= 0);",
		qt.Commentf("modified constraint must be re-added; got:\n%s", sql))
	dropIdx := strings.Index(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_down")
	addIdx := strings.Index(sql, "ALTER TABLE things ADD CONSTRAINT chk_down")
	c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
		qt.Commentf("DROP must precede the re-ADD; drop=%d add=%d; got:\n%s", dropIdx, addIdx, sql))
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"old_constraint"},
	}
	generated := &goschema.Database{}

	pl := postgres.New()
	nodes := pl.GenerateMigrationAST(diff, generated)

	// One DO block per removed constraint. The previous implementation emitted
	// four nodes (create/exec/drop/drop) but none of them actually invoked the
	// drop logic, leaving the constraint in place — see commit message for the
	// full incident report. The DO block executes immediately on parse and
	// leaves no temporary functions behind.
	c.Assert(nodes, qt.HasLen, 1)

	sql, err := renderer.RenderSQL("postgres", nodes[0])
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DO $ptah$")
	c.Assert(sql, qt.Contains, "information_schema.table_constraints")
	c.Assert(sql, qt.Contains, "constraint_name = 'old_constraint'")
	c.Assert(sql, qt.Contains, "ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I")
	c.Assert(sql, qt.Contains, "$ptah$")
	// The DO block MUST end with a semicolon. SplitSQLStatements is what the
	// migrator uses to chop a migration into individual db.Exec calls, and it
	// tokenizes on `;`. A DO block whose dollar-quoted body ends at `$tag$`
	// without a trailing `;` merges with the following statement and Postgres
	// rejects the merged chunk with `syntax error at or near "DO"`.
	c.Assert(strings.HasSuffix(strings.TrimRight(sql, " \t\r\n"), ";"), qt.IsTrue,
		qt.Commentf("rendered DO block must end with a semicolon; got: %s", sql))
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved_MultipleSplitCleanly(t *testing.T) {
	c := qt.New(t)

	// Two consecutive constraint drops must split into two statements through
	// the SQL statement splitter. The regression this guards against is the
	// missing `;` in VisitRawSQL: without it, two DO blocks merge into one
	// chunk and Postgres rejects the second `DO`.
	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"first_constraint", "second_constraint"},
	}
	statements := planner.GenerateSchemaDiffSQLStatements(diff, &goschema.Database{}, "postgres")
	c.Assert(statements, qt.HasLen, 2,
		qt.Commentf("each DO block must end up as its own statement after SQL splitting; got %d statements:\n%s",
			len(statements), strings.Join(statements, "\n---\n")))
	c.Assert(statements[0], qt.Contains, "first_constraint")
	c.Assert(statements[1], qt.Contains, "second_constraint")
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved_EscapesSingleQuoteInName(t *testing.T) {
	c := qt.New(t)

	// PostgreSQL allows quoted identifiers with embedded apostrophes; the
	// DO block interpolates the constraint name into five distinct contexts
	// (one SQL literal, one EXECUTE format() argument, two RAISE NOTICE
	// strings, one SQL comment) and every literal substitution must escape.
	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"don't_drop"},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
	c.Assert(nodes, qt.HasLen, 1)

	sql, err := renderer.RenderSQL("postgres", nodes[0])
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "'don''t_drop'", qt.Commentf("single quote in constraint name must be SQL-escaped"))
	// The bare unescaped form must NOT appear in any SQL string literal.
	c.Assert(sql, qt.Not(qt.Contains), "'don't_drop'",
		qt.Commentf("unescaped name in a literal would break parsing; got: %s", sql))
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved_RejectsUnsafeName(t *testing.T) {
	// Names containing $ or newlines would either break the surrounding
	// `$ptah$` dollar-quote tag or terminate the leading SQL comment line.
	// Rather than emit a malformed drop (or a silent warning comment that
	// would loop on every subsequent generate run), the planner emits a DO
	// block whose only action is RAISE EXCEPTION — so the migration fails
	// loudly and the operator has to rename the constraint.
	cases := []struct {
		input          string
		expectVisible  string // the escaped name as it should appear in the SQL literal
		mustNotContain []string
	}{
		{
			input:          "foo$ptah$bar",
			expectVisible:  `foo\$ptah\$bar`, // `$` rendered as `\$` so it can't collapse $ptah$
			mustNotContain: []string{"$ptah$bar"},
		},
		{
			input:         "two\nlines",
			expectVisible: `two\nlines`,
		},
		{
			input:         "carriage\rreturn",
			expectVisible: `carriage\rreturn`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{ConstraintsRemoved: []string{tc.input}}
			nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
			c.Assert(nodes, qt.HasLen, 1)

			sql, err := renderer.RenderSQL("postgres", nodes[0])
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "RAISE EXCEPTION",
				qt.Commentf("planner must emit RAISE EXCEPTION for unsafe names; got: %s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "ALTER TABLE %I DROP CONSTRAINT",
				qt.Commentf("unsafe name must NOT produce a drop statement; got: %s", sql))
			// The rejected name must appear inside an embedded SQL single-
			// quoted literal (not as a Postgres identifier), so the operator
			// sees a clean exception message.
			c.Assert(sql, qt.Contains, "''"+tc.expectVisible+"''",
				qt.Commentf("rejected name must appear escaped inside the exception message; got: %s", sql))
			for _, forbidden := range tc.mustNotContain {
				c.Assert(sql, qt.Not(qt.Contains), forbidden,
					qt.Commentf("escaped output must not contain raw substring %q; got: %s", forbidden, sql))
			}
		})
	}
}
