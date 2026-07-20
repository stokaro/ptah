package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_TableQualifiedCheckAndUniqueAdditions(t *testing.T) {
	tests := []struct {
		name     string
		diff     *types.SchemaDiff
		wantDrop string
		wantAdd  string
	}{
		{
			name: "unique to check",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"products_quantity_guard"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:            "products_quantity_guard",
					TableName:       "products",
					Type:            "CHECK",
					CheckExpression: "quantity > 10",
				}},
				ConstraintsRemoved: []string{"products_quantity_guard"},
				ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
					Name:      "products_quantity_guard",
					TableName: "products",
					Type:      "UNIQUE",
				}},
			},
			wantDrop: "ALTER TABLE products DROP CONSTRAINT IF EXISTS products_quantity_guard;",
			wantAdd:  "ALTER TABLE products ADD CONSTRAINT products_quantity_guard CHECK (quantity > 10);",
		},
		{
			name: "check to unique",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"accounts_identity"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:      "accounts_identity",
					TableName: "accounts",
					Type:      "UNIQUE",
					Columns:   []string{"email", "region"},
				}},
				ConstraintsRemoved: []string{"accounts_identity"},
				ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
					Name:      "accounts_identity",
					TableName: "accounts",
					Type:      "CHECK",
				}},
			},
			wantDrop: "ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_identity;",
			wantAdd:  "ALTER TABLE accounts ADD CONSTRAINT accounts_identity UNIQUE (email, region);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("postgres", postgres.New().GenerateMigrationAST(tt.diff, &goschema.Database{})...)
			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)

			dropIdx := strings.Index(sql, tt.wantDrop)
			addIdx := strings.Index(sql, tt.wantAdd)
			c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
				qt.Commentf("drop must precede add; got:\n%s", sql))
			c.Assert(strings.Count(sql, tt.wantAdd), qt.Equals, 1)
		})
	}
}

// TestPlanner_GenerateMigrationAST_HostlessReAdd_DropsExactlyOnce guards the
// hostless-re-add ownership rule (issue #229). When a name is re-added with NO
// recorded addition hosts (ConstraintsAdded carries it but
// ConstraintsAddedWithTables has no entry — the shape of every reverse/down
// diff whose old constraint body could not be reconstructed), the add side
// drops every recorded removal host BEFORE the re-add, and removeConstraints
// must skip the name entirely.
//
// Before the fix removeConstraints emitted a second guarded drop AFTER the
// re-add — and IF EXISTS is no protection against dropping a constraint that
// exists again: the down migration silently deleted the constraint it had
// just restored (DROP → ADD → DROP). The exactly-once counts below fail
// against that behavior.
func TestPlanner_GenerateMigrationAST_HostlessReAdd_DropsExactlyOnce(t *testing.T) {
	t.Run("single removal host", func(t *testing.T) {
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

		sql, err := renderer.RenderSQL("postgres", postgres.New().GenerateMigrationAST(diff, generated)...)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)

		c.Assert(strings.Count(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_down;"), qt.Equals, 1,
			qt.Commentf("the drop must be emitted exactly once across both planner phases; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ALTER TABLE things ADD CONSTRAINT chk_down CHECK (qty >= 0);"), qt.Equals, 1,
			qt.Commentf("got:\n%s", sql))

		dropIdx := strings.Index(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_down")
		addIdx := strings.Index(sql, "ALTER TABLE things ADD CONSTRAINT chk_down")
		c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
			qt.Commentf("the single drop must precede the re-add; got:\n%s", sql))
		lastDrop := strings.LastIndex(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_down")
		c.Assert(lastDrop < addIdx, qt.IsTrue,
			qt.Commentf("no drop may follow the re-add — it would delete the restored constraint; got:\n%s", sql))
	})

	t.Run("two removal hosts", func(t *testing.T) {
		// The down migration of a multi-host non-FK constraint modify: the
		// bare name arrives duplicated, every host sits in
		// ConstraintsRemovedWithTables, ConstraintsAddedWithTables is empty.
		// Every recorded host must be dropped exactly once, all before the
		// re-add, and removeConstraints must add nothing on top.
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

		sql, err := renderer.RenderSQL("postgres", postgres.New().GenerateMigrationAST(diff, generated)...)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)

		c.Assert(strings.Count(sql, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS shared_check;"), qt.Equals, 1,
			qt.Commentf("first removal host dropped exactly once; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ALTER TABLE pages DROP CONSTRAINT IF EXISTS shared_check;"), qt.Equals, 1,
			qt.Commentf("second removal host dropped exactly once; got:\n%s", sql))

		firstAdd := strings.Index(sql, "ADD CONSTRAINT shared_check")
		c.Assert(firstAdd >= 0, qt.IsTrue)
		c.Assert(strings.LastIndex(sql, "ALTER TABLE articles DROP CONSTRAINT IF EXISTS shared_check") < firstAdd, qt.IsTrue,
			qt.Commentf("all drops must precede the re-add; got:\n%s", sql))
		c.Assert(strings.LastIndex(sql, "ALTER TABLE pages DROP CONSTRAINT IF EXISTS shared_check") < firstAdd, qt.IsTrue,
			qt.Commentf("all drops must precede the re-add; got:\n%s", sql))
	})
}

// TestPlanner_GenerateMigrationAST_EmptyTableNameAdditionTreatedAsHostless is
// the postgres port of the MySQL guard from PR #228 (the literal issue #229
// trigger). A ConstraintsAddedWithTables entry with an empty TableName must
// not count as a recorded addition host: if it did, addedHostsByName would
// contain only "" (matching no real removal host), the required pre-drop
// would be skipped, and the re-ADD would collide with the still-present
// constraint (42710) — IF EXISTS cannot help with a drop that is never
// emitted. With the guard the name degrades to hostless-re-add semantics: one
// pre-drop per recorded removal host, then the re-add, nothing after it.
func TestPlanner_GenerateMigrationAST_EmptyTableNameAdditionTreatedAsHostless(t *testing.T) {
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

	sql, err := renderer.RenderSQL("postgres", postgres.New().GenerateMigrationAST(diff, generated)...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(strings.Count(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_ghost;"), qt.Equals, 1,
		qt.Commentf("the recorded removal host must be dropped exactly once; got:\n%s", sql))
	c.Assert(strings.Count(sql, "ALTER TABLE things ADD CONSTRAINT chk_ghost CHECK (qty >= 0);"), qt.Equals, 1,
		qt.Commentf("the re-add must still be emitted; got:\n%s", sql))

	dropIdx := strings.Index(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_ghost")
	addIdx := strings.Index(sql, "ALTER TABLE things ADD CONSTRAINT chk_ghost")
	c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
		qt.Commentf("the drop must be the add-side pre-drop, before the re-add; got:\n%s", sql))
	lastDrop := strings.LastIndex(sql, "ALTER TABLE things DROP CONSTRAINT IF EXISTS chk_ghost")
	c.Assert(lastDrop < addIdx, qt.IsTrue,
		qt.Commentf("no drop may follow the re-add; got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "information_schema.table_constraints",
		qt.Commentf("the name-only DO block must not be used when removal hosts are known"))
}

func TestPlanner_GenerateMigrationAST_TableQualifiedPrimaryKeyAddition(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsAdded: []string{"memberships_pkey"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name:      "memberships_pkey",
			TableName: "memberships",
			Type:      "PRIMARY KEY",
			Columns:   []string{"org_id", "user_id"},
		}},
	}

	sql, err := renderer.RenderSQL("postgres", postgres.New().GenerateMigrationAST(diff, &goschema.Database{})...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "ALTER TABLE memberships ADD PRIMARY KEY (org_id, user_id);")
}
