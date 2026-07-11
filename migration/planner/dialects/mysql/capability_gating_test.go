package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// mixedSharedFKDiff is the issue #207 mixed scenario (shared_fk modified on
// articles, purely removed from pages), reused here to prove the capability
// gating composes with the exactly-once drop discipline instead of replacing
// it.
func mixedSharedFKDiff() *types.SchemaDiff {
	return &types.SchemaDiff{
		ConstraintsAdded:   []string{"shared_fk"},
		ConstraintsRemoved: []string{"shared_fk"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
			{
				Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY",
				Columns: []string{"author_id"}, ForeignTable: "users", ForeignColumn: "id", OnDelete: "CASCADE",
			},
		},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "shared_fk", TableName: "articles", Type: "FOREIGN KEY"},
			{Name: "shared_fk", TableName: "pages", Type: "FOREIGN KEY"},
		},
	}
}

// TestPlanner_CapabilityGating_MariaDBGuardedConstraintDrops covers the
// issue #226 improvement: a planner configured with the MariaDB preset
// requests the IF EXISTS guard on constraint drops, and the mariadb renderer
// accepts it. Drops stay exactly-once per (table, name) — the guard is
// belt-and-braces on top of the #207 ownership discipline, not a replacement.
func TestPlanner_CapabilityGating_MariaDBGuardedConstraintDrops(t *testing.T) {
	c := qt.New(t)

	nodes := mysql.NewWithCapabilities(capability.MariaDB1011()).GenerateMigrationAST(mixedSharedFKDiff(), &goschema.Database{})
	sql, err := renderer.RenderSQL("mariadb", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Count(sql, "ALTER TABLE articles DROP FOREIGN KEY IF EXISTS shared_fk;"), qt.Equals, 1,
		qt.Commentf("modified host drop must carry the MariaDB IF EXISTS guard; got:\n%s", sql))
	c.Assert(strings.Count(sql, "ALTER TABLE pages DROP FOREIGN KEY IF EXISTS shared_fk;"), qt.Equals, 1,
		qt.Commentf("pure-removal drop must carry the guard too; got:\n%s", sql))
	c.Assert(strings.Count(sql, "DROP FOREIGN KEY IF EXISTS shared_fk;"), qt.Equals, 2,
		qt.Commentf("guards must not change the exactly-once discipline; got:\n%s", sql))
	c.Assert(strings.Count(sql, "ALTER TABLE articles ADD CONSTRAINT shared_fk FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE;"), qt.Equals, 1,
		qt.Commentf("the re-add is unaffected by drop guarding; got:\n%s", sql))

	// Ordering discipline is intact.
	articlesDrop := strings.Index(sql, "ALTER TABLE articles DROP FOREIGN KEY IF EXISTS shared_fk")
	articlesAdd := strings.Index(sql, "ALTER TABLE articles ADD CONSTRAINT shared_fk")
	pagesDrop := strings.Index(sql, "ALTER TABLE pages DROP FOREIGN KEY IF EXISTS shared_fk")
	c.Assert(articlesDrop >= 0 && articlesAdd >= 0 && pagesDrop >= 0, qt.IsTrue)
	c.Assert(articlesDrop < articlesAdd, qt.IsTrue)
	c.Assert(pagesDrop > articlesAdd, qt.IsTrue)
}

// TestPlanner_CapabilityGating_RendererStripsGuardsForMySQL pins the validity
// layer: even when a planner records the IF EXISTS intent (MariaDB preset),
// the mysql renderer must strip it — MySQL 8/9 reject the guard on every
// constraint-drop spelling, so a stray intent flag must never reach a MySQL
// server (issue #226).
func TestPlanner_CapabilityGating_RendererStripsGuardsForMySQL(t *testing.T) {
	c := qt.New(t)

	nodes := mysql.NewWithCapabilities(capability.MariaDB1011()).GenerateMigrationAST(mixedSharedFKDiff(), &goschema.Database{})
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS",
		qt.Commentf("the mysql renderer must strip guards the target rejects; got:\n%s", sql))
	c.Assert(strings.Count(sql, "ALTER TABLE articles DROP FOREIGN KEY shared_fk;"), qt.Equals, 1)
	c.Assert(strings.Count(sql, "ALTER TABLE pages DROP FOREIGN KEY shared_fk;"), qt.Equals, 1)
}

// TestPlanner_CapabilityGating_MySQLPlannerEmitsNoGuardIntent pins the intent
// layer from the opposite side: a MySQL-preset planner records no IF EXISTS
// intent, so even the guard-capable mariadb renderer has nothing to render.
func TestPlanner_CapabilityGating_MySQLPlannerEmitsNoGuardIntent(t *testing.T) {
	c := qt.New(t)

	nodes := mysql.New().GenerateMigrationAST(mixedSharedFKDiff(), &goschema.Database{})
	sql, err := renderer.RenderSQL("mariadb", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Not(qt.Contains), "DROP FOREIGN KEY IF EXISTS",
		qt.Commentf("a MySQL-preset planner must not request guards; got:\n%s", sql))
	c.Assert(strings.Count(sql, "ALTER TABLE articles DROP FOREIGN KEY shared_fk;"), qt.Equals, 1)
}

// TestPlanner_CapabilityGating_DropCheckSpellingWithoutGenericClause covers
// the MySQL 8.0.16–8.0.18 window (capability.MySQL8016): the generic
// DROP CONSTRAINT clause does not exist there, so a CHECK removal must use
// the dedicated ALTER TABLE ... DROP CHECK spelling.
func TestPlanner_CapabilityGating_DropCheckSpellingWithoutGenericClause(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"chk_qty"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "chk_qty", TableName: "things", Type: "CHECK"},
		},
	}

	nodes := mysql.NewWithCapabilities(capability.MySQL8016()).GenerateMigrationAST(diff, &goschema.Database{})
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Count(sql, "ALTER TABLE things DROP CHECK chk_qty;"), qt.Equals, 1,
		qt.Commentf("without drop_constraint_generic a CHECK drop must use DROP CHECK; got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT",
		qt.Commentf("the generic clause must not be emitted for this target; got:\n%s", sql))

	// The current MySQL line keeps the generic clause.
	nodes = mysql.New().GenerateMigrationAST(diff, &goschema.Database{})
	sql, err = renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(sql, "ALTER TABLE things DROP CONSTRAINT chk_qty;"), qt.Equals, 1,
		qt.Commentf("modern MySQL keeps DROP CONSTRAINT; got:\n%s", sql))
}

// TestPlanner_CapabilityGating_NoGenericClauseFallbacks covers the remaining
// constraint types on targets without the generic DROP CONSTRAINT clause
// (MySQL before 8.0.19): a UNIQUE removal must use DROP INDEX (dropping the
// backing index — valid across the whole family), and a CHECK removal on a
// target with NEITHER spelling (MySQLLegacy) must degrade to a loud WARNING
// instead of emitting SQL the server rejects.
func TestPlanner_CapabilityGating_NoGenericClauseFallbacks(t *testing.T) {
	t.Run("unique removal uses DROP INDEX", func(t *testing.T) {
		c := qt.New(t)

		diff := &types.SchemaDiff{
			ConstraintsRemoved: []string{"uq_email"},
			ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
				{Name: "uq_email", TableName: "users", Type: "UNIQUE"},
			},
		}
		// The DROP INDEX spelling is version-universal (issue #195), so every
		// preset uses it — including targets without the generic clause,
		// where DROP CONSTRAINT would be invalid SQL.
		for _, caps := range []capability.Capabilities{capability.MySQL8016(), capability.MySQL80()} {
			nodes := mysql.NewWithCapabilities(caps).GenerateMigrationAST(diff, &goschema.Database{})
			sql, err := renderer.RenderSQL("mysql", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Count(sql, "ALTER TABLE users DROP INDEX uq_email;"), qt.Equals, 1,
				qt.Commentf("a UNIQUE drop must use DROP INDEX; got:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT",
				qt.Commentf("got:\n%s", sql))
		}
	})

	t.Run("check removal without any valid spelling warns", func(t *testing.T) {
		c := qt.New(t)

		diff := &types.SchemaDiff{
			ConstraintsRemoved: []string{"chk_qty"},
			ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
				{Name: "chk_qty", TableName: "things", Type: "CHECK"},
			},
		}
		nodes := mysql.NewWithCapabilities(capability.MySQLLegacy()).GenerateMigrationAST(diff, &goschema.Database{})
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)
		// No statement may be emitted at all — only the warning comment
		// (asserting on statement shape, not bare keywords, because the
		// warning text itself names the missing clauses).
		c.Assert(sql, qt.Not(qt.Contains), "ALTER TABLE", qt.Commentf("got:\n%s", sql))
		c.Assert(sql, qt.Contains, "WARNING: cannot drop CHECK constraint chk_qty",
			qt.Commentf("the impossibility must be loud; got:\n%s", sql))
	})
}

// TestPlanner_CapabilityGating_CheckAddSkippedWhenUnenforced covers the
// capability.MySQLLegacy window (before 8.0.16): the server parses CHECK
// constraints and silently ignores them, so emitting ADD CONSTRAINT ... CHECK
// would leave ptah believing a constraint exists that the server never
// enforces. The planner surfaces a warning comment instead — for both the
// table-level and the synthesized field-level CHECK paths.
func TestPlanner_CapabilityGating_CheckAddSkippedWhenUnenforced(t *testing.T) {
	t.Run("table-level constraint", func(t *testing.T) {
		c := qt.New(t)

		diff := &types.SchemaDiff{ConstraintsAdded: []string{"positive_price"}}
		generated := &goschema.Database{
			Constraints: []goschema.Constraint{
				{StructName: "Product", Name: "positive_price", Type: "CHECK", Table: "products", CheckExpression: "price > 0"},
			},
		}

		nodes := mysql.NewWithCapabilities(capability.MySQLLegacy()).GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT",
			qt.Commentf("an unenforced CHECK must not be emitted; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "WARNING: CHECK constraint positive_price skipped",
			qt.Commentf("the skip must be loud; got:\n%s", sql))

		// The enforcing window (8.0.16+) emits the constraint as usual.
		nodes = mysql.NewWithCapabilities(capability.MySQL8016()).GenerateMigrationAST(diff, generated)
		sql, err = renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, "ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
			qt.Commentf("enforcing targets keep the ADD; got:\n%s", sql))
	})

	t.Run("field-level synthesized constraint", func(t *testing.T) {
		c := qt.New(t)

		diff := &types.SchemaDiff{
			ConstraintsAdded:   []string{"things_qty_check"},
			ConstraintsRemoved: []string{},
		}
		generated := &goschema.Database{
			Tables: []goschema.Table{{StructName: "Thing", Name: "things"}},
			Fields: []goschema.Field{
				{StructName: "Thing", Name: "qty", Type: "INT", Check: "qty >= 0"},
			},
		}

		nodes := mysql.NewWithCapabilities(capability.MySQLLegacy()).GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT",
			qt.Commentf("an unenforced field-level CHECK must not be emitted; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "WARNING: CHECK constraint things_qty_check skipped",
			qt.Commentf("the skip must be loud; got:\n%s", sql))

		// Positive control at the unit level: an enforcing target emits the
		// field-level ADD as before.
		nodes = mysql.New().GenerateMigrationAST(diff, generated)
		sql, err = renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, "ALTER TABLE things ADD CONSTRAINT things_qty_check CHECK (qty >= 0);",
			qt.Commentf("enforcing targets keep the field-level ADD; got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "WARNING")
	})
}

// TestPlanner_CapabilityGating_ZeroValuePlannerBehavesLikeNew guards the
// documented construction `planner := &mysql.Planner{}` (the type's own usage
// example): the nil capability set must default to the modern MySQL preset,
// NOT to an assume-nothing set — otherwise a zero-value planner would
// silently skip CHECK additions (turning a CHECK modification into a
// destructive drop-without-re-add) and re-spell CHECK drops as DROP CHECK.
func TestPlanner_CapabilityGating_ZeroValuePlannerBehavesLikeNew(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsAdded:   []string{"positive_price"},
		ConstraintsRemoved: []string{"chk_old"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "chk_old", TableName: "things", Type: "CHECK"},
		},
	}
	generated := &goschema.Database{
		Constraints: []goschema.Constraint{
			{StructName: "Product", Name: "positive_price", Type: "CHECK", Table: "products", CheckExpression: "price > 0"},
		},
	}

	zero := &mysql.Planner{}
	zeroSQL, err := renderer.RenderSQL("mysql", zero.GenerateMigrationAST(diff, generated)...)
	c.Assert(err, qt.IsNil)
	newSQL, err := renderer.RenderSQL("mysql", mysql.New().GenerateMigrationAST(diff, generated)...)
	c.Assert(err, qt.IsNil)

	c.Assert(zeroSQL, qt.Equals, newSQL,
		qt.Commentf("a zero-value planner must be byte-identical to New()"))
	c.Assert(zeroSQL, qt.Contains, "ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
		qt.Commentf("the CHECK addition must be emitted, not downgraded to a warning; got:\n%s", zeroSQL))
	c.Assert(zeroSQL, qt.Contains, "ALTER TABLE things DROP CONSTRAINT chk_old;",
		qt.Commentf("the CHECK removal must use the generic clause, not DROP CHECK; got:\n%s", zeroSQL))
}

// TestPlanner_CapabilityGating_DropCheckDegradesOnMariaDBRenderer pins the
// renderer-side spelling resolution: DROP CHECK is a MySQL-only clause
// (MariaDB 10.11 rejects it — verified live), so a plan built for the MySQL
// 8.0.16–8.0.18 window rendered through the mariadb renderer must degrade to
// the generic DROP CONSTRAINT clause rather than emit SQL MariaDB rejects.
func TestPlanner_CapabilityGating_DropCheckDegradesOnMariaDBRenderer(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"chk_qty"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "chk_qty", TableName: "things", Type: "CHECK"},
		},
	}
	nodes := mysql.NewWithCapabilities(capability.MySQL8016()).GenerateMigrationAST(diff, &goschema.Database{})

	sql, err := renderer.RenderSQL("mariadb", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE things DROP CONSTRAINT chk_qty;",
		qt.Commentf("the mariadb renderer must degrade DROP CHECK to the generic clause; got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP CHECK",
		qt.Commentf("MariaDB has no DROP CHECK clause; got:\n%s", sql))
}

// TestPlanner_CapabilityGating_DropIndexGuard covers the DROP INDEX guard
// through both capability layers: the guard is emitted only when the PLANNER
// records the intent (its capability set includes DropIndexIfExists — the
// MariaDB preset) AND the RENDERER accepts it (mariadb; the mysql renderer
// strips the guard because MySQL has no such form). This also makes the
// capability a live knob: a MySQL-preset planner produces no guard even for
// the guard-capable mariadb renderer.
func TestPlanner_CapabilityGating_DropIndexGuard(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesRemoved: []string{"idx_things_qty"},
		IndexesRemovedWithTables: []types.IndexRemovalInfo{
			{Name: "idx_things_qty", TableName: "things"},
		},
	}

	// MariaDB-preset planner: intent recorded.
	nodes := mysql.NewWithCapabilities(capability.MariaDB1011()).GenerateMigrationAST(diff, &goschema.Database{})

	sqlMariaDB, err := renderer.RenderSQL("mariadb", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sqlMariaDB, qt.Contains, "DROP INDEX IF EXISTS idx_things_qty ON things;",
		qt.Commentf("mariadb honors the guard intent; got:\n%s", sqlMariaDB))

	sqlMySQL, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sqlMySQL, qt.Contains, "DROP INDEX idx_things_qty ON things;",
		qt.Commentf("the mysql renderer strips the guard it cannot parse; got:\n%s", sqlMySQL))
	c.Assert(sqlMySQL, qt.Not(qt.Contains), "IF EXISTS")

	// MySQL-preset planner: no intent, so even the guard-capable mariadb
	// renderer emits the plain form — the capability is a real knob.
	nodes = mysql.New().GenerateMigrationAST(diff, &goschema.Database{})
	sqlMariaDB, err = renderer.RenderSQL("mariadb", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sqlMariaDB, qt.Contains, "DROP INDEX idx_things_qty ON things;",
		qt.Commentf("got:\n%s", sqlMariaDB))
	c.Assert(sqlMariaDB, qt.Not(qt.Contains), "IF EXISTS",
		qt.Commentf("a MySQL-preset planner must not request the index-drop guard; got:\n%s", sqlMariaDB))
}

// TestPlanner_UniqueConstraintRemoval_UsesDropIndex is the issue #195
// acceptance test: a table-level UNIQUE constraint removal renders the
// version-universal ALTER TABLE ... DROP INDEX spelling on the MySQL family —
// never the generic DROP CONSTRAINT clause, which is invalid SQL before MySQL
// 8.0.19 (and empirically unnecessary on newer lines, where DROP INDEX works
// too). FK and PK behavior is unchanged: FK keeps DROP FOREIGN KEY, PK
// removals stay skipped. On MariaDB the spelling carries the IF EXISTS guard
// (verified live: MariaDB 10.11 accepts it, MySQL 9.7 rejects it).
func TestPlanner_UniqueConstraintRemoval_UsesDropIndex(t *testing.T) {
	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"uq_email", "fk_posts_user", "pk_legacy"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "uq_email", TableName: "users", Type: "UNIQUE"},
			{Name: "fk_posts_user", TableName: "posts", Type: "FOREIGN KEY"},
			{Name: "pk_legacy", TableName: "legacy", Type: "PRIMARY KEY"},
		},
	}

	t.Run("mysql", func(t *testing.T) {
		c := qt.New(t)

		nodes := mysql.New().GenerateMigrationAST(diff, &goschema.Database{})
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Count(sql, "ALTER TABLE users DROP INDEX uq_email;"), qt.Equals, 1,
			qt.Commentf("UNIQUE removal must render DROP INDEX; got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT",
			qt.Commentf("the generic clause must not be used for UNIQUE; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user;"), qt.Equals, 1,
			qt.Commentf("FK drops unchanged; got:\n%s", sql))
		c.Assert(sql, qt.Not(qt.Contains), "pk_legacy",
			qt.Commentf("PK removals stay skipped; got:\n%s", sql))
	})

	t.Run("mariadb preset", func(t *testing.T) {
		c := qt.New(t)

		nodes := mysql.NewWithCapabilities(capability.MariaDB1011()).GenerateMigrationAST(diff, &goschema.Database{})
		sql, err := renderer.RenderSQL("mariadb", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Count(sql, "ALTER TABLE users DROP INDEX IF EXISTS uq_email;"), qt.Equals, 1,
			qt.Commentf("MariaDB guards the DROP INDEX spelling; got:\n%s", sql))
		c.Assert(strings.Count(sql, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user;"), qt.Equals, 1,
			qt.Commentf("got:\n%s", sql))

		// The same plan through the mysql renderer strips every guard.
		sqlMySQL, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(sqlMySQL, qt.Not(qt.Contains), "IF EXISTS", qt.Commentf("got:\n%s", sqlMySQL))
		c.Assert(sqlMySQL, qt.Contains, "ALTER TABLE users DROP INDEX uq_email;",
			qt.Commentf("got:\n%s", sqlMySQL))
	})
}

// TestPlanner_UniqueDropGuard_FollowsIndexCapability pins the guard-source
// decoupling: the UNIQUE removal spelling is an index drop, so its IF EXISTS
// intent follows capability.DropIndexIfExists — independently of the
// constraint-drop guard capability. Identical on shipped presets; a composed
// set enabling the guards separately must guard each spelling per its own
// capability.
func TestPlanner_UniqueDropGuard_FollowsIndexCapability(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"uq_email", "fk_posts_user"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "uq_email", TableName: "users", Type: "UNIQUE"},
			{Name: "fk_posts_user", TableName: "posts", Type: "FOREIGN KEY"},
		},
	}

	// Index guards off, constraint guards on: FK guarded, UNIQUE not.
	caps := capability.MariaDB1011().With(capability.DropIndexIfExists, false)
	c.Assert(caps.Validate(), qt.IsNil)
	nodes := mysql.NewWithCapabilities(caps).GenerateMigrationAST(diff, &goschema.Database{})
	sql, err := renderer.RenderSQL("mariadb", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE users DROP INDEX uq_email;",
		qt.Commentf("got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP INDEX IF EXISTS",
		qt.Commentf("UNIQUE guard must follow the index-drop capability; got:\n%s", sql))
	c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY IF EXISTS fk_posts_user;",
		qt.Commentf("got:\n%s", sql))

	// Constraint guards off, index guards on: UNIQUE guarded, FK not.
	caps = capability.MariaDB1011().With(capability.DropConstraintIfExists, false)
	c.Assert(caps.Validate(), qt.IsNil)
	nodes = mysql.NewWithCapabilities(caps).GenerateMigrationAST(diff, &goschema.Database{})
	sql, err = renderer.RenderSQL("mariadb", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE users DROP INDEX IF EXISTS uq_email;",
		qt.Commentf("got:\n%s", sql))
	c.Assert(sql, qt.Contains, "ALTER TABLE posts DROP FOREIGN KEY fk_posts_user;",
		qt.Commentf("got:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP FOREIGN KEY IF EXISTS",
		qt.Commentf("FK guard must follow the constraint-drop capability; got:\n%s", sql))
}
