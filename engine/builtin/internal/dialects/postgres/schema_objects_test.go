package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
)

func TestPostgreSQLRenderer_ViewsMaterializedViewsAndTriggers(t *testing.T) {
	c := qt.New(t)

	sql, err := builtin.RenderSQL("postgres",
		ast.NewCreateView("active_users").
			SetReplace().
			SetBody("SELECT id FROM users WHERE deleted_at IS NULL").
			SetWithCheck(true),
		ast.NewCreateMaterializedView("user_stats").
			SetBody("SELECT id, COUNT(*) FROM users GROUP BY id"),
		ast.NewCreateTrigger("set_updated_at", "users").
			SetTiming("BEFORE").
			SetEvent("UPDATE").
			SetBody("NEW.updated_at = NOW(); RETURN NEW;").
			SetFunctionName("ptah_trigger_set_updated_at").
			SetReplace(),
	)
	c.Assert(err, qt.IsNil)
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "CREATE OR REPLACE VIEW active_users AS")
	c.Assert(sql, qt.Contains, "WITH CHECK OPTION")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "CREATE MATERIALIZED VIEW user_stats AS")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_set_updated_at()")
	c.Assert(sql, qt.Contains, "RETURNS trigger AS $$")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION ptah_trigger_set_updated_at();")
}

func TestPostgreSQLRenderer_DropTriggerUsesConfiguredFunctionName(t *testing.T) {
	c := qt.New(t)

	sql, err := builtin.RenderSQL("postgres",
		ast.NewDropTrigger("set_updated_at", "users").
			SetIfExists().
			SetFunctionName("ptah_trigger_custom_set_updated_at"),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "DROP TRIGGER IF EXISTS set_updated_at ON users;")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "DROP FUNCTION IF EXISTS ptah_trigger_custom_set_updated_at();")
}

// The table part carries a "." between schema and table, and the trigger
// name here carries its own underscores; sanitizeTriggerFunctionPart escapes
// both, so the derived name below doubles every underscore that was not
// itself the join FunctionName inserts (schemamodel's own
// TestTrigger_FunctionName_EscapesTheJoinBoundary covers the escaping itself;
// this locks in that the renderer's own auto-derive call sites still reach
// it, for both CREATE and DROP).
func TestPostgreSQLRenderer_DefaultTriggerFunctionNameIsTableScoped(t *testing.T) {
	c := qt.New(t)

	sql, err := builtin.RenderSQL("postgres",
		ast.NewCreateTrigger("set_updated_at", "public.users").
			SetTiming("BEFORE").
			SetEvent("UPDATE").
			SetBody("NEW.updated_at = NOW(); RETURN NEW;"),
		ast.NewDropTrigger("set_updated_at", "public.users").SetIfExists(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "CREATE FUNCTION ptah_trigger_public__users_set__updated__at()")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "EXECUTE FUNCTION ptah_trigger_public__users_set__updated__at();")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "DROP FUNCTION IF EXISTS ptah_trigger_public__users_set__updated__at();")
}

// TestPostgreSQLRenderer_CollidingTableAndTriggerNamesGetDistinctFunctions is
// the reported collision, reached through the renderer's own auto-derive call
// sites rather than through schemamodel.Trigger.FunctionName directly:
// VisitCreateTrigger and VisitDropTrigger both derive the function name from
// a bare ast node when FunctionName is left unset, which is the path
// postgresTriggerFunctionName used to duplicate before it was rerouted to the
// one implementation in core/schemamodel.
func TestPostgreSQLRenderer_CollidingTableAndTriggerNamesGetDistinctFunctions(t *testing.T) {
	c := qt.New(t)

	underscoreInTable, err := builtin.RenderSQL("postgres",
		ast.NewCreateTrigger("c", "a_b").SetTiming("BEFORE").SetEvent("UPDATE").SetBody("RETURN NEW;"),
	)
	c.Assert(err, qt.IsNil)

	underscoreInName, err := builtin.RenderSQL("postgres",
		ast.NewCreateTrigger("b_c", "a").SetTiming("BEFORE").SetEvent("UPDATE").SetBody("RETURN NEW;"),
	)
	c.Assert(err, qt.IsNil)

	c.Assert(legacyPostgresSQL(underscoreInTable), qt.Contains, "CREATE FUNCTION ptah_trigger_a__b_c()")
	c.Assert(legacyPostgresSQL(underscoreInName), qt.Contains, "CREATE FUNCTION ptah_trigger_a_b__c()")

	dropUnderscoreInTable, err := builtin.RenderSQL("postgres",
		ast.NewDropTrigger("c", "a_b").SetIfExists(),
	)
	c.Assert(err, qt.IsNil)

	dropUnderscoreInName, err := builtin.RenderSQL("postgres",
		ast.NewDropTrigger("b_c", "a").SetIfExists(),
	)
	c.Assert(err, qt.IsNil)

	c.Assert(legacyPostgresSQL(dropUnderscoreInTable), qt.Contains, "DROP FUNCTION IF EXISTS ptah_trigger_a__b_c();")
	c.Assert(legacyPostgresSQL(dropUnderscoreInName), qt.Contains, "DROP FUNCTION IF EXISTS ptah_trigger_a_b__c();")
}

func TestPostgreSQLRenderer_EscapesReservedIdentifiers(t *testing.T) {
	c := qt.New(t)

	sql, err := builtin.RenderSQL("postgres",
		ast.NewCreateTable("user").
			AddColumn(ast.NewColumn("order", "integer").SetNotNull()).
			AddColumn(ast.NewColumn("key", "text")).
			AddConstraint(&ast.ConstraintNode{
				Type:    ast.UniqueConstraint,
				Name:    "user_order_key",
				Columns: []string{"order", "key"},
			}),
		ast.NewIndex("idx_user_order", "user", "order"),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "user" (`)
	c.Assert(sql, qt.Contains, `"order" integer NOT NULL`)
	c.Assert(sql, qt.Contains, `"key" text`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "user_order_key" UNIQUE ("order", "key")`)
	c.Assert(sql, qt.Contains, `CREATE INDEX "idx_user_order" ON "user" ("order");`)
}

func TestPostgreSQLRenderer_EscapesEmbeddedDoubleQuotes(t *testing.T) {
	c := qt.New(t)

	sql, err := builtin.RenderSQL("postgres",
		ast.NewCreateTable(`tenant"data`).
			AddColumn(ast.NewColumn(`order"key`, "integer")),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "tenant""data" (`)
	c.Assert(sql, qt.Contains, `"order""key" integer`)
}

func TestPostgreSQLRenderer_EscapesQualifiedDropIndex(t *testing.T) {
	c := qt.New(t)

	createSQL, err := builtin.RenderSQL("postgres",
		ast.NewIndex("idx_user_order", "audit.user", "order"),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(createSQL, qt.Contains, `CREATE INDEX "idx_user_order" ON "audit"."user" ("order");`)

	dropSQL, err := builtin.RenderSQL("postgres",
		ast.NewDropIndex("idx_user_order").SetTable("audit.user").SetIfExists(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(dropSQL, qt.Contains, `DROP INDEX IF EXISTS "audit"."idx_user_order";`)
}

func TestCockroachDBRenderer_QualifiesDropIndexWithOwningTable(t *testing.T) {
	c := qt.New(t)

	sql, err := builtin.RenderSQL(
		platform.CockroachDB,
		ast.NewDropIndex("idx_shared").
			SetTable("public.users").
			SetIfExists(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "DROP INDEX IF EXISTS \"public\".\"users\"@\"idx_shared\";\n")
}
