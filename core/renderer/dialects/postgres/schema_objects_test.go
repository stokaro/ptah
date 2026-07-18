package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer"
)

func TestPostgreSQLRenderer_ViewsMaterializedViewsAndTriggers(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("postgres",
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

	sql, err := renderer.RenderSQL("postgres",
		ast.NewDropTrigger("set_updated_at", "users").
			SetIfExists().
			SetFunctionName("ptah_trigger_custom_set_updated_at"),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "DROP TRIGGER IF EXISTS set_updated_at ON users;")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "DROP FUNCTION IF EXISTS ptah_trigger_custom_set_updated_at();")
}

func TestPostgreSQLRenderer_DefaultTriggerFunctionNameIsTableScoped(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("postgres",
		ast.NewCreateTrigger("set_updated_at", "public.users").
			SetTiming("BEFORE").
			SetEvent("UPDATE").
			SetBody("NEW.updated_at = NOW(); RETURN NEW;"),
		ast.NewDropTrigger("set_updated_at", "public.users").SetIfExists(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_public_users_set_updated_at()")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "EXECUTE FUNCTION ptah_trigger_public_users_set_updated_at();")
	c.Assert(legacyPostgresSQL(sql), qt.Contains, "DROP FUNCTION IF EXISTS ptah_trigger_public_users_set_updated_at();")
}

func TestPostgreSQLRenderer_EscapesReservedIdentifiers(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("postgres",
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

	sql, err := renderer.RenderSQL("postgres",
		ast.NewCreateTable(`tenant"data`).
			AddColumn(ast.NewColumn(`order"key`, "integer")),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "tenant""data" (`)
	c.Assert(sql, qt.Contains, `"order""key" integer`)
}

func TestPostgreSQLRenderer_EscapesQualifiedDropIndex(t *testing.T) {
	c := qt.New(t)

	createSQL, err := renderer.RenderSQL("postgres",
		ast.NewIndex("audit.idx_user_order", "audit.user", "order"),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(createSQL, qt.Contains, `CREATE INDEX "audit"."idx_user_order" ON "audit"."user" ("order");`)

	dropSQL, err := renderer.RenderSQL("postgres",
		ast.NewDropIndex("audit.idx_user_order").SetIfExists(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(dropSQL, qt.Contains, `DROP INDEX IF EXISTS "audit"."idx_user_order";`)
}
