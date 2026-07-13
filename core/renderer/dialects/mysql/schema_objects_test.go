package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
)

func TestMySQLRenderer_ViewsAndTriggers(t *testing.T) {
	c := qt.New(t)

	sql := renderMySQL(t,
		ast.NewCreateView("active_users").
			SetReplace().
			SetBody("SELECT id FROM users WHERE deleted_at IS NULL"),
		ast.NewCreateTrigger("set_updated_at", "users").
			SetTiming("BEFORE").
			SetEvent("UPDATE").
			SetBody("SET NEW.updated_at = NOW()").
			SetReplace(),
		ast.NewCreateMaterializedView("user_stats").
			SetBody("SELECT id, COUNT(*) FROM users GROUP BY id"),
	)

	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW active_users AS")
	c.Assert(sql, qt.Contains, "DROP TRIGGER IF EXISTS set_updated_at;")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW SET NEW.updated_at = NOW();")
	c.Assert(sql, qt.Contains, "-- MYSQL does not support CREATE MATERIALIZED VIEW user_stats")
}
