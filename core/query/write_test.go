package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

func TestInsertBuilder(t *testing.T) {
	t.Run("single row", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.InsertInto("users").
			Columns("id", "name").
			Values(int64(1), "alice").
			Build()
		sql, args, err := renderer.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "users" ("id", "name") VALUES ($1, $2)`)
		c.Assert(args, qt.DeepEquals, []any{int64(1), "alice"})
	})

	t.Run("multi row with returning", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.InsertInto("users").
			Columns("id", "name").
			Values(int64(1), "alice").
			Values(int64(2), "bob").
			Returning("id").
			Build()
		sql, args, err := renderer.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "users" ("id", "name") VALUES ($1, $2), ($3, $4) RETURNING "id"`)
		c.Assert(args, qt.DeepEquals, []any{int64(1), "alice", int64(2), "bob"})
	})

	t.Run("nil value binds SQL NULL", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.InsertInto("users").
			Columns("name", "deleted_at").
			Values("alice", nil).
			Build()
		sql, args, err := renderer.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "users" ("name", "deleted_at") VALUES ($1, $2)`)
		c.Assert(args, qt.DeepEquals, []any{"alice", nil})
	})
}

func TestUpdateBuilder(t *testing.T) {
	t.Run("set values are numbered before the where value", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.Update("users").
			Set("name", "bob").
			Set("email", "bob@example.com").
			Where(query.Eq("id", int64(7))).
			Returning("updated_at").
			Build()
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "users" SET "name" = $1, "email" = $2 WHERE "id" = $3 RETURNING "updated_at"`)
		c.Assert(args, qt.DeepEquals, []any{"bob", "bob@example.com", int64(7)})
	})

	t.Run("unconditional update renders without a where", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.Update("flags").
			Set("enabled", true).
			Unconditional().
			Build()
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "flags" SET "enabled" = $1`)
		c.Assert(args, qt.DeepEquals, []any{true})
	})

	t.Run("a where-less update without the opt-in is rejected at render time", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.Update("users").Set("active", false).Build()
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, "renderer: update without a WHERE clause must be marked unconditional")
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})
}

func TestDeleteBuilder(t *testing.T) {
	t.Run("delete with a where", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.DeleteFrom("users").
			Where(query.Eq("id", int64(7))).
			Build()
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "users" WHERE "id" = $1`)
		c.Assert(args, qt.DeepEquals, []any{int64(7)})
	})

	t.Run("unconditional delete renders without a where", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.DeleteFrom("sessions").Unconditional().Build()
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "sessions"`)
		c.Assert(args, qt.HasLen, 0)
	})

	t.Run("a where-less delete without the opt-in is rejected at render time", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.DeleteFrom("users").Build()
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, "renderer: delete without a WHERE clause must be marked unconditional")
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})
}

// TestWriteBuilder_SharedWhereFragment builds one WHERE expression and attaches it
// to both an UPDATE and a DELETE, mirroring the SELECT builder's shared-fragment
// story: the expression constructors return plain nodes, so a filter composes
// across statement kinds.
func TestWriteBuilder_SharedWhereFragment(t *testing.T) {
	filter := query.And(query.Eq("tenant_id", int64(42)), query.Eq("draft", true))

	t.Run("update reuses the fragment", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.Update("commodities").Set("archived", true).Where(filter).Build()
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "commodities" SET "archived" = $1 WHERE ("tenant_id" = $2 AND "draft" = $3)`)
		c.Assert(args, qt.DeepEquals, []any{true, int64(42), true})
	})

	t.Run("delete reuses the fragment", func(t *testing.T) {
		c := qt.New(t)
		stmt := query.DeleteFrom("commodities").Where(filter).Build()
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "commodities" WHERE ("tenant_id" = $1 AND "draft" = $2)`)
		c.Assert(args, qt.DeepEquals, []any{int64(42), true})
	})
}
