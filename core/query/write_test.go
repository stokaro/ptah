package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

func TestInsertBuilder(t *testing.T) {
	c := qt.New(t)

	c.Run("single row", func(c *qt.C) {
		stmt := query.InsertInto("users").
			Columns("id", "name").
			Values(int64(1), "alice").
			Build()
		sql, args, err := renderer.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "users" ("id", "name") VALUES ($1, $2)`)
		c.Assert(args, qt.DeepEquals, []any{int64(1), "alice"})
	})

	c.Run("multi row with returning", func(c *qt.C) {
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

	c.Run("nil value binds SQL NULL", func(c *qt.C) {
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
	c := qt.New(t)

	c.Run("set values are numbered before the where value", func(c *qt.C) {
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

	c.Run("unconditional update renders without a where", func(c *qt.C) {
		stmt := query.Update("flags").
			Set("enabled", true).
			Unconditional().
			Build()
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "flags" SET "enabled" = $1`)
		c.Assert(args, qt.DeepEquals, []any{true})
	})

	c.Run("a where-less update without the opt-in is rejected at render time", func(c *qt.C) {
		stmt := query.Update("users").Set("active", false).Build()
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, "renderer: update without a WHERE clause must be marked unconditional")
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})
}

func TestDeleteBuilder(t *testing.T) {
	c := qt.New(t)

	c.Run("delete with a where", func(c *qt.C) {
		stmt := query.DeleteFrom("users").
			Where(query.Eq("id", int64(7))).
			Build()
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "users" WHERE "id" = $1`)
		c.Assert(args, qt.DeepEquals, []any{int64(7)})
	})

	c.Run("unconditional delete renders without a where", func(c *qt.C) {
		stmt := query.DeleteFrom("sessions").Unconditional().Build()
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "sessions"`)
		c.Assert(args, qt.HasLen, 0)
	})

	c.Run("a where-less delete without the opt-in is rejected at render time", func(c *qt.C) {
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
	c := qt.New(t)

	filter := query.And(query.Eq("tenant_id", int64(42)), query.Eq("draft", true))

	c.Run("update reuses the fragment", func(c *qt.C) {
		stmt := query.Update("commodities").Set("archived", true).Where(filter).Build()
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "commodities" SET "archived" = $1 WHERE ("tenant_id" = $2 AND "draft" = $3)`)
		c.Assert(args, qt.DeepEquals, []any{true, int64(42), true})
	})

	c.Run("delete reuses the fragment", func(c *qt.C) {
		stmt := query.DeleteFrom("commodities").Where(filter).Build()
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "commodities" WHERE ("tenant_id" = $1 AND "draft" = $2)`)
		c.Assert(args, qt.DeepEquals, []any{int64(42), true})
	})
}
