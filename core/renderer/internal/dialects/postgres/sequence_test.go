package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/postgres"
)

func TestPostgreSQLRenderer_VisitCreateSequence(t *testing.T) {
	t.Run("all options in canonical order", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		seq := ast.NewCreateSequence("order_seq").
			SetAs("bigint").
			SetIncrement(2).
			SetMinValue(1).
			SetMaxValue(9999).
			SetStart(1000).
			SetCache(20).
			SetCycle(true)
		sql, err := renderer.Render(seq)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `CREATE SEQUENCE "order_seq" AS bigint INCREMENT BY 2 MINVALUE 1 MAXVALUE 9999 START WITH 1000 CACHE 20 CYCLE;`)
	})

	t.Run("minimal", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		sql, err := renderer.Render(ast.NewCreateSequence("s"))

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `CREATE SEQUENCE "s";`)
	})

	t.Run("if not exists and schema qualifier", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		seq := ast.NewCreateSequence("s").SetSchema("app").SetIfNotExists()
		sql, err := renderer.Render(seq)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `CREATE SEQUENCE IF NOT EXISTS "app"."s";`)
	})

	t.Run("owned by", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		seq := ast.NewCreateSequence("s").SetOwnedBy("orders.id")
		sql, err := renderer.Render(seq)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `OWNED BY "orders"."id"`)
	})
}

func TestPostgreSQLRenderer_VisitAlterSequence(t *testing.T) {
	t.Run("changed options only", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		seq := ast.NewAlterSequence("s").SetIncrement(5).SetCycle(false)
		sql, err := renderer.Render(seq)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `ALTER SEQUENCE "s" INCREMENT BY 5 NO CYCLE;`)
	})

	t.Run("owned by none", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		seq := ast.NewAlterSequence("s").SetOwnedBy("NONE")
		sql, err := renderer.Render(seq)

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `ALTER SEQUENCE "s" OWNED BY NONE;`)
	})

	t.Run("no options renders nothing", func(t *testing.T) {
		c := qt.New(t)
		renderer := postgres.New()

		sql, err := renderer.Render(ast.NewAlterSequence("s"))

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Not(qt.Contains), "ALTER SEQUENCE")
	})
}

func TestPostgreSQLRenderer_VisitDropSequence(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()

	seq := ast.NewDropSequence("s").SetIfExists().SetCascade()
	sql, err := renderer.Render(seq)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP SEQUENCE IF EXISTS "s" CASCADE;`)
}
