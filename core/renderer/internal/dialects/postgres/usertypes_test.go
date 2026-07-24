package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/postgres"
)

func TestPostgreSQLRenderer_VisitCreateType_Domain(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()

	domain := ast.NewDomainTypeDef("TEXT").SetNotNull().SetCheck("VALUE ~ '@'")
	sql, err := renderer.Render(ast.NewCreateType("email", domain))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE DOMAIN "email" AS TEXT NOT NULL CHECK (VALUE ~ '@');`)
}

func TestPostgreSQLRenderer_VisitCreateType_Composite(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()

	composite := ast.NewCompositeTypeDef(
		&ast.CompositeField{Name: "street", Type: "TEXT"},
		&ast.CompositeField{Name: "zip", Type: "INTEGER"},
	)
	sql, err := renderer.Render(ast.NewCreateType("address", composite))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TYPE "address" AS ("street" TEXT, "zip" INTEGER);`)
}

func TestPostgreSQLRenderer_VisitCreateType_Range(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()

	rangeDef := ast.NewRangeTypeDef("float8").SetSubtypeDiff("float8mi")
	sql, err := renderer.Render(ast.NewCreateType("floatrange", rangeDef))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TYPE "floatrange" AS RANGE (SUBTYPE = float8, SUBTYPE_DIFF = float8mi);`)
}

func TestPostgreSQLRenderer_VisitDropType_Domain(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()

	sql, err := renderer.Render(ast.NewDropType("email").SetDomain().SetIfExists().SetCascade())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP DOMAIN IF EXISTS "email" CASCADE;`)
}
