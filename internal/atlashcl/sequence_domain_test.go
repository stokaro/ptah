package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseSequenceFullOptions(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
sequence "order_seq" {
  type      = bigint
  start     = 1000
  increment = 2
  min_value = 1
  max_value = 9999
  cache     = 20
  cycle     = true
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Name, qt.Equals, "order_seq")
	c.Assert(db.Sequences[0].AsType, qt.Equals, "bigint")
	c.Assert(db.Sequences[0].Start, qt.IsNotNil)
	c.Assert(*db.Sequences[0].Start, qt.Equals, int64(1000))
	c.Assert(db.Sequences[0].Cycle, qt.IsTrue)

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE SEQUENCE order_seq AS bigint INCREMENT BY 2 MINVALUE 1 MAXVALUE 9999 START WITH 1000 CACHE 20 CYCLE;`)
}

func TestParseSequenceMinimal(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
sequence "s" {}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Start, qt.IsNil)

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE SEQUENCE s;`)
}

func TestParseSequenceSchemaOwnedByAndIfNotExists(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
sequence "s" {
  schema        = schema.app
  owned_by      = "orders.id"
  if_not_exists = true
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Schema, qt.Equals, "app")
	c.Assert(db.Sequences[0].OwnedBy, qt.Equals, "orders.id")
	c.Assert(db.Sequences[0].IfNotExists, qt.IsTrue)

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE SEQUENCE IF NOT EXISTS app.s`)
	c.Assert(sql, qt.Contains, `OWNED BY orders.id`)
}

func TestParseSequenceTwoLabelSchema(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
sequence "app" "s" {}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Name, qt.Equals, "s")
	c.Assert(db.Sequences[0].Schema, qt.Equals, "app")
}

func TestParseSequenceRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
sequence "s" {
  nonsense = true
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported sequence attribute "nonsense".*`)
}

func TestParseSequenceRejectsNonIntegerStart(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
sequence "s" {
  start = 1.5
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*sequence attribute "start" must be an integer.*`)
}

func TestParseSequenceRejectsNonIntegerType(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
sequence "s" {
  type = text
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*sequence "s" type "text" is invalid.*`)
}

func TestParseSequenceRejectsOverflowingStart(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
sequence "s" {
  start = 99999999999999999999
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*sequence attribute "start" must be an integer within the int64 range.*`)
}

func TestParseSequenceCanonicalizesTypeAlias(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
sequence "s" {
  type = int8
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].AsType, qt.Equals, "bigint")
}

func TestParseSequenceRejectsNestedBlock(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
sequence "s" {
  owner {
    column = column.orders.id
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported sequence block "owner".*`)
}

func TestParseDomainFullOptions(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
domain "email" {
  type  = text
  null  = false
  check = "VALUE ~ '@'"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Domains, qt.HasLen, 1)
	c.Assert(db.Domains[0].Name, qt.Equals, "email")
	c.Assert(db.Domains[0].BaseType, qt.Equals, "text")
	c.Assert(db.Domains[0].NotNull, qt.IsTrue)
	c.Assert(db.Domains[0].Check, qt.Equals, "VALUE ~ '@'")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE DOMAIN email AS text NOT NULL CHECK (VALUE ~ '@');`)
}

func TestParseDomainNullableByDefault(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
domain "nickname" {
  type = varchar(64)
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Domains, qt.HasLen, 1)
	c.Assert(db.Domains[0].NotNull, qt.IsFalse)
}

func TestParseDomainSchemaAndDefault(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
domain "positive" {
  schema  = schema.app
  type    = integer
  default = "0"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Domains, qt.HasLen, 1)
	c.Assert(db.Domains[0].Schema, qt.Equals, "app")
	c.Assert(db.Domains[0].Default, qt.Equals, "0")
	c.Assert(db.Domains[0].DefaultExpr, qt.Equals, "")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE DOMAIN app.positive AS integer`)
	c.Assert(sql, qt.Contains, `DEFAULT`)
}

func TestParseDomainDefaultExpression(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
domain "created" {
  type    = timestamptz
  default = sql("now()")
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Domains, qt.HasLen, 1)
	c.Assert(db.Domains[0].DefaultExpr, qt.Equals, "now()")
	c.Assert(db.Domains[0].Default, qt.Equals, "")
}

func TestParseDomainRequiresType(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
domain "x" {}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*domain "x" requires type.*`)
}

func TestParseDomainRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
domain "x" {
  type     = text
  nonsense = true
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported domain attribute "nonsense".*`)
}
