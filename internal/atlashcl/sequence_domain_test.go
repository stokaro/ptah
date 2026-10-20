package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/hclext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
)

func TestParseSequenceFullOptions(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(t.Context(), []byte(`
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

	db, err := atlashcl.Parse(t.Context(), []byte(`
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

	db, err := atlashcl.Parse(t.Context(), []byte(`
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

	db, err := atlashcl.Parse(t.Context(), []byte(`
sequence "app" "s" {}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].Name, qt.Equals, "s")
	c.Assert(db.Sequences[0].Schema, qt.Equals, "app")
}

func TestParseSequenceRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse(t.Context(), []byte(`
sequence "s" {
  nonsense = true
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported sequence attribute "nonsense".*`)
}

func TestParseSequenceRejectsNonIntegerStart(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse(t.Context(), []byte(`
sequence "s" {
  start = 1.5
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*sequence attribute "start" must be an integer.*`)
}

func TestParseSequenceRejectsNonIntegerType(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse(t.Context(), []byte(`
sequence "s" {
  type = text
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*sequence "s" type "text" is invalid.*`)
}

func TestParseSequenceRejectsOverflowingStart(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse(t.Context(), []byte(`
sequence "s" {
  start = 99999999999999999999
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*sequence attribute "start" must be an integer within the int64 range.*`)
}

func TestParseSequenceCanonicalizesTypeAlias(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(t.Context(), []byte(`
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

	_, err := atlashcl.Parse(t.Context(), []byte(`
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

	db, err := parseTypeHCL(t.Context(), []byte(`
domain "email" {
  type  = text
  null  = false
  check = "VALUE ~ '@'"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	objects := typeObjects(db, pgtype.DomainKind)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.DeepEquals, pgtype.Ref(pgtype.DomainKind, "", "email"))
	c.Assert(objects[0].Value, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Check: "VALUE ~ '@'"})

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE DOMAIN email AS text NOT NULL CHECK (VALUE ~ '@');`)
}

func TestParseDomainNullableByDefault(t *testing.T) {
	c := qt.New(t)

	db, err := parseTypeHCL(t.Context(), []byte(`
domain "nickname" {
  type = varchar(64)
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	objects := typeObjects(db, pgtype.DomainKind)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Value, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "varchar(64)"})
}

func TestParseDomainSchemaAndDefault(t *testing.T) {
	c := qt.New(t)

	db, err := parseTypeHCL(t.Context(), []byte(`
schema "app" {}

domain "positive" {
  schema  = schema.app
  type    = integer
  default = "0"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	objects := typeObjects(db, pgtype.DomainKind)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.DeepEquals, pgtype.Ref(pgtype.DomainKind, "app", "positive"))
	c.Assert(objects[0].Value, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "integer", Default: "0"})

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE DOMAIN app.positive AS integer DEFAULT 0;`)
}

func TestParseDomainDefaultExpression(t *testing.T) {
	c := qt.New(t)

	db, err := parseTypeHCL(t.Context(), []byte(`
domain "created" {
  type    = timestamptz
  default = sql("now()")
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	objects := typeObjects(db, pgtype.DomainKind)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Value, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "timestamptz", DefaultExpr: "now()"})
}

func TestParseDomainRequiresType(t *testing.T) {
	c := qt.New(t)

	_, err := parseTypeHCL(t.Context(), []byte(`
domain "x" {}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*domain "x" requires type.*`)
}

func TestParseDomainRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := parseTypeHCL(t.Context(), []byte(`
domain "x" {
  type     = text
  nonsense = true
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported domain attribute "nonsense".*`)
}

// TestParseDomain_NoOwnerReadsIt_FailurePath pins that a domain or composite
// block no selected owner reads is refused rather than dropped: a read with
// no owners, and a document of a target no type owner serves.
func TestParseDomain_NoOwnerReadsIt_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		options  atlashcl.Options
		wantErr  string
	}{
		{
			name:     "a domain with no owner selected",
			document: "domain \"x\" {\n  type = text\n}\n",
			options:  atlashcl.Options{HCL: hclext.None()},
			wantErr:  `parse HCL schema at schema.hcl:1,1-7: unsupported top-level block "domain"`,
		},
		{
			name:     "a composite type with no owner selected",
			document: "composite \"x\" {\n  field \"a\" {\n    type = text\n  }\n}\n",
			options:  atlashcl.Options{HCL: hclext.None()},
			wantErr:  `parse HCL schema at schema.hcl:1,1-10: unsupported top-level block "composite"`,
		},
		{
			name:     "a domain in a MySQL document",
			document: "domain \"x\" {\n  type = text\n}\n",
			options:  atlashcl.Options{HCL: builtintest.Runtime().HCL(), Dialect: "mysql"},
			wantErr:  `.*domain has no owner for dialect "mysql".*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.ParseWithOptions(t.Context(), []byte(test.document), "schema.hcl", test.options)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
