package dbmlrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbmlparse"
	"ptah.run/internal/dbmlrender"
)

// keyedSchema holds every key and constraint shape the model can carry, as
// schema inspect produces them: a composite primary key on the table, a
// composite foreign key and a second key over one column as FOREIGN KEY
// constraints, a multi-column UNIQUE, named and unnamed checks, a column
// check, and an EXCLUDE, plus a DEFERRABLE key.
func keyedSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "p", Name: "p", PrimaryKey: []string{"a", "b"}, PrimaryKeyName: "p_pkey"},
			{StructName: "ch", Name: "ch"},
			{StructName: "r", Name: "r"},
			{StructName: "two", Name: "two", Checks: []string{"ref <> id"}},
			{StructName: "bookings", Name: "bookings"},
		},
		Fields: []schemamodel.Field{
			{StructName: "p", Name: "a", Type: "integer"},
			{StructName: "p", Name: "b", Type: "integer"},
			{StructName: "ch", Name: "a", Type: "integer", Nullable: true},
			{StructName: "ch", Name: "b", Type: "integer", Nullable: true},
			{StructName: "r", Name: "id", Type: "integer", Primary: true},
			{StructName: "two", Name: "id", Type: "integer", Primary: true},
			{
				StructName: "two", Name: "ref", Type: "integer", Nullable: true, Check: "ref > 0",
				Foreign: "r(id)", ForeignKeyName: "two_ref_a", Deferrable: true,
			},
			{StructName: "bookings", Name: "room", Type: "integer", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "ch", Name: "ch_ab_fk", Type: "FOREIGN KEY", Table: "ch",
				Columns: []string{"a", "b"}, ForeignTable: "p", ForeignColumns: []string{"a", "b"},
			},
			{
				StructName: "two", Name: "two_ref_b", Type: "FOREIGN KEY", Table: "two",
				Columns: []string{"ref"}, ForeignTable: "r", ForeignColumns: []string{"id"}, OnDelete: "CASCADE",
			},
			{StructName: "ch", Name: "ch_ab_uq", Type: "UNIQUE", Table: "ch", Columns: []string{"a", "b"}},
			{StructName: "two", Name: "two_id_positive", Type: "CHECK", Table: "two", CheckExpression: "id > 0"},
			{
				StructName: "bookings", Name: "bookings_room_excl", Type: "EXCLUDE", Table: "bookings",
				UsingMethod: "gist", ExcludeElements: "room WITH =",
			},
		},
	}
}

// TestRender_WritesEveryKeyDBMLCanSpell is stokaro/ptah#3917: DBML output kept
// a foreign key only when a column carried it and a primary key only when it
// was one column, and said nothing about the rest.
//
// DBML spells a composite primary key in Indexes, a composite foreign key as
// `table.(a, b)`, a multi-column UNIQUE as a unique Indexes entry, and checks
// in a Checks block or on the column.
func TestRender_WritesEveryKeyDBMLCanSpell(t *testing.T) {
	c := qt.New(t)

	result, err := renderDBML(c, keyedSchema(), dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.DBML, qt.Equals, `Table "bookings" {
  "room" integer
}

Table "ch" {
  "a" integer
  "b" integer

  Indexes {
    ("a", "b") [unique, name: "ch_ab_uq"]
  }
}

Table "p" {
  "a" integer [not null]
  "b" integer [not null]

  Indexes {
    ("a", "b") [pk, name: "p_pkey"]
  }
}

Table "r" {
  "id" integer [pk, not null]
}

Table "two" {
  "id" integer [pk, not null]
  "ref" integer [check: `+"`ref > 0`"+`]

  Checks {
    `+"`id > 0`"+` [name: "two_id_positive"]
    `+"`ref <> id`"+`
  }
}

Ref "ch_ab_fk": "ch".("a", "b") > "p".("a", "b")

Ref "two_ref_a": "two"."ref" > "r"."id"

Ref "two_ref_b": "two"."ref" > "r"."id" [delete: cascade]
`)
}

// TestRender_NamesWhatTheKeysCannotCarry holds the other half: an EXCLUDE has
// no DBML spelling, and neither has a key's DEFERRABLE, so both are named
// rather than dropped.
func TestRender_NamesWhatTheKeysCannotCarry(t *testing.T) {
	c := qt.New(t)

	result, err := renderDBML(c, keyedSchema(), dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Omitted, qt.DeepEquals, []string{
		"DEFERRABLE on keys (1)",
		"EXCLUDE constraints (1)",
	})
}

// TestRender_KeysReadBack holds the export to what Ptah reads: DBML written
// from a schema parses back to the same keys and checks. A composite key the
// reader refused, or a second reference it wrote over the first, would make the
// export a file Ptah cannot use.
//
// UNIQUE comes back as a unique index, since DBML writes the rule and not the
// kind of object, and an EXCLUDE does not come back at all, which is what
// Omitted names.
func TestRender_KeysReadBack(t *testing.T) {
	c := qt.New(t)
	rendered, err := renderDBML(c, keyedSchema(), dbmlrender.Options{})
	c.Assert(err, qt.IsNil)

	read, err := dbmlparse.Parse(rendered.DBML, dbmlparse.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(tableKeys(read.Tables), qt.DeepEquals, map[string]tableKey{
		"bookings": {},
		"ch":       {},
		"p":        {PrimaryKey: []string{"a", "b"}, PrimaryKeyName: "p_pkey"},
		"r":        {},
		"two":      {Checks: []string{"ref <> id"}},
	})
	c.Assert(read.Constraints, qt.DeepEquals, []schemamodel.Constraint{
		{StructName: "two", Name: "two_id_positive", Type: "CHECK", Table: "two", CheckExpression: "id > 0"},
		{
			StructName: "ch", Name: "ch_ab_fk", Type: "FOREIGN KEY", Table: "ch",
			Columns: []string{"a", "b"}, ForeignTable: "p", ForeignColumn: "a", ForeignColumns: []string{"a", "b"},
		},
		{
			StructName: "two", Name: "two_ref_b", Type: "FOREIGN KEY", Table: "two",
			Columns: []string{"ref"}, ForeignTable: "r", ForeignColumn: "id", ForeignColumns: []string{"id"},
			OnDelete: "CASCADE",
		},
	})
	ref := fieldNamed(read.Fields, "two", "ref")
	c.Assert(ref.Foreign, qt.Equals, "r(id)")
	c.Assert(ref.ForeignKeyName, qt.Equals, "two_ref_a")
	c.Assert(ref.Check, qt.Equals, "ref > 0")
	c.Assert(read.Indexes, qt.DeepEquals, []schemamodel.Index{
		{StructName: "ch", Name: "ch_ab_uq", Fields: []string{"a", "b"}, Unique: true},
	})
}

// tableKey is the part of a table this test reads back.
type tableKey struct {
	PrimaryKey     []string
	PrimaryKeyName string
	Checks         []string
}

func tableKeys(tables []schemamodel.Table) map[string]tableKey {
	keys := make(map[string]tableKey, len(tables))
	for _, table := range tables {
		keys[table.Name] = tableKey{
			PrimaryKey:     table.PrimaryKey,
			PrimaryKeyName: table.PrimaryKeyName,
			Checks:         table.Checks,
		}
	}
	return keys
}

func fieldNamed(fields []schemamodel.Field, structName, name string) schemamodel.Field {
	for _, field := range fields {
		if field.StructName == structName && field.Name == name {
			return field
		}
	}
	return schemamodel.Field{}
}
