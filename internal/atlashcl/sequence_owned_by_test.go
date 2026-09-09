package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// ownedByDocument renders a schema whose sequence names its owning column with
// the given expression.
func ownedByDocument(ownedBy string) []byte {
	return []byte(`
schema "public" {}

table "rooms" {
  schema = schema.public
  column "id" { type = int }
}

sequence "s" {
  schema   = schema.public
  owned_by = ` + ownedBy + `
}
`)
}

// TestParseSequenceOwnedByReadsBothSpellings_HappyPath pins that a sequence
// names its owning column the same way every other column-naming attribute in
// this format does.
//
// The reference form went through the string reader, which stringifies a
// traversal whole, so the renderer split `table.rooms.column.id` on its dots and
// emitted a four-part name PostgreSQL refuses as an improper qualified name. The
// document was accepted at exit 0 and the corruption surfaced only against a
// server (stokaro/ptah#3121).
func TestParseSequenceOwnedByReadsBothSpellings_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		ownedBy string
	}{
		{name: "reference", ownedBy: "table.rooms.column.id"},
		{name: "parenthesised reference", ownedBy: "(table.rooms.column.id)"},
		{name: "string", ownedBy: `"rooms.id"`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(ownedByDocument(row.ownedBy), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Sequences, qt.HasLen, 1)
			c.Assert(db.Sequences[0].OwnedBy, qt.Equals, "rooms.id")
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, `OWNED BY "rooms"."id"`)
			c.Assert(sql, qt.Not(qt.Contains), `"table"."rooms"`)
		})
	}
}

// TestParseSequenceOwnedByKeepsALiteralItCannotResolve_HappyPath is the control
// for the reader above.
//
// A value that names no column has to reach the renderer unchanged, or reading
// the reference form would be bought by dropping every literal spelling that
// already worked.
func TestParseSequenceOwnedByKeepsALiteralItCannotResolve_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		ownedBy string
		want    string
	}{
		{name: "schema qualified string", ownedBy: `"app.orders.id"`, want: "app.orders.id"},
		{name: "bare table", ownedBy: `"orders"`, want: "orders"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(ownedByDocument(row.ownedBy), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Sequences, qt.HasLen, 1)
			c.Assert(db.Sequences[0].OwnedBy, qt.Equals, row.want)
		})
	}
}
