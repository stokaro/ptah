package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// temporalHCL is the shape the public Atlas HCL reference gives temporal keys:
// without_overlaps on primary_key and unique, and period on foreign_key.
const temporalHCL = `
table "rooms" {
  column "id" {
    type = int
  }
  column "valid_at" {
    type = daterange
  }
  primary_key {
    columns = [column.id, column.valid_at]
    without_overlaps = true
  }
}
table "bookings" {
  column "room_id" {
    type = int
  }
  column "valid_at" {
    type = daterange
  }
  unique "bookings_uq" {
    columns = [column.room_id, column.valid_at]
    without_overlaps = true
  }
  foreign_key "bookings_room_fk" {
    columns = [column.room_id, column.valid_at]
    ref_columns = [table.rooms.column.id, table.rooms.column.valid_at]
    period = true
  }
}
`

// TestParse_TemporalKeys_HappyPath reads the three attributes onto the model
// (stokaro/ptah#6330).
func TestParse_TemporalKeys_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(t.Context(), []byte(temporalHCL), "schema.hcl")

	c.Assert(err, qt.IsNil)
	tables := make(map[string]bool)
	for _, table := range db.Tables {
		tables[table.Name] = table.PrimaryKeyWithoutOverlaps
	}
	c.Assert(tables, qt.DeepEquals, map[string]bool{"rooms": true, "bookings": false})
	flags := make(map[string][2]bool)
	for _, constraint := range db.Constraints {
		flags[constraint.Name] = [2]bool{constraint.WithoutOverlaps, constraint.Period}
	}
	c.Assert(flags, qt.DeepEquals, map[string][2]bool{"bookings_uq": {true, false}, "bookings_room_fk": {false, true}})
}

// TestParse_TemporalKeys_PlainKeysStayPlain is the control: the same document
// without the attributes reads no clause.
func TestParse_TemporalKeys_PlainKeysStayPlain(t *testing.T) {
	c := qt.New(t)
	plain := temporalHCLWithout("without_overlaps = true", "period = true")

	db, err := atlashcl.Parse(t.Context(), []byte(plain), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].PrimaryKeyWithoutOverlaps, qt.IsFalse)
	c.Assert(db.Tables[1].PrimaryKeyWithoutOverlaps, qt.IsFalse)
	c.Assert(db.Constraints[0].WithoutOverlaps, qt.IsFalse)
	c.Assert(db.Constraints[1].Period, qt.IsFalse)
}

// TestParse_TemporalKeys_GiSTTypeIsTheSameKey reads a temporal key that names
// its GiST index, as the public Atlas HCL reference allows, as the key that
// leaves the type out: the clause builds the index, so no method is recorded.
func TestParse_TemporalKeys_GiSTTypeIsTheSameKey(t *testing.T) {
	c := qt.New(t)
	typed := strings.NewReplacer(
		"    without_overlaps = true\n  }\n}\ntable \"bookings\"",
		"    without_overlaps = true\n    type = GIST\n  }\n}\ntable \"bookings\"",
		"    without_overlaps = true\n  }\n  foreign_key",
		"    without_overlaps = true\n    type = GIST\n  }\n  foreign_key",
	).Replace(temporalHCL)

	db, err := atlashcl.Parse(t.Context(), []byte(typed), "schema.hcl")
	untyped, untypedErr := atlashcl.Parse(t.Context(), []byte(temporalHCL), "schema.hcl")

	c.Assert(strings.Count(typed, "type = GIST"), qt.Equals, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(untypedErr, qt.IsNil)
	c.Assert(db.Tables[0].PrimaryKeyMethod, qt.Equals, "")
	c.Assert(db.Tables, qt.DeepEquals, untyped.Tables)
	c.Assert(db.Constraints, qt.DeepEquals, untyped.Constraints)
}

// TestParse_TemporalKeys_FailurePath refuses an attribute that is not a bool,
// a key of one column, and a `type` that disagrees with without_overlaps.
func TestParse_TemporalKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		hcl  string
		want string
	}{
		{
			name: "without_overlaps that is not a bool",
			hcl: singleColumnTable(`  unique "u" {
    columns = [column.a]
    without_overlaps = "yes"
  }`),
			want: `(?s).*unique attribute "without_overlaps" must be a bool.*`,
		},
		{
			name: "without_overlaps on one column of a unique",
			hcl: singleColumnTable(`  unique "u" {
    columns = [column.a]
    without_overlaps = true
  }`),
			want: `(?s).*unique "u" has without_overlaps and fewer than two columns.*`,
		},
		{
			name: "without_overlaps on one column of a primary_key",
			hcl: singleColumnTable(`  primary_key {
    columns = [column.a]
    without_overlaps = true
  }`),
			want: `(?s).*primary_key has without_overlaps and fewer than two columns.*`,
		},
		{
			name: "type GIST on a plain primary_key",
			hcl: twoColumnTable(`  primary_key {
    columns = [column.a, column.b]
    type = GIST
  }`),
			want: `(?s).*primary_key has type GIST and no without_overlaps; PostgreSQL backs only a temporal key with a GiST index.*`,
		},
		{
			name: "type BTREE on a temporal primary_key",
			hcl: twoColumnTable(`  primary_key {
    columns = [column.a, column.b]
    without_overlaps = true
    type = BTREE
  }`),
			want: `(?s).*primary_key has without_overlaps and type BTREE; a temporal key is backed by a GiST index.*`,
		},
		{
			name: "type GIST on a plain unique",
			hcl: twoColumnTable(`  unique "u" {
    columns = [column.a, column.b]
    type = GIST
  }`),
			want: `(?s).*unique "u" has type GIST and no without_overlaps; PostgreSQL backs only a temporal key with a GiST index.*`,
		},
		{
			name: "type BTREE on a unique",
			hcl: twoColumnTable(`  unique "u" {
    columns = [column.a, column.b]
    without_overlaps = true
    type = BTREE
  }`),
			want: `(?s).*unsupported unique type "BTREE".*`,
		},
		{
			name: "period on one column",
			hcl: singleColumnTable(`  foreign_key "f" {
    columns = [column.a]
    ref_columns = [table.t.column.a]
    period = true
  }`),
			want: `(?s).*foreign_key "f" has period and fewer than two columns.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse(t.Context(), []byte(test.hcl), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// temporalHCLWithout is temporalHCL with each attribute line removed.
func temporalHCLWithout(attributes ...string) string {
	document := temporalHCL
	for _, attribute := range attributes {
		document = strings.ReplaceAll(document, "    "+attribute+"\n", "")
	}
	return document
}

// singleColumnTable is a table t of one column a, with body inside it.
func singleColumnTable(body string) string {
	return "table \"t\" {\n  column \"a\" {\n    type = int\n  }\n" + body + "\n}\n"
}

// twoColumnTable is a table t of columns a and b, with body inside it.
func twoColumnTable(body string) string {
	return "table \"t\" {\n  column \"a\" {\n    type = int\n  }\n  column \"b\" {\n    type = daterange\n  }\n" +
		body + "\n}\n"
}
