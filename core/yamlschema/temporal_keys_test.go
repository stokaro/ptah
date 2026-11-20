package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// TestParse_TemporalKeys_HappyPath reads the temporal clauses off a YAML
// document, with the composite foreign_columns a PERIOD key needs
// (stokaro/ptah#6330).
func TestParse_TemporalKeys_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(t.Context(), yamlext.None(), []byte(`tables:
  rooms:
    primary_key: [id, valid_at]
    primary_key_without_overlaps: true
    columns:
      id: {type: INTEGER}
      valid_at: {type: DATERANGE}
  bookings:
    columns:
      room_id: {type: INTEGER}
      valid_at: {type: DATERANGE}
    constraints:
      bookings_uq:
        type: UNIQUE
        columns: [room_id, valid_at]
        without_overlaps: true
      bookings_room_fk:
        type: FOREIGN KEY
        columns: [room_id, valid_at]
        foreign_table: rooms
        foreign_columns: [id, valid_at]
        period: true
`))

	c.Assert(err, qt.IsNil)
	tables := make(map[string]bool)
	for _, table := range db.Tables {
		tables[table.Name] = table.PrimaryKeyWithoutOverlaps
	}
	c.Assert(tables, qt.DeepEquals, map[string]bool{"rooms": true, "bookings": false})
	c.Assert(db.Constraints, qt.HasLen, 2)
	flags := make(map[string][2]bool)
	foreignColumns := make(map[string][]string)
	for _, constraint := range db.Constraints {
		flags[constraint.Name] = [2]bool{constraint.WithoutOverlaps, constraint.Period}
		foreignColumns[constraint.Name] = constraint.ForeignColumns
	}
	c.Assert(flags, qt.DeepEquals, map[string][2]bool{"bookings_uq": {true, false}, "bookings_room_fk": {false, true}})
	c.Assert(foreignColumns["bookings_room_fk"], qt.DeepEquals, []string{"id", "valid_at"})
}

// TestParse_TemporalKeys_FailurePath refuses the clauses on the wrong kind of
// constraint, on a key too short to carry them, and with a value that is not a
// boolean.
func TestParse_TemporalKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "period on a unique constraint",
			yaml: "constraints:\n  u:\n    table: t\n    type: UNIQUE\n    columns: [a, b]\n    period: true\n",
			want: `(?s).*constraint "u" is a UNIQUE, and period belongs to a FOREIGN KEY constraint`,
		},
		{
			name: "without_overlaps on one column",
			yaml: "constraints:\n  u:\n    table: t\n    type: UNIQUE\n    columns: [valid_at]\n    without_overlaps: true\n",
			want: `(?s).*constraint "u" has without_overlaps and fewer than two columns`,
		},
		{
			name: "primary_key_without_overlaps on one column",
			yaml: "tables:\n  t:\n    primary_key: [valid_at]\n    primary_key_without_overlaps: true\n    columns:\n      valid_at: {type: DATERANGE}\n",
			want: `(?s).*table "t" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`,
		},
		{
			name: "without_overlaps that is not a boolean",
			yaml: "constraints:\n  u:\n    table: t\n    type: UNIQUE\n    columns: [a, b]\n    without_overlaps: maybe\n",
			want: `(?s).*cannot unmarshal !!str .maybe. into bool.*`,
		},
		{
			name: "a foreign key naming neither foreign_column nor foreign_columns",
			yaml: "constraints:\n  f:\n    table: t\n    type: FOREIGN KEY\n    columns: [a, b]\n    foreign_table: p\n",
			want: `(?s).*constraint "f" requires foreign_table and foreign_column or foreign_columns`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(t.Context(), yamlext.None(), []byte(test.yaml))

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
