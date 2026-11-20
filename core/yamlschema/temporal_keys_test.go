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

// TestParse_TemporalKeys_ReadsAYAMLBoolean reads a temporal flag as every
// boolean key of a schema is read, YAML 1.1 spellings included, and an absent
// flag as false.
func TestParse_TemporalKeys_ReadsAYAMLBoolean(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "yes", value: "    without_overlaps: yes\n", want: true},
		{name: "on", value: "    without_overlaps: on\n", want: true},
		{name: "True", value: "    without_overlaps: True\n", want: true},
		{name: "no", value: "    without_overlaps: no\n", want: false},
		{name: "absent", value: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(t.Context(), yamlext.None(),
				[]byte("constraints:\n  u:\n    table: t\n    type: UNIQUE\n    columns: [a, b]\n"+test.value))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Constraints, qt.HasLen, 1)
			c.Assert(db.Constraints[0].WithoutOverlaps, qt.Equals, test.want)
		})
	}
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
			name: "without_overlaps with no value",
			yaml: "constraints:\n  u:\n    table: t\n    type: UNIQUE\n    columns: [a, b]\n    without_overlaps:\n",
			want: `(?s).*constraint "u" has without_overlaps with no value; write true or false`,
		},
		{
			name: "period written as null",
			yaml: "constraints:\n  f:\n    table: t\n    type: FOREIGN KEY\n    columns: [a, b]\n    foreign_table: p\n" +
				"    foreign_columns: [a, b]\n    period: ~\n",
			want: `(?s).*constraint "f" has period with no value; write true or false`,
		},
		{
			name: "primary_key_without_overlaps written as null",
			yaml: "tables:\n  t:\n    primary_key: [id, valid_at]\n    primary_key_without_overlaps: null\n" +
				"    columns:\n      id: {type: INTEGER}\n      valid_at: {type: DATERANGE}\n",
			want: `(?s).*table "t" has primary_key_without_overlaps with no value; write true or false`,
		},
		{
			name: "period aliased to a null anchor",
			yaml: "constraints:\n  f:\n    table: t\n    type: FOREIGN KEY\n    columns: [a, b]\n    foreign_table: p\n" +
				"    comment: &n null\n    foreign_columns: [a, b]\n    period: *n\n",
			want: `(?s).*constraint "f" has period with no value; write true or false`,
		},
		{
			name: "period with an action other than NO ACTION",
			yaml: "constraints:\n  f:\n    table: t\n    type: FOREIGN KEY\n    columns: [a, b]\n    foreign_table: p\n" +
				"    foreign_columns: [a, b]\n    on_update: SET DEFAULT\n    period: true\n",
			want: `(?s).*constraint "f" has period and ON UPDATE SET DEFAULT; a PERIOD foreign key takes only NO ACTION`,
		},
		{
			name: "both foreign_column and foreign_columns",
			yaml: "constraints:\n  f:\n    table: t\n    type: FOREIGN KEY\n    columns: [a]\n    foreign_table: p\n" +
				"    foreign_column: x\n    foreign_columns: [y]\n",
			want: `(?s).*constraint "f" sets both foreign_column and foreign_columns; write one`,
		},
		{
			name: "foreign_columns of another length than columns",
			yaml: "constraints:\n  f:\n    table: t\n    type: FOREIGN KEY\n    columns: [a, b]\n    foreign_table: p\n" +
				"    foreign_columns: [x]\n",
			want: `(?s).*constraint "f" has 2 columns and 1 foreign_columns; each column needs one referenced column`,
		},
		{
			name: "foreign_column for two columns",
			yaml: "constraints:\n  f:\n    table: t\n    type: FOREIGN KEY\n    columns: [a, b]\n    foreign_table: p\n" +
				"    foreign_column: x\n",
			want: `(?s).*constraint "f" has 2 columns and one foreign_column; each column needs one referenced column`,
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
