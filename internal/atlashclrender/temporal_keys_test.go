package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// temporalDatabase is a room table with a WITHOUT OVERLAPS primary key, and a
// booking table with a WITHOUT OVERLAPS unique constraint, a named WITHOUT
// OVERLAPS primary key constraint on a third table, and a PERIOD foreign key.
func temporalDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Room", Name: "rooms", PrimaryKey: []string{"id", "valid_at"}, PrimaryKeyWithoutOverlaps: true},
			{StructName: "Booking", Name: "bookings"},
			{StructName: "Stay", Name: "stays"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Room", Name: "id", Type: "INTEGER"},
			{StructName: "Room", Name: "valid_at", Type: "DATERANGE"},
			{StructName: "Booking", Name: "room_id", Type: "INTEGER"},
			{StructName: "Booking", Name: "valid_at", Type: "DATERANGE"},
			{StructName: "Stay", Name: "id", Type: "INTEGER"},
			{StructName: "Stay", Name: "valid_at", Type: "DATERANGE"},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Booking", Table: "bookings", Name: "bookings_uq", Type: "UNIQUE",
				Columns: []string{"room_id", "valid_at"}, WithoutOverlaps: true,
			},
			{
				StructName: "Booking", Table: "bookings", Name: "bookings_room_fk", Type: "FOREIGN KEY",
				Columns: []string{"room_id", "valid_at"}, ForeignTable: "rooms",
				ForeignColumns: []string{"id", "valid_at"}, Period: true,
			},
			{
				StructName: "Stay", Table: "stays", Name: "stays_pk", Type: "PRIMARY KEY",
				Columns: []string{"id", "valid_at"}, WithoutOverlaps: true,
			},
		},
	}
}

// TestRender_TemporalKeys_HappyPath writes the attributes the public Atlas HCL
// reference gives temporal keys, and reads the document back to the model it
// came from (stokaro/ptah#6330).
func TestRender_TemporalKeys_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := temporalDatabase()
	schemamodel.Finalize(database)

	rendered, err := atlashclrender.Render(database, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil)
	document := string(rendered.Data)
	c.Assert(document, qt.Contains, "primary_key {\n    columns = [column.id, column.valid_at]\n    without_overlaps = true\n  }")
	c.Assert(document, qt.Contains, "unique \"bookings_uq\" {\n    columns = [column.room_id, column.valid_at]\n    without_overlaps = true\n  }")
	c.Assert(document, qt.Contains, "    period = true\n")
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	parsed, err := atlashcl.ParseWithOptions(t.Context(), rendered.Data, "schema.hcl",
		atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", document))
	tables := make(map[string]bool)
	for _, table := range parsed.Tables {
		tables[table.Name] = table.PrimaryKeyWithoutOverlaps
	}
	c.Assert(tables, qt.DeepEquals, map[string]bool{"rooms": true, "bookings": false, "stays": false})
	flags := make(map[string][2]bool)
	for _, constraint := range parsed.Constraints {
		flags[constraint.Name] = [2]bool{constraint.WithoutOverlaps, constraint.Period}
	}
	c.Assert(flags["bookings_uq"], qt.Equals, [2]bool{true, false})
	c.Assert(flags["bookings_room_fk"], qt.Equals, [2]bool{false, true})
	c.Assert(flags["stays_pk"], qt.Equals, [2]bool{true, false})
}

// TestRender_TemporalKeys_PlainKeysWriteNoAttribute is the control: the same
// tables without the flags write neither attribute.
func TestRender_TemporalKeys_PlainKeysWriteNoAttribute(t *testing.T) {
	c := qt.New(t)
	database := temporalDatabase()
	database.Tables[0].PrimaryKeyWithoutOverlaps = false
	database.Constraints[0].WithoutOverlaps = false
	database.Constraints[1].Period = false
	database.Constraints[2].WithoutOverlaps = false
	schemamodel.Finalize(database)

	rendered, err := atlashclrender.Render(database, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "without_overlaps")
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "period")
}
