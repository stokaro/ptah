package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// temporalSchema is a table with a WITHOUT OVERLAPS primary key and a child
// table with a WITHOUT OVERLAPS unique constraint and a PERIOD foreign key.
func temporalSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Room", Name: "rooms", PrimaryKey: []string{"id", "valid_at"}, PrimaryKeyWithoutOverlaps: true},
			{StructName: "Booking", Name: "bookings"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Room", FieldName: "ID", Name: "id", Type: "INTEGER"},
			{StructName: "Room", FieldName: "ValidAt", Name: "valid_at", Type: "DATERANGE"},
			{StructName: "Booking", FieldName: "RoomID", Name: "room_id", Type: "INTEGER"},
			{StructName: "Booking", FieldName: "ValidAt", Name: "valid_at", Type: "DATERANGE"},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Booking", Name: "bookings_uq", Type: "UNIQUE", Table: "bookings",
				Columns: []string{"room_id", "valid_at"}, WithoutOverlaps: true,
			},
			{
				StructName: "Booking", Name: "bookings_room_fk", Type: "FOREIGN KEY", Table: "bookings",
				Columns: []string{"room_id", "valid_at"}, ForeignTable: "rooms",
				ForeignColumn: "id", ForeignColumns: []string{"id", "valid_at"}, Period: true,
			},
		},
	}
}

// TestRender_TemporalKeys keeps a temporal key through the Go source: the
// annotations carry the clauses, and reading the source back gives the model it
// came from (stokaro/ptah#6330).
func TestRender_TemporalKeys(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(c.Context(), temporalSchema(),
		goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	reparsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", files[0].Data)

	c.Assert(err, qt.IsNil)
	source := string(files[0].Data)
	c.Assert(source, qt.Contains, `primary_key="id,valid_at" primary_key_without_overlaps="true"`)
	c.Assert(source, qt.Contains, `columns="room_id,valid_at" without_overlaps="true"`)
	c.Assert(source, qt.Contains, `foreign_columns="id,valid_at" period="true"`)
	tables := make(map[string]bool)
	for _, table := range reparsed.Tables {
		tables[table.Name] = table.PrimaryKeyWithoutOverlaps
	}
	c.Assert(tables, qt.DeepEquals, map[string]bool{"rooms": true, "bookings": false})
	flags := make(map[string][2]bool)
	for _, constraint := range reparsed.Constraints {
		flags[constraint.Name] = [2]bool{constraint.WithoutOverlaps, constraint.Period}
	}
	c.Assert(flags, qt.DeepEquals, map[string][2]bool{"bookings_uq": {true, false}, "bookings_room_fk": {false, true}})
}
