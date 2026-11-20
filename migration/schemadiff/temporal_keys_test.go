package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// temporalDesired declares `rooms (id, valid_at)` keyed on both columns and a
// `bookings (room_id, valid_at)` with a unique constraint and a foreign key
// over the same two. The flags decide which of the three carry the temporal
// clause.
func temporalDesired(primary, unique, period bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Room", Name: "rooms", PrimaryKey: []string{"id", "valid_at"}, PrimaryKeyWithoutOverlaps: primary},
			{StructName: "Booking", Name: "bookings"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Room", Name: "id", Type: "integer"},
			{StructName: "Room", Name: "valid_at", Type: "daterange"},
			{StructName: "Booking", Name: "room_id", Type: "integer", Nullable: true},
			{StructName: "Booking", Name: "valid_at", Type: "daterange", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Booking", Table: "bookings", Name: "bookings_uq", Type: "UNIQUE",
				Columns: []string{"room_id", "valid_at"}, WithoutOverlaps: unique,
			},
			{
				StructName: "Booking", Table: "bookings", Name: "bookings_room_fk", Type: "FOREIGN KEY",
				Columns: []string{"room_id", "valid_at"}, ForeignTable: "rooms", ForeignColumn: "id",
				ForeignColumns: []string{"id", "valid_at"}, Period: period,
			},
		},
	}
}

// temporalLive is the same pair as PostgreSQL 18.6 reports it.
func temporalLive(primary, unique, period bool) *catalog.Database {
	parent, column, action := "rooms", "id", "NO ACTION"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "rooms", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "valid_at", DataType: "daterange", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "bookings", Columns: []catalog.Column{
				{Name: "room_id", DataType: "integer", IsNullable: "YES"},
				{Name: "valid_at", DataType: "daterange", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{
			{
				Name: "rooms_pkey", TableName: "rooms", Type: "PRIMARY KEY", ColumnName: "id",
				ColumnNames: []string{"id", "valid_at"}, WithoutOverlaps: primary,
			},
			{
				Name: "bookings_uq", TableName: "bookings", Type: "UNIQUE", ColumnName: "room_id",
				ColumnNames: []string{"room_id", "valid_at"}, WithoutOverlaps: unique,
			},
			{
				Name: "bookings_room_fk", TableName: "bookings", Type: "FOREIGN KEY", ColumnName: "room_id",
				ColumnNames: []string{"room_id", "valid_at"}, ForeignTable: &parent, ForeignColumn: &column,
				ForeignColumns: []string{"id", "valid_at"}, DeleteRule: &action, UpdateRule: &action, Period: period,
			},
		},
	}
}

// TestCompare_TemporalKeys_Synced pairs a declaration with the keys the server
// built from it, temporal clause and all (stokaro/ptah#6330).
func TestCompare_TemporalKeys_Synced(t *testing.T) {
	tests := []struct {
		name                     string
		primary, unique, periods bool
	}{
		{name: "every key temporal", primary: true, unique: true, periods: true},
		{name: "no key temporal"},
		{name: "only the primary key", primary: true},
		{name: "only the unique constraint", unique: true},
		{name: "only the foreign key", periods: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.Postgres,
				temporalDesired(test.primary, test.unique, test.periods),
				temporalLive(test.primary, test.unique, test.periods))

			c.Assert(changedConstraints(diff), qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
		})
	}
}

// TestCompare_TemporalKeys_Changed plans a drop and an add of a key whose
// temporal clause differs from the one the database holds, and the addition
// carries the clause. PostgreSQL 18.6 cannot turn a key into a temporal one in
// place, so a change of the flag is a constraint replacement.
func TestCompare_TemporalKeys_Changed(t *testing.T) {
	tests := []struct {
		name         string
		desired      *schemamodel.Database
		live         *catalog.Database
		wantChanged  []string
		wantAdded    string
		wantOverlaps bool
		wantPeriod   bool
	}{
		{
			name: "a unique constraint becomes temporal", desired: temporalDesired(false, true, false),
			live: temporalLive(false, false, false), wantChanged: []string{"+bookings_uq", "-bookings_uq"},
			wantAdded: "bookings_uq", wantOverlaps: true,
		},
		{
			name: "a temporal unique constraint becomes plain", desired: temporalDesired(false, false, false),
			live: temporalLive(false, true, false), wantChanged: []string{"+bookings_uq", "-bookings_uq"},
			wantAdded: "bookings_uq",
		},
		{
			name: "a primary key becomes temporal", desired: temporalDesired(true, false, false),
			live: temporalLive(false, false, false),
			// The foreign key rests on the key, so the plan lifts it for the
			// replacement and puts it back.
			wantChanged: []string{"+bookings_room_fk", "+rooms_pkey", "-bookings_room_fk", "-rooms_pkey"},
			wantAdded:   "rooms_pkey", wantOverlaps: true,
		},
		{
			name: "a foreign key takes PERIOD", desired: temporalDesired(false, false, true),
			live: temporalLive(false, false, false), wantChanged: []string{"+bookings_room_fk", "-bookings_room_fk"},
			wantAdded: "bookings_room_fk", wantPeriod: true,
		},
		{
			name: "a foreign key drops PERIOD", desired: temporalDesired(false, false, false),
			live: temporalLive(false, false, true), wantChanged: []string{"+bookings_room_fk", "-bookings_room_fk"},
			wantAdded: "bookings_room_fk",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.Postgres, test.desired, test.live)

			c.Assert(changedConstraints(diff), qt.ContentEquals, test.wantChanged)
			c.Assert(added(diff, test.wantAdded).WithoutOverlaps, qt.Equals, test.wantOverlaps)
			c.Assert(added(diff, test.wantAdded).Period, qt.Equals, test.wantPeriod)
		})
	}
}

// TestCompare_TemporalKeys_ThePeriodColumnIsTheLastOne plans a replacement when
// the same two columns are keyed with the range first. The columns of a key
// compare as a set; the last column of a temporal key is its period, so the set
// alone would call the two keys equal.
func TestCompare_TemporalKeys_ThePeriodColumnIsTheLastOne(t *testing.T) {
	c := qt.New(t)
	live := temporalLive(false, true, false)
	live.Constraints[1].ColumnNames = []string{"valid_at", "room_id"}

	diff := compareForDialect(platform.Postgres, temporalDesired(false, true, false), live)

	c.Assert(changedConstraints(diff), qt.ContentEquals, []string{"+bookings_uq", "-bookings_uq"})
}

// added answers the addition a diff carries for the named constraint.
func added(diff *difftypes.SchemaDiff, name string) difftypes.ConstraintAdditionInfo {
	for _, addition := range diff.ConstraintsAdded {
		if addition.Name == name {
			return addition
		}
	}
	return difftypes.ConstraintAdditionInfo{}
}
