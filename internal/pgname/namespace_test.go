package pgname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
	"ptah.run/internal/sqlschema"
)

const ownedConstraintKind schemaext.Kind = "example.org/constraint"

type ownedConstraint struct{ indexed bool }

func (*ownedConstraint) Kind() schemaext.Kind     { return ownedConstraintKind }
func (v *ownedConstraint) Clone() schemaext.Value { return new(*v) }
func (v *ownedConstraint) Equal(other schemaext.Value) bool {
	o, ok := other.(*ownedConstraint)
	return ok && v.indexed == o.indexed
}
func (v *ownedConstraint) DefinedConstraint(ref objectidentity.ID) schemaext.DefinedConstraint {
	return schemaext.DefinedConstraint{Ref: ref, Indexed: v.indexed}
}

func constraintProjection(c *qt.C, table, name string, indexed bool) schemaext.Objects {
	c.Helper()
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Constraint(table, name)
	ref.Kind = objectidentity.Kind(ownedConstraintKind)
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &ownedConstraint{indexed: indexed}})
	c.Assert(err, qt.IsNil)
	return objects
}

func TestOwnedConstraintNamespaces(t *testing.T) {
	for _, test := range []struct {
		name, table, constraint, schema, stored string
		indexed, inSchema                       bool
	}{
		{name: "default schema", table: "rooms", constraint: "rooms_room_key", stored: "rooms_room_key", indexed: true, inSchema: true},
		{name: "explicit schema", table: "app.rooms", constraint: "rooms_room_key", schema: "app", stored: "rooms_room_key", indexed: true, inSchema: true},
		{name: "another schema", table: "app.rooms", constraint: "rooms_room_key", schema: "other", stored: "rooms_room_key", indexed: true},
		{name: "without backing index", table: "rooms", constraint: "rooms_room_key", stored: "rooms_room_key", inSchema: true},
		{name: "quoted dots", table: `"App.One"."Rooms.Two"`, constraint: `"Rooms.Two_Room_key"`, schema: "App.One", stored: "Rooms.Two_Room_key", indexed: true, inSchema: true},
		{name: "escaped quote", table: `"App""One".rooms`, constraint: `"Room""Key"`, schema: `App"One`, stored: `Room"Key`, indexed: true, inSchema: true},
		{name: "unquoted folding", table: "APP.rooms", constraint: "ROOM_KEY", schema: "app", stored: "room_key", indexed: true, inSchema: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{FeatureObjects: constraintProjection(c, test.table, test.constraint, test.indexed)}
			constraints := pgname.ConstraintNames([]*schemamodel.Database{nil, database}, test.schema)
			relations := pgname.RelationNames([]*schemamodel.Database{nil, database}, test.schema)
			c.Assert(constraints.Taken(test.stored), qt.Equals, test.inSchema)
			c.Assert(relations.Taken(test.stored), qt.Equals, test.inSchema && test.indexed)
		})
	}
}

func TestReadOnto_AnOwnedConstraintReservesADerivedName(t *testing.T) {
	for _, test := range []struct {
		name, table, constraint, sql, want string
	}{
		{name: "unquoted", table: "earlier", constraint: "rooms_room_key", sql: "CREATE TABLE rooms (room int, UNIQUE (room));", want: "rooms_room_key1"},
		{name: "quoted schema and name", table: `"App.One".earlier`, constraint: `"Rooms.Two_Room_key"`, sql: `CREATE TABLE "App.One"."Rooms.Two" ("Room" int, UNIQUE ("Room"));`, want: "Rooms.Two_Room_key1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			previous, _, err := sqlschema.Read([]byte("CREATE TABLE "+test.table+" (id int);"), "postgres")
			c.Assert(err, qt.IsNil)
			previous.FeatureObjects = constraintProjection(c, test.table, test.constraint, true)
			document := sqlschema.NewDocument(&previous)
			got, _, err := sqlschema.ReadOnto([]byte(test.sql), "postgres", document)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Constraints, qt.HasLen, 1)
			c.Assert(got.Constraints[0].Name, qt.Equals, test.want)
		})
	}
}
