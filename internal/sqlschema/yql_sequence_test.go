package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

const yqlSerialTable = "CREATE TABLE `app/orders` (id BigSerial NOT NULL,note Utf8,PRIMARY KEY(id));"

func readYQLSerial(c *qt.C, source, root string) schemamodel.Database {
	c.Helper()
	document := sqlschema.NewDocument(nil)
	document.YDBDatabasePath = root
	database, _, err := sqlschema.ReadOnto([]byte(source), "ydb", document)
	c.Assert(err, qt.IsNil)
	return database
}

func TestReadYQLSerialSequence(t *testing.T) {
	for _, clause := range []string{"START WITH 100 INCREMENT BY 5", "INCREMENT 5 START 100"} {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)
			database := readYQLSerial(c, yqlSerialTable+"ALTER SEQUENCE `/local/app/orders/_serial_column_id` "+clause+";", "/local")
			c.Assert(database.Sequences, qt.HasLen, 0)
			c.Assert(database.Fields, qt.HasLen, 2)
			c.Assert(database.Fields[0].IdentityStart, qt.Equals, "100")
			c.Assert(database.Fields[0].IdentityIncrement, qt.Equals, "5")
			c.Assert(database.Fields[0].IdentityGeneration, qt.Equals, "")
			c.Assert(database.Fields[0].AutoInc, qt.IsFalse)
			c.Assert(database.DatabasePath, qt.Equals, "")
		})
	}
}

func TestReadYQLSerialSequencePreservesOtherSettings(t *testing.T) {
	c := qt.New(t)
	source := yqlSerialTable + "ALTER SEQUENCE `/Root/db/app/orders/_serial_column_id` START WITH 100 INCREMENT BY 5; ALTER SEQUENCE `/Root/db/app/orders/_serial_column_id` INCREMENT BY 7;"
	database := readYQLSerial(c, source, "/Root/db")
	c.Assert(database.Fields[0].IdentityStart, qt.Equals, "100")
	c.Assert(database.Fields[0].IdentityIncrement, qt.Equals, "7")
}

func TestReadYQLSerialSequenceKeepsTableAndColumnIdentity(t *testing.T) {
	c := qt.New(t)
	source := "CREATE TABLE `one/orders.v2` (`key.id` BigSerial NOT NULL,PRIMARY KEY(`key.id`)); CREATE TABLE `two/orders.v2` (`key.id` BigSerial NOT NULL,PRIMARY KEY(`key.id`)); ALTER SEQUENCE `/local/two/orders.v2/_serial_column_key.id` INCREMENT 9;"
	database := readYQLSerial(c, source, "/local")
	c.Assert(database.Fields, qt.HasLen, 2)
	c.Assert(database.Fields[0].IdentityIncrement, qt.Equals, "")
	c.Assert(database.Fields[1].IdentityIncrement, qt.Equals, "9")
}

func TestReadYQLSerialSequenceRefusals(t *testing.T) {
	for _, tc := range []struct{ name, source, root string }{
		{"missing root", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` START 2;", ""},
		{"another database", yqlSerialTable + "ALTER SEQUENCE `/other/app/orders/_serial_column_id` START 2;", "/local"},
		{"root prefix collision", yqlSerialTable + "ALTER SEQUENCE `/locality/app/orders/_serial_column_id` START 2;", "/local"},
		{"relative sequence", yqlSerialTable + "ALTER SEQUENCE `app/orders/_serial_column_id` START 2;", "/local"},
		{"missing table", "ALTER SEQUENCE `/local/app/orders/_serial_column_id` START 2;", "/local"},
		{"ordinary column", "CREATE TABLE `app/orders` (id Int64 NOT NULL,PRIMARY KEY(id)); ALTER SEQUENCE `/local/app/orders/_serial_column_id` START 2;", "/local"},
		{"unknown sequence", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/other` START 2;", "/local"},
		{"directory traversal", yqlSerialTable + "ALTER SEQUENCE `/local/app/../app/orders/_serial_column_id` START 2;", "/local"},
		{"restart", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` START 2 RESTART WITH 2;", "/local"},
		{"unmodeled bound", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` MAXVALUE 10;", "/local"},
		{"duplicate start", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` START 2 START 3;", "/local"},
		{"duplicate increment", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` INCREMENT 2 INCREMENT 3;", "/local"},
		{"zero", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` INCREMENT 0;", "/local"},
		{"negative", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` START -1;", "/local"},
		{"overflow", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id` START 9223372036854775808;", "/local"},
		{"empty", yqlSerialTable + "ALTER SEQUENCE `/local/app/orders/_serial_column_id`;", "/local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			document := sqlschema.NewDocument(nil)
			document.YDBDatabasePath = tc.root
			database, statements, err := sqlschema.ReadOnto([]byte(tc.source), "ydb", document)
			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// A sequence setting must not replace an earlier identity policy. In
// particular, GENERATED ALWAYS remains available for the planner to refuse.
func TestReadYQLSequenceKeepsEarlierIdentityPolicy(t *testing.T) {
	for _, generation := range []string{"BY_DEFAULT", "ALWAYS"} {
		t.Run(generation, func(t *testing.T) {
			c := qt.New(t)
			earlier := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "orders", Schema: "app", StructName: "Order"}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "Order", Type: "Int64", Primary: true, IdentityGeneration: generation}},
			}
			document := sqlschema.NewDocument(earlier)
			document.YDBDatabasePath = "/local"
			_, _, err := sqlschema.ReadOnto([]byte("ALTER SEQUENCE `/local/app/orders/_serial_column_id` INCREMENT 5;"), "ydb", document)
			c.Assert(err, qt.IsNil)
			c.Assert(earlier.Fields[0].IdentityGeneration, qt.Equals, generation)
			c.Assert(earlier.Fields[0].AutoInc, qt.IsFalse)
			c.Assert(earlier.Fields[0].IdentityIncrement, qt.Equals, "5")
		})
	}
}
