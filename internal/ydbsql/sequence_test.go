package ydbsql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

const yqlSerialTable = "CREATE TABLE `app/orders` (id BigSerial NOT NULL,note Utf8,PRIMARY KEY(id));"

func readYQLSerial(c *qt.C, source, root string) schemamodel.Database {
	c.Helper()
	load, err := readYQLLoad(c, root, source)
	c.Assert(err, qt.IsNil)
	return load.Database
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
			load, err := readYQLLoad(c, tc.root, tc.source)
			c.Assert(err, qt.IsNotNil)
			c.Assert(load.Database.Tables, qt.HasLen, 0)
		})
	}
}
