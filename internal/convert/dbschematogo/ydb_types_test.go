package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/ydbtype"
)

// A column the YDB reader reports keeps its YDB type name as the field's
// declared type, and that name is its own declaration in internal/ydbtype, the
// map the renderer and the comparison read: a description converted to a
// model and rendered again builds the column it read.
//
// Each type is checked on the line that has it. The 64-bit date and time types
// and a Decimal other than (22,9) do not exist on 25.1, and the map refuses
// them there rather than reading them as another type.
func TestConvertDBSchemaToGoSchema_YDBTypeIsItsOwnDeclaration(t *testing.T) {
	tests := []struct {
		ydbType string
		caps    capability.Capabilities
	}{
		{ydbType: "Bool", caps: capability.YDB251()},
		{ydbType: "Int8", caps: capability.YDB251()},
		{ydbType: "Int16", caps: capability.YDB251()},
		{ydbType: "Int32", caps: capability.YDB251()},
		{ydbType: "Int64", caps: capability.YDB251()},
		{ydbType: "Uint8", caps: capability.YDB251()},
		{ydbType: "Uint16", caps: capability.YDB251()},
		{ydbType: "Uint32", caps: capability.YDB251()},
		{ydbType: "Uint64", caps: capability.YDB251()},
		{ydbType: "Float", caps: capability.YDB251()},
		{ydbType: "Double", caps: capability.YDB251()},
		{ydbType: "DyNumber", caps: capability.YDB251()},
		{ydbType: "String", caps: capability.YDB251()},
		{ydbType: "Utf8", caps: capability.YDB251()},
		{ydbType: "Json", caps: capability.YDB251()},
		{ydbType: "JsonDocument", caps: capability.YDB251()},
		{ydbType: "Yson", caps: capability.YDB251()},
		{ydbType: "Uuid", caps: capability.YDB251()},
		{ydbType: "Date", caps: capability.YDB251()},
		{ydbType: "Datetime", caps: capability.YDB251()},
		{ydbType: "Timestamp", caps: capability.YDB251()},
		{ydbType: "Interval", caps: capability.YDB251()},
		{ydbType: "Decimal(22,9)", caps: capability.YDB251()},
		{ydbType: "Date32", caps: capability.YDB262()},
		{ydbType: "Datetime64", caps: capability.YDB262()},
		{ydbType: "Timestamp64", caps: capability.YDB262()},
		{ydbType: "Interval64", caps: capability.YDB262()},
		{ydbType: "Decimal(35,10)", caps: capability.YDB262()},
	}

	for _, test := range tests {
		t.Run(test.ydbType, func(t *testing.T) {
			c := qt.New(t)
			column := catalog.Column{Name: "c", DataType: test.ydbType, ColumnType: test.ydbType, IsNullable: "YES"}
			db := &catalog.Database{Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{column}}}}

			model := dbschematogo.ConvertDBSchemaToGoSchema(db, platform.YDB)

			c.Assert(model.Fields, qt.HasLen, 1)
			c.Assert(model.Fields[0].Type, qt.Equals, test.ydbType)
			mapping, err := ydbtype.Map(model.Fields[0].Type, test.caps)
			c.Assert(err, qt.IsNil)
			c.Assert(mapping, qt.Equals, ydbtype.Mapping{Type: test.ydbType})
		})
	}
}

// A Serial column reads back as its integer type, filled from a sequence, and
// converts to an incrementing field of that type: the declaration the YDB
// renderer writes as the Serial type again.
func TestConvertDBSchemaToGoSchema_YDBSerialColumn(t *testing.T) {
	c := qt.New(t)
	column := catalog.Column{
		Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO",
		IsPrimaryKey: true, IsAutoIncrement: true,
	}
	db := &catalog.Database{Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{column}}}}

	model := dbschematogo.ConvertDBSchemaToGoSchema(db, platform.YDB)

	c.Assert(model.Fields, qt.HasLen, 1)
	c.Assert(model.Fields[0].Type, qt.Equals, "Int64")
	c.Assert(model.Fields[0].AutoInc, qt.IsTrue)
	c.Assert(model.Fields[0].Default, qt.Equals, "")
	serial, ok := ydbtype.SerialFor(model.Fields[0].Type)
	c.Assert(ok, qt.IsTrue)
	c.Assert(serial, qt.Equals, "BigSerial")
}
