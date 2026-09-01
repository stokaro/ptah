package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/modelast"
)

// identityField is an auto-incrementing column carrying the given range and no
// generation mode, which is how SQL Server's is spelled.
func identityField(start, increment string) schemamodel.Field {
	return schemamodel.Field{
		StructName:        "Orders",
		Name:              "id",
		Type:              "INT",
		Primary:           true,
		AutoInc:           true,
		IdentityStart:     start,
		IdentityIncrement: increment,
	}
}

// TestFromField_CarriesTheIdentityRangeOfAnAutoIncrementColumn pins that a
// range reaches the AST without a generation mode beside it.
//
// The range was set only inside `if field.IdentityGeneration != ""`, which is
// PostgreSQL's spelling of an identity column. SQL Server's carries AutoInc and
// never a generation mode, so a seed read off SQL Server could not reach the
// renderer even once the reader started reporting it (stokaro/ptah#2196).
func TestFromField_CarriesTheIdentityRangeOfAnAutoIncrementColumn(t *testing.T) {
	c := qt.New(t)

	column := modelast.FromField(identityField("1000", "5"), nil, "sqlserver")

	c.Assert(column.AutoInc, qt.IsTrue)
	c.Assert(column.IdentityStart, qt.Equals, "1000")
	c.Assert(column.IdentityIncrement, qt.Equals, "5")
}

// TestFromFieldWithoutForeignKeys_CarriesTheIdentityRange covers the second
// entry point.
//
// The two conversions are maintained by hand and side by side, which is how a
// field comes to be carried by one of them and not the other.
func TestFromFieldWithoutForeignKeys_CarriesTheIdentityRange(t *testing.T) {
	c := qt.New(t)

	column := modelast.FromFieldWithoutForeignKeys(identityField("1000", "5"), nil, "sqlserver")

	c.Assert(column.AutoInc, qt.IsTrue)
	c.Assert(column.IdentityStart, qt.Equals, "1000")
	c.Assert(column.IdentityIncrement, qt.Equals, "5")
}

// TestFromField_LeavesAnAutoIncrementColumnWithoutARangeAlone is the control.
//
// A column that declares no range must not gain one, so the renderer keeps
// emitting the engine's own default rather than a range this conversion
// invented.
func TestFromField_LeavesAnAutoIncrementColumnWithoutARangeAlone(t *testing.T) {
	c := qt.New(t)

	column := modelast.FromField(identityField("", ""), nil, "sqlserver")

	c.Assert(column.AutoInc, qt.IsTrue)
	c.Assert(column.IdentityStart, qt.Equals, "")
	c.Assert(column.IdentityIncrement, qt.Equals, "")
}
