package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDatabaseInfo_SQLServerUnknownTableSemantics_FailurePath covers
// two declared tables whose catalog identity the connection cannot separate.
//
// SQL Server folds identifiers by a collation the server chooses, so whether
// two names are one object is the server's answer rather than the document's.
// Without it the comparison refuses instead of guessing, and the refusal is
// about the declaration as a whole rather than about anything a change set
// names (stokaro/ptah#2315).
func TestCompareWithDatabaseInfo_SQLServerUnknownTableSemantics_FailurePath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&schemamodel.Database{Tables: []schemamodel.Table{
			{StructName: "Order", Schema: "dbo", Name: "orders"},
			{StructName: "User", Schema: "dbo", Name: "users"},
		}},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.SQLServer},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*target tables dbo\.orders and dbo\.users may have the same catalog identity.*`)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabaseInfo_SQLServerUnknownColumnSemantics_FailurePath is
// the same question one level down, for two columns of one table.
func TestCompareWithDatabaseInfo_SQLServerUnknownColumnSemantics_FailurePath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&schemamodel.Database{
			Tables: []schemamodel.Table{
				{StructName: "User", Schema: "dbo", Name: "users"},
			},
			Fields: []schemamodel.Field{
				{StructName: "User", Name: "email", Type: "NVARCHAR(320)"},
				{StructName: "User", Name: "status", Type: "INT"},
			},
		},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.SQLServer},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*target columns dbo\.users\.email and dbo\.users\.status may have the same catalog identity.*`,
	)
	c.Assert(diff, qt.IsNil)
}
