package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDialect_PostgresArrayAndDomainColumnsCompareEqualToThemselves
// compares a PostgreSQL schema against ITSELF (stokaro/ptah#1138).
//
// The desired side is not written out by hand -- it is the same *types.DBSchema
// put through the database-to-schema converter, which is the path
// `ptah-compat schema diff --from <db> --to <db>` takes. That is what makes the
// assertion meaningful: any column the two sides read out of different fields
// shows up as a change from a database to itself.
//
// The columns and their catalog values are copied from PostgreSQL 17. The
// comparator used to read ColumnType/UDTName while the converter read the
// server's own format_type, and the run reported seven ALTER COLUMN ... TYPE
// statements retyping each column to the type it already had, e.g.
//
//	arrays.a_bit    type: _bit    -> bit(8)[]
//	scalars.c_tags  type: text    -> tags
//
// The scalar rows are here so the test would notice a fix that simply stopped
// comparing types: an ordinary varchar and an ordinary user-defined column go
// down the untouched path and must still match.
func TestCompareWithDialect_PostgresArrayAndDomainColumnsCompareEqualToThemselves(t *testing.T) {
	c := qt.New(t)

	database := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "arrays",
				Type: "TABLE",
				Columns: []types.DBColumn{
					{Name: "a_bit", DataType: "ARRAY", UDTName: "_bit", FormattedType: "bit(8)[]", IsNullable: "NO"},
					{Name: "a_char", DataType: "ARRAY", UDTName: "_bpchar", FormattedType: "character(5)[]", IsNullable: "NO"},
					{Name: "a_cube", DataType: "ARRAY", UDTName: "_cube", FormattedType: "cube[]", IsNullable: "NO"},
					{Name: "a_enum", DataType: "ARRAY", UDTName: "_status", FormattedType: "status[]", IsNullable: "NO"},
					{Name: "a_numeric", DataType: "ARRAY", UDTName: "_numeric", FormattedType: "numeric(10,2)[]", IsNullable: "NO"},
					{
						Name:          "a_varchar",
						DataType:      "ARRAY",
						UDTName:       "_varchar",
						FormattedType: "character varying(100)[]",
						IsNullable:    "NO",
					},
					{
						Name:          "a_timestamptz",
						DataType:      "ARRAY",
						UDTName:       "_timestamptz",
						FormattedType: "timestamp(3) with time zone[]",
						IsNullable:    "NO",
					},
				},
			},
			{
				Name: "scalars",
				Type: "TABLE",
				Columns: []types.DBColumn{
					{Name: "c_domain", DataType: "integer", UDTName: "int4", FormattedType: "positive_int", IsNullable: "NO"},
					{Name: "c_point3d", DataType: "USER-DEFINED", UDTName: "cube", FormattedType: "point3d", IsNullable: "NO"},
					{Name: "c_tags", DataType: "ARRAY", UDTName: "_text", FormattedType: "tags", IsNullable: "NO"},
					{Name: "c_cube", DataType: "USER-DEFINED", UDTName: "cube", IsNullable: "NO"},
					{Name: "c_varchar", DataType: "character varying", UDTName: "varchar", CharacterMaxLength: new(100), IsNullable: "NO"},
				},
			},
		},
	}

	diff := schemadiff.CompareWithDialect(dbschematogo.ConvertDBSchemaToGoSchema(database), database, platform.Postgres)

	c.Assert(diff.TablesModified, qt.HasLen, 0, qt.Commentf("a database compared against itself reported %+v", diff.TablesModified))
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
}
