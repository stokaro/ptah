package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDialect_PostgresDomainNamedLikeATypeStillReportsARealChange
// pins that a domain's NAME never stands in for its base type when the
// comparator decides whether a column changed.
//
// The comparator reads a column's type spelling and hands it to
// normalize.Type, which matches by SUBSTRING: anything containing "int" is
// "integer" and anything containing "text" is "text". Feeding it a domain name
// therefore makes the name's spelling decide the answer. Both domains below are
// built on integer and carry ordinary names a schema author would pick:
//
//	CREATE DOMAIN waypoint AS integer CHECK (VALUE > 0);
//	CREATE DOMAIN context  AS integer;
//
// "waypoint" contains "int" and "context" contains "text", so a column of
// either compares EQUAL to a desired BIGINT and TEXT respectively, and the
// ALTER COLUMN ... TYPE the desired schema asks for is dropped from the plan.
// The plan still carries DROP DOMAIN ... CASCADE for the domain the column
// uses, so applying it destroys the column and its data instead of converting
// it.
//
// Measured on PostgreSQL 17.10, two databases, `schema diff --from P --to Q`
// and `ptah schema compare --root-dir <models> --db-url P`.
//
// The catalog values are copied from that server: information_schema reports a
// domain column under its BASE type, so DataType is "integer" and UDTName is
// "int4" while format_type names the domain.
func TestCompareWithDialect_PostgresDomainNamedLikeATypeStillReportsARealChange(t *testing.T) {
	c := qt.New(t)

	database := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "t",
				Type: "TABLE",
				Columns: []types.DBColumn{
					{Name: "a", DataType: "integer", UDTName: "int4", FormattedType: "waypoint", IsNullable: "NO"},
					{Name: "b", DataType: "integer", UDTName: "int4", FormattedType: "context", IsNullable: "NO"},
				},
			},
		},
	}

	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "a", Type: "BIGINT"},
			{StructName: "T", Name: "b", Type: "TEXT"},
		},
	}

	diff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)

	c.Assert(diff.TablesModified, qt.HasLen, 1,
		qt.Commentf("a column of domain waypoint (over integer) against a desired BIGINT, and one of domain context "+
			"(over integer) against a desired TEXT, are both real changes; reporting none leaves the plan with only "+
			"DROP DOMAIN ... CASCADE, which drops the columns"))
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 2,
		qt.Commentf("got %+v", diff.TablesModified[0].ColumnsModified))
}
