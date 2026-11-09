package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// TestCompareTypeSize_ASizeOnOneSideIsAChange plans a type the catalog holds against one
// that spells its size differently. A row that plans nothing here has the schema saying
// one thing and the database holding another. The rating of a row follows what the bare
// side holds on the engine.
func TestCompareTypeSize_ASizeOnOneSideIsAChange(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		held        string
		declared    string
		wantNarrows bool
	}{
		{"PostgreSQL any number to a size", "postgres", "numeric", "numeric(10,2)", true},
		{"PostgreSQL a size to any number", "postgres", "numeric(10,2)", "numeric", false},
		{"PostgreSQL arrays to a size", "postgres", "numeric[]", "numeric(10,2)[]", true},
		{"PostgreSQL arrays to any number", "postgres", "numeric(10,2)[]", "numeric[]", false},
		{"PostgreSQL bare bpchar to bare char", "postgres", "bpchar", "char", true},
		{"CockroachDB any number to a size", "cockroachdb", "numeric", "decimal(10,2)", true},
		{"CockroachDB a size to any number", "cockroachdb", "NUMERIC(10,2)", "decimal", false},
		{"YugabyteDB any number to a size", "yugabytedb", "numeric", "numeric(10,2)", true},
		{"YugabyteDB a size to any number", "yugabytedb", "numeric(10,2)", "numeric", false},
		{"MySQL a fraction to bare", "mysql", "decimal(10,2)", "decimal", true},
		{"MySQL more digits to bare", "mysql", "decimal(12,0)", "decimal", true},
		{"MySQL fewer digits to bare", "mysql", "decimal(5,0)", "decimal", false},
		{"MariaDB a fraction to bare", "mariadb", "decimal(10,2)", "numeric", true},
		{"SQL Server MAX to a length", "sqlserver", "NVARCHAR(MAX)", "nvarchar(50)", true},
		{"SQL Server a length to MAX", "sqlserver", "NVARCHAR(50)", "nvarchar(max)", false},
		{"SQL Server a length to bare", "sqlserver", "NVARCHAR(50)", "nvarchar", true},
		{"SQL Server a fraction to bare", "sqlserver", "DECIMAL(10,2)", "decimal", true},
		{"SQL Server more digits to bare", "sqlserver", "DECIMAL(20,0)", "numeric", true},
		{"Oracle any number to a size", "oracle", "NUMBER", "NUMBER(10,2)", true},
		{"Oracle a size to any number", "oracle", "NUMBER(10,2)", "NUMBER", false},
		{"Oracle no fraction to any number", "oracle", "NUMBER(*,0)", "NUMBER", false},
		{"Oracle any number to no fraction", "oracle", "NUMBER", "NUMBER(*,0)", true},
		{"Oracle a length in characters", "oracle", "VARCHAR2(100)", "VARCHAR2(50 CHAR)", true},
		{"Oracle another column under INTEGER", "oracle", "NUMBER(19)", "INTEGER", true},
		{"Oracle another float under DOUBLE PRECISION", "oracle", "FLOAT(63)", "DOUBLE PRECISION", true},
		{"Oracle a number under REAL", "oracle", "NUMBER(*,0)", "REAL", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Changes["type"], qt.Not(qt.Equals), "")
			c.Assert(column.TypeNarrows, qt.Equals, test.wantNarrows)
		})
	}
}

// TestCompareTypeSize_OneTypeInTwoSpellingsIsNoChange holds the other direction. The
// catalog spells a type with the size the engine gave it and a declaration may leave it
// out, so each row is what an adopted database reads as. A plan here is a statement that
// changes nothing, on every run.
func TestCompareTypeSize_OneTypeInTwoSpellingsIsNoChange(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		held     string
		declared string
	}{
		{"PostgreSQL bare numeric", "postgres", "numeric", "numeric"},
		{"PostgreSQL a decimal is numeric", "postgres", "numeric", "decimal"},
		{"PostgreSQL a scale of zero", "postgres", "numeric(10,0)", "numeric(10)"},
		{"PostgreSQL an array of numbers", "postgres", "numeric(10,2)[]", "decimal(10,2)[]"},
		{"PostgreSQL char is the catalog's one character", "postgres", "bpchar(1)", "char"},
		{"PostgreSQL character is char", "postgres", "bpchar(1)", "character"},
		{"PostgreSQL a bare bpchar", "postgres", "bpchar", "bpchar"},
		{"PostgreSQL bit", "postgres", "bit(1)", "bit"},
		{"PostgreSQL float", "postgres", "double precision", "float"},
		{"PostgreSQL float(53)", "postgres", "double precision", "float(53)"},
		{"PostgreSQL float(24)", "postgres", "real", "float(24)"},
		{"CockroachDB bare numeric", "cockroachdb", "numeric", "decimal"},
		{"CockroachDB float", "cockroachdb", "float8", "float"},
		{"CockroachDB float(10)", "cockroachdb", "float4", "float(10)"},
		{"YugabyteDB bare numeric", "yugabytedb", "numeric", "numeric"},
		{"MySQL bare decimal", "mysql", "decimal(10,0)", "decimal"},
		{"MySQL bare numeric", "mysql", "decimal(10,0)", "numeric"},
		{"MySQL no scale", "mysql", "decimal(5,0)", "decimal(5)"},
		{"MySQL float(24)", "mysql", "float", "float(24)"},
		{"MySQL float(25)", "mysql", "double", "float(25)"},
		{"MySQL real", "mysql", "double", "real"},
		{"MySQL double precision", "mysql", "double", "double precision"},
		{"MySQL char", "mysql", "char(1)", "char"},
		{"MySQL binary", "mysql", "binary(1)", "binary"},
		{"MySQL bit", "mysql", "bit(1)", "bit"},
		{"MariaDB bare decimal", "mariadb", "decimal(10,0)", "decimal"},
		{"MariaDB year", "mariadb", "year(4)", "year"},
		{"SQL Server bare decimal", "sqlserver", "DECIMAL(18,0)", "decimal"},
		{"SQL Server bare numeric", "sqlserver", "NUMERIC(18,0)", "numeric"},
		{"SQL Server MAX", "sqlserver", "NVARCHAR(MAX)", "nvarchar(max)"},
		{"SQL Server a bare portable varchar", "sqlserver", "NVARCHAR(MAX)", "varchar"},
		{"SQL Server an nvarchar", "sqlserver", "NVARCHAR(1)", "nvarchar"},
		{"SQL Server a char", "sqlserver", "CHAR(1)", "char"},
		{"SQL Server an nchar", "sqlserver", "NCHAR(1)", "nchar"},
		{"SQL Server a binary", "sqlserver", "BINARY(1)", "binary"},
		{"SQL Server a varbinary", "sqlserver", "VARBINARY(1)", "varbinary"},
		{"SQL Server float", "sqlserver", "FLOAT", "float(53)"},
		{"SQL Server float(25)", "sqlserver", "FLOAT", "float(25)"},
		{"SQL Server real", "sqlserver", "REAL", "float(24)"},
		{"Oracle bare NUMBER", "oracle", "NUMBER", "NUMBER"},
		{"Oracle a star is bare", "oracle", "NUMBER", "NUMBER(*)"},
		{"Oracle a star and no fraction", "oracle", "NUMBER(*,0)", "NUMBER(*,0)"},
		{"Oracle a decimal", "oracle", "NUMBER(10,2)", "decimal(10,2)"},
		{"Oracle FLOAT", "oracle", "FLOAT(126)", "FLOAT"},
		{"Oracle CHAR", "oracle", "CHAR(1)", "CHAR"},
		{"Oracle NCHAR", "oracle", "NCHAR(1)", "NCHAR"},
		{"Oracle a length in characters", "oracle", "VARCHAR2(100)", "VARCHAR2(100 CHAR)"},
		{"Oracle a RAW's bytes", "oracle", "RAW(16)", "RAW(16)"},
		{"Oracle a column the server built as INTEGER", "oracle", "NUMBER(*,0)", "INTEGER"},
		{"Oracle a column Ptah built as INTEGER", "oracle", "NUMBER(10)", "INTEGER"},
		{"Oracle a column the server built as SMALLINT", "oracle", "NUMBER(*,0)", "smallint"},
		{"Oracle a column the server built as DECIMAL", "oracle", "NUMBER(*,0)", "DECIMAL"},
		{"Oracle a bare DECIMAL Ptah built", "oracle", "NUMBER", "DECIMAL"},
		{"Oracle a column the server built as DOUBLE PRECISION", "oracle", "FLOAT(126)", "DOUBLE PRECISION"},
		{"Oracle a column Ptah built as DOUBLE PRECISION", "oracle", "BINARY_DOUBLE", "DOUBLE PRECISION"},
		{"Oracle a column the server built as REAL", "oracle", "FLOAT(63)", "REAL"},
		{"Oracle a column the server built as BOOLEAN", "oracle", "BOOLEAN", "BOOLEAN"},
		{"Oracle a UUID's bytes", "oracle", "RAW(16)", "UUID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}
