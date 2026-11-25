package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// TestCompareColumn_RoundingAsTheEngineReadsIt holds the comparison's answer to which
// type changes keep fewer digits after the point, for each engine that converts such a
// column without an error, and the type the read reports against the declared one. A
// type that declares no size means what the engine says it means.
func TestCompareColumn_RoundingAsTheEngineReadsIt(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		held         string
		declared     string
		temporal     bool
		wantDeclared string
	}{
		{"PostgreSQL a numeric's scale", "postgres", "numeric(8,3)", "numeric(8,1)", false, "numeric(8,1)"},
		{"PostgreSQL a numeric under an integer", "postgres", "numeric(8,3)", "integer", false, "integer"},
		{"PostgreSQL money under one digit", "postgres", "money", "numeric(12,1)", false, "numeric(12,1)"},
		{"PostgreSQL bare timestamp to fewer", "postgres", "timestamp without time zone", "timestamp(0)", true, "timestamp(0)"},
		{"PostgreSQL an interval's digits", "postgres", "interval(6)", "interval(0)", true, "interval(0)"},
		{"PostgreSQL an interval to whole hours", "postgres", "interval", "interval hour", true, "interval hour"},
		{"CockroachDB a decimal's scale", "cockroachdb", "DECIMAL(8,3)", "decimal(8,1)", false, "decimal(8,1)"},
		{"CockroachDB six to three", "cockroachdb", "TIMESTAMP(6)", "timestamp(3)", true, "timestamp(3)"},
		{"MySQL a decimal's scale", "mysql", "decimal(8,3)", "decimal(8,1)", false, "decimal(8,1)"},
		{"MySQL a decimal under an integer", "mysql", "decimal(8,3)", "int", false, "int"},
		{"MySQL six to bare", "mysql", "datetime(6)", "datetime", true, "datetime"},
		{"MariaDB a time", "mariadb", "time(6)", "time(3)", true, "time(3)"},
		{"SQL Server a decimal's scale", "sqlserver", "DECIMAL(5,2)", "decimal(5,1)", false, "decimal(5,1)"},
		{"SQL Server bare to fewer", "sqlserver", "DATETIME2", "datetime2(3)", true, "datetime2(3)"},
		{"SQL Server datetime2 under datetime", "sqlserver", "DATETIME2(7)", "datetime", true, "datetime"},
		{"SQL Server to whole minutes", "sqlserver", "DATETIME2(0)", "smalldatetime", true, "smalldatetime"},
		{"ClickHouse a decimal's scale", "clickhouse", "Decimal(8, 3)", "Decimal(8, 1)", false, "Decimal(8, 1)"},
		{"ClickHouse down", "clickhouse", "DateTime64(6)", "DateTime64(3)", true, "DateTime64(3)"},
		{"Oracle down", "oracle", "TIMESTAMP(6)", "TIMESTAMP(0)", true, "TIMESTAMP(0)"},
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
			c.Assert(column.TypeNarrows, qt.IsTrue)
			c.Assert(column.Rounding, qt.DeepEquals, &difftypes.Rounding{
				Temporal: test.temporal, HeldType: test.held, DeclaredType: test.wantDeclared,
			})
		})
	}
}

// TestCompareColumn_NoRoundingWhereEveryDigitStays holds the changes that keep every
// digit after the point: a wider type, fewer digits before the point, and a type of
// another kind, which loses more than digits and is a narrowing alone.
func TestCompareColumn_NoRoundingWhereEveryDigitStays(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		held        string
		declared    string
		wantNarrows bool
	}{
		{"a higher scale", "mysql", "decimal(8,1)", "decimal(10,3)", false},
		{"fewer digits before the point", "postgres", "numeric(8,3)", "numeric(6,3)", true},
		{"a higher precision", "postgres", "timestamp(3) without time zone", "timestamp(6)", false},
		{"a timestamp to a date", "mysql", "datetime(6)", "date", true},
		{"a whole decimal to an integer", "mysql", "decimal(8,0)", "int", false},
		{"a decimal to a float", "mysql", "decimal(8,3)", "double", false},
		{"money under its own digits", "postgres", "money", "numeric(12,3)", true},
		{"a string", "postgres", "character varying(10)", "varchar(3)", true},
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
			c.Assert(column.TypeNarrows, qt.Equals, test.wantNarrows)
			c.Assert(column.Rounding, qt.IsNil)
		})
	}
}
