package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// Every non-NULL zoned value loses its offset or its unambiguous instant when
// its column becomes zone-free, even when both types keep the same digits.
func TestCompareColumn_RecordsTimeZoneLoss(t *testing.T) {
	tests := []struct {
		dialect, held, declared string
	}{
		{"postgres", "timetz(6)", "time(6)"},
		{"postgres", "timestamptz(6)", "timestamp(6)"},
		{"postgres", "timestamp with time zone", "timestamp without time zone"},
		{"cockroachdb", "TIMETZ", "TIME"},
		{"yugabytedb", "timestamptz", "timestamp"},
		{"sqlserver", "DATETIMEOFFSET(7)", "datetime2(7)"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.held, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Rounding, qt.DeepEquals, &difftypes.Rounding{
				Temporal: true, HeldType: test.held, DeclaredType: test.declared,
			})
			c.Assert(column.ReverseRounding, qt.IsNil)
			c.Assert(column.TypeNarrows, qt.IsTrue)
		})
	}
}

// A column gaining a zone needs the same protection when its migration is
// rolled back, because rows inserted meanwhile may use different offsets.
func TestCompareColumn_RecordsRollbackTimeZoneLoss(t *testing.T) {
	tests := []struct {
		dialect, held, declared string
	}{
		{"postgres", "time(6)", "timetz(6)"},
		{"postgres", "timestamp(6)", "timestamptz(6)"},
		{"cockroachdb", "TIME", "TIMETZ"},
		{"yugabytedb", "timestamp", "timestamptz"},
		{"sqlserver", "DATETIME2(7)", "datetimeoffset(7)"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.held, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Rounding, qt.IsNil)
			c.Assert(column.ReverseRounding, qt.DeepEquals, &difftypes.Rounding{
				Temporal: true, HeldType: test.declared, DeclaredType: test.held,
			})
		})
	}
}
