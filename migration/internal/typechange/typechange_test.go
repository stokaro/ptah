package typechange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/internal/typechange"
)

func TestIsWidening(t *testing.T) {
	tests := []struct {
		name    string
		oldType string
		newType string
		want    bool
	}{
		// Integer width increases (the core INTEGER -> BIGINT gap).
		{"smallint to integer", "smallint", "integer", true},
		{"integer to bigint", "integer", "bigint", true},
		{"smallint to bigint", "smallint", "bigint", true},
		{"int2 to int8 postgres aliases", "int2", "int8", true},

		// Same range through dialect aliases must not read as a change.
		{"int alias integer", "int", "integer", false},
		{"int4 alias integer", "int4", "integer", false},
		{"int8 alias bigint", "int8", "bigint", false},
		{"identical integer", "integer", "integer", false},

		// The narrowing direction is not a widening.
		{"bigint to integer", "bigint", "integer", false},
		{"integer to smallint", "integer", "smallint", false},

		// String length increases.
		{"varchar length up", "varchar(50)", "varchar(100)", true},
		{"varchar length up postgres spelling", "character varying(50)", "varchar(100)", true},
		{"varchar length down", "varchar(100)", "varchar(50)", false},
		{"varchar same length", "varchar(100)", "varchar(100)", false},
		// text has no length, so a text/varchar transition is not a same-category widening
		// (it is a narrowing, handled by IsNarrowing).
		{"text to varchar", "text", "varchar(255)", false},

		// Decimal precision/scale increases.
		{"decimal precision up", "numeric(10,2)", "numeric(12,2)", true},
		{"decimal scale up", "numeric(10,2)", "numeric(10,4)", true},
		{"decimal precision down", "numeric(12,2)", "numeric(10,2)", false},
		{"decimal unchanged", "numeric(10,2)", "numeric(10,2)", false},

		// Cross-category and empty inputs never widen.
		{"integer to text", "integer", "text", false},
		{"empty old", "", "bigint", false},
		{"empty new", "integer", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.IsWidening(tt.oldType, tt.newType), qt.Equals, tt.want)
		})
	}
}

// TestWideningNarrowingAreOpposites documents that within a category the two
// detectors are mirror images: a pure width/length increase widens one way and
// narrows the other, and neither fires when the range is unchanged.
func TestWideningNarrowingAreOpposites(t *testing.T) {
	pairs := []struct {
		name    string
		smaller string
		larger  string
	}{
		{"integer range", "integer", "bigint"},
		{"varchar length", "varchar(50)", "varchar(100)"},
		{"decimal precision", "numeric(10,2)", "numeric(12,2)"},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.IsWidening(p.smaller, p.larger), qt.IsTrue)
			c.Assert(typechange.IsNarrowing(p.smaller, p.larger), qt.IsFalse)
			c.Assert(typechange.IsWidening(p.larger, p.smaller), qt.IsFalse)
			c.Assert(typechange.IsNarrowing(p.larger, p.smaller), qt.IsTrue)
		})
	}
}
