package managedrows

// White-box testing required: typedYDBRows decides the YDB type every value of
// a declared row set is written in, and the exported Compare reaches it only
// through a live YDB connection, which the unit suite does not provision. The
// live tests drive it through Compare on both certified lines.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
)

// A live column keeps the type the table has, a column the table lacks takes
// the type its declaration lands on for the target, and both row sets come
// back in canonical form.
func TestTypedYDBRows_HappyPath(t *testing.T) {
	c := qt.New(t)
	live := &catalog.Table{Name: "rates", Columns: []catalog.Column{
		{Name: "code", DataType: "Utf8"},
		{Name: "rate", DataType: "Decimal(10,2)"},
		{Name: "joined", DataType: "Timestamp"},
	}}
	declared := map[string]string{"code": "VARCHAR(8)", "rate": "DECIMAL(10,2)", "joined": "TIMESTAMP", "since": "DATE"}
	desired := []map[string]any{{"code": "CZ", "rate": "1.50", "joined": "2026-01-02", "since": "2026-01-02"}}
	read := []map[string]any{{"code": "CZ", "rate": "1.5",
		"joined": time.Date(2026, time.January, 2, 1, 0, 0, 0, time.FixedZone("CET", 3600))}}

	types, typedDesired, typedLive, err := typedYDBRows(capability.YDB262(), live, declared, desired, read)

	c.Assert(err, qt.IsNil)
	c.Assert(types, qt.DeepEquals, map[string]string{
		"code": "Utf8", "rate": "Decimal(10,2)", "joined": "Timestamp", "since": "Date32",
	})
	midnight := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)
	c.Assert(typedDesired, qt.DeepEquals, []map[string]any{
		{"code": "CZ", "rate": "1.5", "joined": midnight, "since": midnight},
	})
	c.Assert(typedLive, qt.DeepEquals, []map[string]any{{"code": "CZ", "rate": "1.5", "joined": midnight}})
	// The canonical rows are copies: the caller's maps are left as they were.
	c.Assert(desired[0]["rate"], qt.Equals, "1.50")
}

// A line without the 64-bit types lands a declared DATE on Date, and a column
// no declaration types keeps its value for the diff to refuse.
func TestTypedYDBRows_HappyPath_NarrowLineAndUntypedColumn(t *testing.T) {
	c := qt.New(t)
	types, typedDesired, _, err := typedYDBRows(capability.YDB251(), nil,
		map[string]string{"since": "DATE"},
		[]map[string]any{{"since": "2026-01-02", "note": 7}}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(types, qt.DeepEquals, map[string]string{"since": "Date"})
	c.Assert(typedDesired, qt.DeepEquals, []map[string]any{
		{"since": time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC), "note": 7},
	})
}

func TestTypedYDBRows_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		live     *catalog.Table
		declared map[string]string
		desired  []map[string]any
		read     []map[string]any
		wantErr  string
	}{
		{
			name:     "a declared value its column cannot hold",
			live:     &catalog.Table{Columns: []catalog.Column{{Name: "n", DataType: "Int32"}}},
			declared: map[string]string{"n": "INTEGER"},
			desired:  []map[string]any{{"n": int64(5000000000)}},
			wantErr: `declared row 1, column "n": Int32 cannot hold 5000000000: it is outside the range ` +
				`-2147483648 to 2147483647`,
		},
		{
			name:     "a declared type with no YDB counterpart",
			declared: map[string]string{"t": "TIME"},
			wantErr:  `column "t": TIME has no YDB counterpart: YDB has no time-of-day type`,
		},
		{
			name:    "a live value its column cannot hold",
			live:    &catalog.Table{Columns: []catalog.Column{{Name: "n", DataType: "Uint8"}}},
			read:    []map[string]any{{"n": -1}},
			wantErr: `live row 1, column "n": Uint8 cannot hold -1: it is outside the range 0 to 255`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			types, typedDesired, typedLive, err := typedYDBRows(capability.YDB262(), test.live, test.declared,
				test.desired, test.read)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(types, qt.IsNil)
			c.Assert(typedDesired, qt.IsNil)
			c.Assert(typedLive, qt.IsNil)
		})
	}
}
