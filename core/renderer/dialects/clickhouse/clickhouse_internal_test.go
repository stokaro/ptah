package clickhouse

// White-box tests for helpers whose behaviour cannot be observed through the
// renderer's public surface alone. The rest of the dialect's test coverage
// lives in clickhouse_test.go as a black-box package.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestSplitColumns_DirectReturnValues(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"single bare column", "id", []string{"id"}},
		{"two bare columns", "id, created_at", []string{"id", "created_at"}},
		{"outer-wrapped list is unwrapped", "(id, created_at)", []string{"id", "created_at"}},
		{"function call kept intact", "tuple(a, b), c", []string{"tuple(a, b)", "c"}},
		{"intDiv inner comma not a split", "intDiv(ts, 86400), user_id", []string{"intDiv(ts, 86400)", "user_id"}},
		{"toYYYYMM expression", "toYYYYMM(ts), id", []string{"toYYYYMM(ts)", "id"}},
		{"cityHash64 expression", "cityHash64(user_id), event_time", []string{"cityHash64(user_id)", "event_time"}},
		// Outer parens that wrap only a prefix (not the whole expression) must
		// stay attached.
		{"partial outer parens not stripped", "(a, b), c", []string{"(a, b)", "c"}},
		// Empty / whitespace handling — leading, trailing and consecutive
		// commas must not produce empty entries.
		{"leading comma dropped", ",id", []string{"id"}},
		{"trailing comma dropped", "id,", []string{"id"}},
		{"consecutive commas collapsed", "id,,created_at", []string{"id", "created_at"}},
		{"whitespace-only entry dropped", "id, ,created_at", []string{"id", "created_at"}},
		{"empty input", "", nil},
		{"comma only", ",", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got := splitColumns(tc.in)
			c.Assert(got, qt.DeepEquals, tc.want)
		})
	}
}
