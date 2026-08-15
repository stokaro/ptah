package dbtest

// White-box testing required: normalizeScalar is an unexported helper whose
// formatting contract — deterministic rendering of time.Time (RFC3339, not the
// default " +0000 UTC" form) and SQL NULL — cannot be asserted through the
// exported API without depending on driver-specific value typing, so it is
// exercised directly here.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestNormalizeScalar(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{name: "bytes", in: []byte("hello"), want: "hello"},
		{name: "string", in: "hello", want: "hello"},
		{name: "int64", in: int64(1), want: "1"},
		{name: "null", in: nil, want: "<nil>"},
		{name: "time", in: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC), want: "2025-01-15T10:30:00Z"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalizeScalar(tc.in), qt.Equals, tc.want)
		})
	}
}
