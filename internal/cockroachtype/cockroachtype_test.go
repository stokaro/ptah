package cockroachtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cockroachtype"
)

// TestResolve covers the declarations CockroachDB builds at the session's
// integer width and the ones it builds as written.
func TestResolve(t *testing.T) {
	tests := []struct {
		name           string
		declared       string
		defaultIntSize int
		want           string
	}{
		{name: "integer with no setting read", declared: "INTEGER", want: "int8"},
		{name: "int with no setting read", declared: "int", want: "int8"},
		{name: "integer in an eight-byte session", declared: " integer ", defaultIntSize: 8, want: "int8"},
		{name: "integer in a four-byte session", declared: "INTEGER", defaultIntSize: 4, want: "int4"},
		{name: "a named width", declared: "INT4", defaultIntSize: 8, want: "INT4"},
		{name: "bigint", declared: "BIGINT", want: "BIGINT"},
		{name: "another type", declared: "VARCHAR(32)", want: "VARCHAR(32)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(cockroachtype.Resolve(test.declared, test.defaultIntSize), qt.Equals, test.want)
		})
	}
}
