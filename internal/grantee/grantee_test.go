package grantee_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/grantee"
)

// TestKey_HappyPath pins that PUBLIC keys alike in every case, as the
// catalogs report it in one case and a statement writes it in any, and that
// every other name keys as written.
func TestKey_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "PUBLIC as a statement writes it", in: "PUBLIC", want: "PUBLIC"},
		{name: "public as SQL Server reports it", in: "public", want: "PUBLIC"},
		{name: "mixed case with spaces", in: " Public ", want: "PUBLIC"},
		{name: "a role keeps its case", in: "Reader", want: "Reader"},
		{name: "a role is trimmed", in: " reader ", want: "reader"},
		{name: "a role that starts with public", in: "public_reader", want: "public_reader"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(grantee.Key(test.in), qt.Equals, test.want)
		})
	}
}
