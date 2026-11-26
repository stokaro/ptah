package rowaccess_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/rowaccess"
)

// TestOf classifies the privileges a grant on a table names.
func TestOf(t *testing.T) {
	tests := []struct {
		privilege   string
		read, write bool
	}{
		{privilege: "SELECT", read: true},
		{privilege: " select ", read: true},
		{privilege: "INSERT", write: true},
		{privilege: "UPDATE", write: true},
		{privilege: "DELETE", write: true},
		{privilege: "TRUNCATE", write: true},
		{privilege: "ALL", read: true, write: true},
		{privilege: "ALL PRIVILEGES", read: true, write: true},
		{privilege: "REFERENCES"},
		{privilege: "EVENT"},
		{privilege: "TRIGGER"},
	}
	for _, test := range tests {
		t.Run(test.privilege, func(t *testing.T) {
			c := qt.New(t)

			read, write := rowaccess.Of(test.privilege)

			c.Assert(read, qt.Equals, test.read)
			c.Assert(write, qt.Equals, test.write)
		})
	}
}
