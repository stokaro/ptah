package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbindex"
)

// TestUniqueIndexName names the index a UNIQUE renders as: the constraint's
// own name, or the table's and the columns' for one that has none.
func TestUniqueIndexName(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		declared string
		columns  []string
		want     string
	}{
		{name: "a named constraint", table: "users", declared: "uq_users_email", columns: []string{"email"}, want: "uq_users_email"},
		{name: "a column", table: "users", columns: []string{"email"}, want: "users_email_key"},
		{name: "an unnamed constraint over two columns", table: "users", declared: "  ", columns: []string{"tenant", "login"},
			want: "users_tenant_login_key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.UniqueIndexName(test.table, test.declared, test.columns), qt.Equals, test.want)
		})
	}
}

// TestUniqueIsTheKey folds a UNIQUE over exactly the key columns, in any
// order, and nothing narrower or wider.
func TestUniqueIsTheKey(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
		key     []string
		want    bool
	}{
		{name: "the key", columns: []string{"id"}, key: []string{"id"}, want: true},
		{name: "the key in another order", columns: []string{"b", "a"}, key: []string{"a", "b"}, want: true},
		{name: "part of the key", columns: []string{"a"}, key: []string{"a", "b"}, want: false},
		{name: "the key and more", columns: []string{"a", "b"}, key: []string{"a"}, want: false},
		{name: "a table without a key", columns: []string{"a"}, key: nil, want: false},
		{name: "a column repeated", columns: []string{"a", "a"}, key: []string{"a", "b"}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.UniqueIsTheKey(test.columns, test.key), qt.Equals, test.want)
		})
	}
}
