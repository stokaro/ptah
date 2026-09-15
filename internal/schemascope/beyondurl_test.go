package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/schemascope"
)

// TestUnion pins the order a scope is asked for in. A saved plan's source
// fingerprint is compared byte for byte with a read made later, so two reads
// covering the same schemas must name them the same way.
func TestUnion(t *testing.T) {
	tests := []struct {
		name string
		base []string
		more []string
		want []string
	}{
		{
			name: "both lists merge sorted and de-duplicated",
			base: []string{"public", "extra"},
			more: []string{"extra", "audit"},
			want: []string{"audit", "extra", "public"},
		},
		{
			name: "blank names are dropped rather than read",
			base: []string{" ", "public"},
			more: []string{""},
			want: []string{"public"},
		},
		{
			name: "nothing left is nil, so the reader keeps its default",
			base: []string{""},
			more: nil,
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemascope.Union(test.base, test.more), qt.DeepEquals, test.want)
		})
	}
}

// TestBeyondURL pins which schemas a saved plan has to carry for its
// verification read to cover what its planning read did (stokaro/ptah#3285).
//
// The load-bearing row is the schema-limited URL. The verification read asks
// the URL again, and a URL limited to `public` answers `public`, so a plan
// writing `extra` that does not record it is verified against a schema it never
// touches. A realm-scoped URL records nothing, because the realm is listed again
// at verification and includes a schema created in the meantime.
func TestBeyondURL(t *testing.T) {
	tests := []struct {
		name     string
		info     catalog.ServerInfo
		urlScope []string
		scope    []string
		want     []string
	}{
		{
			name: "a schema-limited URL records the second schema a desired state names",
			info: catalog.ServerInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db?search_path=public",
				Schema:  "public",
			},
			urlScope: []string{"public"},
			scope:    []string{"extra", "public"},
			want:     []string{"extra"},
		},
		{
			name: "a MySQL connection is limited to its database",
			info: catalog.ServerInfo{
				Dialect: "mysql",
				URL:     "mysql://localhost/app",
				Schema:  "app",
			},
			urlScope: []string{"app"},
			scope:    []string{"app", "audit"},
			want:     []string{"audit"},
		},
		{
			name: "a realm-scoped URL records nothing, not even a schema the plan creates",
			info: catalog.ServerInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db",
				Schema:  "public",
			},
			urlScope: []string{"public"},
			scope:    []string{"extra", "public"},
			want:     nil,
		},
		{
			name: "a scope the URL already covers records nothing",
			info: catalog.ServerInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db?search_path=public",
				Schema:  "public",
			},
			urlScope: []string{"public"},
			scope:    []string{"public"},
			want:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemascope.BeyondURL(test.info, test.urlScope, test.scope), qt.DeepEquals, test.want)
		})
	}
}
