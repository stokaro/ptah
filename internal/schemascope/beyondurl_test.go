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

// TestWiden pins how a read scope grows to cover a desired state's schemas. A
// list of names grows as [schemascope.Union] grows it. The reader's default on
// a connection that names no schema, which is every directory of a YDB
// database, does not grow at all: naming the directory a document declares
// would narrow the read to it and lose the tables at the database root.
func TestWiden(t *testing.T) {
	tests := []struct {
		name string
		info catalog.ServerInfo
		base []string
		more []string
		want []string
	}{
		{
			name: "a list of names gains the declared ones",
			info: catalog.ServerInfo{Dialect: "postgres", URL: "postgres://localhost/db?search_path=public", Schema: "public"},
			base: []string{"public"},
			more: []string{"extra"},
			want: []string{"extra", "public"},
		},
		{
			name: "the reader's default on a connection naming no schema stays the default",
			info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local"},
			base: nil,
			more: []string{"shop"},
			want: nil,
		},
		{
			// A whole MySQL server with no database yet lists no names, and
			// the declared database is the one to read.
			name: "a realm that listed nothing gains the declared names",
			info: catalog.ServerInfo{Dialect: "mysql", URL: "mysql://localhost/", WholeServer: true},
			base: nil,
			more: []string{"app"},
			want: []string{"app"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemascope.Widen(test.info, test.base, test.more), qt.DeepEquals, test.want)
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
