package mssql

// White-box testing required: scanSequence turns a sys.sequences row into the
// shared shape, and the decision it makes -- which of the fully populated
// catalog columns become a set option and which stay unset -- is the property
// that keeps an apply loop from re-planning the same sequence forever.
//
// The reason it is tested here rather than through the reader is specific.
// Reaching it from outside needs a live SQL Server, and the live round trip in
// integration/dbschema covers the path end to end. What a live test cannot do
// cheaply is enumerate the is_cached and cache_size combinations, because
// producing the third one needs a server that answers a cache size for a
// sequence created with NO CACHE, which SQL Server does not.

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSequenceCacheReadsTwoFactsNotOne pins that is_cached and cache_size are a
// pair.
//
// A cached sequence whose size the server chose reports is_cached = 1 and a
// NULL cache_size, and no declaration can ask for "whatever the server picks"
// by number -- so it has to read as unset, or every such sequence would compare
// unequal against a declaration that named a size and equal against nothing.
// NO CACHE reports is_cached = 0, which goschema.Sequence also cannot spell.
func TestSequenceCacheReadsTwoFactsNotOne(t *testing.T) {
	size := sql.NullInt64{Int64: 20, Valid: true}
	tests := []struct {
		name  string
		facts sequenceCacheFacts
		want  *int64
	}{
		{name: "explicit size is managed", facts: sequenceCacheFacts{cached: true, size: size}, want: new(int64)},
		{name: "server-chosen size is unset", facts: sequenceCacheFacts{cached: true}, want: nil},
		{name: "no cache is unset", facts: sequenceCacheFacts{}, want: nil},
		{name: "no cache with a stale size is still unset", facts: sequenceCacheFacts{size: size}, want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.facts.managedOption()

			if test.want == nil {
				c.Assert(got, qt.IsNil)
				return
			}
			c.Assert(got, qt.IsNotNil)
			c.Assert(*got, qt.Equals, int64(20))
		})
	}
}
