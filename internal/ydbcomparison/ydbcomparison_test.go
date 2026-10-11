package ydbcomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbcomparison"
)

// TestIndexes_RenameCarries carries an index's partitioning through a rename,
// which the owner compares under the new name, and nothing a rename cannot
// carry, such as a vector index's settings.
func TestIndexes_RenameCarries(t *testing.T) {
	tests := []struct {
		name string
		kind schemaext.Kind
		want bool
	}{
		{name: "partitioning", kind: ydbschema.IndexPartitioningKind, want: true},
		{name: "vector settings", kind: ydbschema.VectorIndexKind, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcomparison.Indexes{}.RenameCarries(test.kind), qt.Equals, test.want)
		})
	}
}

// TestIndexes_OptionsEqual reads a declared access method and a catalog's
// report of one as one kind, and a kind either side cannot read as a
// difference.
func TestIndexes_OptionsEqual(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		held     string
		want     bool
	}{
		{name: "no method and a synchronous index", declared: "", held: "GLOBAL SYNC", want: true},
		{name: "a declared btree", declared: "btree", held: "GLOBAL SYNC", want: true},
		{name: "an asynchronous index", declared: "async", held: "GLOBAL ASYNC", want: true},
		{name: "asynchronous against synchronous", declared: "async", held: "GLOBAL SYNC", want: false},
		{name: "a method nothing reads", declared: "hash", held: "GLOBAL SYNC", want: false},
		{name: "a catalog method nothing reads", declared: "", held: "SOMETHING", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbcomparison.Indexes{}.OptionsEqual(schemamodel.Index{Type: test.declared}, catalog.Index{Method: test.held})
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestViews_QueryEqual compares two queries in the form YDB stores them:
// spacing and comments do not count, and the case of a name does.
func TestViews_QueryEqual(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current string
		want    bool
	}{
		{name: "spacing and a comment", desired: "select   id -- c\n FROM items;", current: "select id FROM items", want: true},
		{name: "the case of a name", desired: "select id from Items", current: "select id from items", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcomparison.Views{}.QueryEqual(test.desired, test.current), qt.Equals, test.want)
		})
	}
}
