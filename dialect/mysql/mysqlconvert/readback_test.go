package mysqlconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dialect/mysql/mysqlconvert"
)

func TestReadBack_IndexMethod(t *testing.T) {
	tests := []struct {
		indexType string
		want      string
	}{
		{indexType: "FULLTEXT", want: "FULLTEXT"},
		{indexType: "SPATIAL", want: "SPATIAL"},
		{indexType: "BTREE", want: ""},
		{indexType: "HASH", want: "HASH"},
		{indexType: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.indexType, func(t *testing.T) {
			c := qt.New(t)

			method, ok := mysqlconvert.ReadBack{}.IndexMethod(test.indexType)

			c.Assert(ok, qt.IsTrue)
			c.Assert(method, qt.Equals, test.want)
		})
	}
}

func TestReadBack_UniqueIndexDetail(t *testing.T) {
	tests := []struct {
		name  string
		index catalog.Index
		want  bool
	}{
		{name: "a plain unique index", index: catalog.Index{IsUnique: true}, want: false},
		{name: "a comment", index: catalog.Index{IsUnique: true, Comment: "lookup"}, want: true},
		{name: "a method", index: catalog.Index{IsUnique: true, Method: "HASH"}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlconvert.ReadBack{}.UniqueIndexDetail(test.index), qt.Equals, test.want)
		})
	}
}

func TestReadBack_ColumnTypeIsAsDeclared(t *testing.T) {
	c := qt.New(t)

	stored, ok := mysqlconvert.ReadBack{}.ColumnType("TEXT")

	c.Assert(ok, qt.IsFalse)
	c.Assert(stored, qt.Equals, "")
}
