package mysqlconvert

import (
	"ptah.run/catalog"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlindex"
)

// ReadBack answers what a MySQL or MariaDB read reports for a declared index.
// A read reports INDEX_TYPE as the index's method, and the comparison reads
// the index's kind and method from it through internal/mysqlindex; a unique
// index that carries a block size, a comment or a method holds options a
// UNIQUE constraint cannot.
type ReadBack struct{}

var _ schemaprojection.ReadBack = ReadBack{}

// ColumnType reports the declared type: the family stores it as written.
func (ReadBack) ColumnType(string) (string, bool) { return "", false }

// IndexMethod returns the index's kind, such as FULLTEXT or SPATIAL, and for
// a plain index the method it asked for, HASH, or "" for the engine default.
func (ReadBack) IndexMethod(indexType string) (string, bool) {
	if kind := mysqlindex.KindOf(indexType); kind != mysqlindex.Plain {
		return string(kind), true
	}
	return mysqlindex.Method(indexType), true
}

// UniqueIndexDetail reports whether a unique index holds a block size, a
// comment or a method. The block-size observation is one only an index
// carries: a reader gives one to an index with a hint or on a table that
// keeps one.
func (ReadBack) UniqueIndexDetail(index catalog.Index) bool {
	_, observed, _ := mysqlschema.IndexBlockSize(index.Facets)
	return observed || index.Comment != "" || mysqlindex.Method(index.Method) != ""
}
