package oracledata

import (
	"ptah.run/catalog"
	"ptah.run/core/schemaprojection"
	"ptah.run/internal/oracletype"
)

// ReadBack answers what an Oracle read reports for a declared column: Oracle
// stores few declared type names as written, a TEXT column as a CLOB and a
// BIGINT as a NUMBER(19), and the reader reports what it stores. The type map
// is oracletype.Map, the one the renderer creates the column with.
type ReadBack struct{}

var _ schemaprojection.ReadBack = ReadBack{}

// ColumnType returns the type Oracle stores a column declared as declaredType
// as.
func (ReadBack) ColumnType(declaredType string) (string, bool) {
	return oracletype.Map(declaredType), true
}

// IndexMethod reports no method: an Oracle read reports none.
func (ReadBack) IndexMethod(string) (string, bool) { return "", false }

// UniqueIndexDetail reports none: Oracle keeps no unique-index option a UNIQUE
// constraint cannot.
func (ReadBack) UniqueIndexDetail(catalog.Index) bool { return false }
