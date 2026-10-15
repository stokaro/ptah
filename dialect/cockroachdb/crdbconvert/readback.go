package crdbconvert

import (
	"ptah.run/catalog"
	"ptah.run/core/schemaprojection"
	"ptah.run/internal/cockroachtype"
)

// ReadBack answers what a CockroachDB read reports for a declared column:
// CockroachDB builds a column declared INT or INTEGER at the session's
// integer width, and a read reports that width. No session is read where a
// declaration is converted, so the width is CockroachDB's own default, as it
// is for an offline comparison.
type ReadBack struct{}

var _ schemaprojection.ReadBack = ReadBack{}

// ColumnType returns the type CockroachDB stores a column declared as
// declaredType as, at the default integer width.
func (ReadBack) ColumnType(declaredType string) (string, bool) {
	return cockroachtype.Resolve(declaredType, 0), true
}

// IndexMethod reports no method of CockroachDB's own; its index type is the
// access method (descriptor.Conversion.IndexTypeIsMethod).
func (ReadBack) IndexMethod(string) (string, bool) { return "", false }

// UniqueIndexDetail reports none.
func (ReadBack) UniqueIndexDetail(catalog.Index) bool { return false }
