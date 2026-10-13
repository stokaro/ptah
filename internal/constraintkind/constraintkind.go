// Package constraintkind defines which constraint kinds belong to the common
// model. Schema sources and render validation share this recognition so an
// owner-specific declaration cannot enter the common constraint pipeline.
package constraintkind

import (
	"strings"

	"ptah.run/core/ast"
)

// Valid reports whether text names a common constraint kind. Case does not
// affect recognition. An empty or unknown kind is invalid.
func Valid(text string) bool {
	switch strings.ToUpper(text) {
	case ast.PrimaryKeyConstraint.String(), ast.UniqueConstraint.String(), ast.ForeignKeyConstraint.String(), ast.CheckConstraint.String():
		return true
	default:
		return false
	}
}
