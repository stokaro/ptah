package renderer

import (
	"fmt"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// validateDeclaredColumns refuses a column declaration whose meaning no target
// receives.
//
// It was found by the field census (stokaro/ptah#2606) and filed as
// stokaro/ptah#2611. It is not a target's limitation: it is a fact an author
// writes that this pipeline replaces with a DIFFERENT constraint, which is
// worse than dropping it.
func validateDeclaredColumns(dialect string, database *schemamodel.Database) error {
	for _, field := range database.Fields {
		if err := validateUniqueExpression(dialect, field); err != nil {
			return err
		}
	}
	return nil
}

// validateUniqueExpression refuses a column declaring uniqueness over an
// expression.
//
// `unique_expr` is an attribute `internal/annotationmeta` documents, and it
// reached no renderer. Measured on PostgreSQL: a column carrying only
// `unique_expr` rendered with no uniqueness at all, and one carrying `unique`
// beside it rendered `UNIQUE` on the raw column -- uniqueness over `s` where
// the author asked for uniqueness over `lower(s)`. A different constraint,
// silently, is worse than a missing one, because the schema looks applied.
//
// Refusing is the answer rather than rendering, because rendering it means an
// expression index the comparison then has to recognize as the column's
// uniqueness, on ten targets that spell expression keys differently. That is a
// feature, and it is not one this refusal should quietly half-implement.
func validateUniqueExpression(dialect string, field schemamodel.Field) error {
	if strings.TrimSpace(field.UniqueExpr) == "" {
		return nil
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"column %q declares unique_expr %q, and no target renders it: "+
				"uniqueness over an expression is not implemented, and rendering the "+
				"column's own UNIQUE instead would enforce a different constraint",
			field.Name,
			strings.TrimSpace(field.UniqueExpr),
		),
	}
}
