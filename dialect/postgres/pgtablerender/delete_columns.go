package pgtablerender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// ValidateConstraintFacets refuses unknown or observed settings and validates
// the declarations the PostgreSQL foreign-key renderer consumes.
func ValidateConstraintFacets(facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		if kind != pgtable.DeleteColumnsKind {
			return fmt.Errorf("%w: PostgreSQL constraint facet %q has no renderer", ptaherr.ErrUnsupportedFeature, kind)
		}
		value, _, err := schemaext.FacetAs[*pgtable.DesiredDeleteColumns](facets, kind)
		if err != nil {
			return err
		}
		if err := pgtable.ValidateDesiredDeleteColumns(value); err != nil {
			return err
		}
	}
	return nil
}

// DeleteColumnsSuffix renders the column selection after ON DELETE, including
// its leading space. Empty facets produce no suffix. Unsupported targets,
// malformed selections and settings outside their foreign key are refused.
func DeleteColumnsSuffix(target string, caps capability.Capabilities, constraint *ast.ConstraintNode) (string, error) {
	if constraint == nil || constraint.Reference == nil {
		return "", fmt.Errorf("%w: ON DELETE suffix requires a foreign key", schemaext.ErrInvalidValue)
	}
	if err := ValidateConstraintFacets(constraint.Facets); err != nil {
		return "", err
	}
	if err := pgtable.ValidateDeleteSelection(target, caps, constraint.Columns, constraint.Reference.OnDelete, constraint.Facets); err != nil {
		return "", err
	}
	columns, _, err := pgtable.DeleteColumnsOf(constraint.Facets)
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", nil
	}
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = sqlident.Quote(platform.Postgres, column)
	}
	return " (" + strings.Join(quoted, ", ") + ")", nil
}
