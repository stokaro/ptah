package pgtablerender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// ValidateSourceExclusions validates a complete batch of source declarations
// without assigning names or folding equal constraints. Empty input succeeds.
// Unsupported targets, kinds and capabilities fail by name. An unnamed
// declaration cannot carry a comment because PostgreSQL chooses its name.
func ValidateSourceExclusions(target string, caps capability.Capabilities, constraints []*ast.OwnedIndexConstraint) error {
	for _, constraint := range constraints {
		if _, err := sourceExclusion(target, caps, constraint); err != nil {
			return err
		}
	}
	return nil
}

// SourceExclusions renders one clause per source declaration, in input order,
// and separate comment statements for explicitly named declarations. It leaves
// unnamed constraints and folding to PostgreSQL. Nil input returns no output;
// any invalid declaration discards all output. Inputs remain unchanged.
func SourceExclusions(target string, caps capability.Capabilities, table string, constraints []*ast.OwnedIndexConstraint) (clauses, comments []string, err error) {
	for _, constraint := range constraints {
		value, err := sourceExclusion(target, caps, constraint)
		if err != nil {
			return nil, nil, err
		}
		clause, err := ExclusionSQL(constraint.Name, value)
		if err != nil {
			return nil, nil, err
		}
		clauses = append(clauses, "  "+clause)
		if value.Comment != "" {
			comments = append(comments, "COMMENT ON CONSTRAINT "+sqlident.Quote(platform.Postgres, constraint.Name)+" ON "+sqlident.QuotePostgresQualified(table)+" IS "+sqlident.StringLiteral(value.Comment)+";")
		}
	}
	return clauses, comments, nil
}

func sourceExclusion(target string, caps capability.Capabilities, constraint *ast.OwnedIndexConstraint) (*pgtable.DesiredExclusion, error) {
	if constraint == nil {
		return nil, fmt.Errorf("%w: nil EXCLUDE source declaration", schemaext.ErrInvalidValue)
	}
	value, ok := constraint.Value.(*pgtable.DesiredExclusion)
	if !ok {
		return nil, fmt.Errorf("%w: no EXCLUDE source renderer for %T", ptaherr.ErrUnsupportedFeature, constraint.Value)
	}
	if err := ValidateExclusionTarget(target); err != nil {
		return nil, err
	}
	if err := pgtable.ValidateDesiredExclusion(value); err != nil {
		return nil, err
	}
	if value.Deferrable && !caps.Has(capability.DeferrableKeys) {
		return nil, fmt.Errorf("%w: EXCLUDE source declares unsupported DEFERRABLE", ptaherr.ErrUnsupportedFeature)
	}
	if constraint.Name == "" && value.Comment != "" {
		return nil, fmt.Errorf("%w: an EXCLUDE source comment requires an explicit constraint name", schemaext.ErrInvalidValue)
	}
	return value, nil
}
