package pgtable

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ValidateDeleteSelection validates a captured foreign key's column selection
// for rendering. Targets without the clause refuse even a list of every key
// column. The action must be SET NULL or SET DEFAULT, and each name must belong
// to the local key. No facet is valid on every target and adds no restriction.
func ValidateDeleteSelection(target string, caps capability.Capabilities, keyColumns []string, action string, facets schemaext.Facets) error {
	columns, found, err := DeleteColumnsOf(facets)
	if err != nil || !found {
		return err
	}
	if len(columns) == 0 {
		return nil
	}
	if !caps.Has(capability.ForeignKeyDeleteColumnList) {
		return &ptaherr.CapabilityError{Dialect: target, Feature: "foreign key ON DELETE column list", Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s does not support a column list on ON DELETE %s", platform.NormalizeDialect(target), action)}
	}
	if action != "SET NULL" && action != "SET DEFAULT" {
		return fmt.Errorf("%w: an ON DELETE column list needs SET NULL or SET DEFAULT, not %q", ptaherr.ErrInvalidSchemaDiff, action)
	}
	for _, column := range columns {
		if !slices.Contains(keyColumns, column) {
			return fmt.Errorf("%w: ON DELETE %s names column %q, which is not one of the key's columns %v", ptaherr.ErrInvalidSchemaDiff, action, column, keyColumns)
		}
	}
	return nil
}

// DeleteSetsColumnNull reports whether the delete action sets column to NULL.
// An absent or empty selection sets every key column. Invalid facets fail.
func DeleteSetsColumnNull(action string, facets schemaext.Facets, column string) (bool, error) {
	columns, _, err := DeleteColumnsOf(facets)
	if err != nil {
		return false, err
	}
	return action == "SET NULL" && (len(columns) == 0 || slices.Contains(columns, column)), nil
}

// ValidateDeleteConstraint refuses this setting on a common constraint that
// is not a foreign key. Unknown facet kinds remain with their own validators.
func ValidateDeleteConstraint(kind string, facets schemaext.Facets) error {
	_, held, err := DeleteColumnsOf(facets)
	if err != nil {
		return err
	}
	if held && !strings.EqualFold(strings.TrimSpace(kind), "FOREIGN KEY") {
		return fmt.Errorf("%w: ON DELETE column list requires a FOREIGN KEY, not %s", ptaherr.ErrInvalidSchemaDiff, kind)
	}
	return nil
}

// ValidateDeleteTarget refuses a declared column list on a target whose SQL
// grammar has no spelling for it. Version capabilities are checked separately.
func ValidateDeleteTarget(target string, facets schemaext.Facets) error {
	columns, held, err := DeleteColumnsOf(facets)
	if err != nil || !held || len(columns) == 0 {
		return err
	}
	target = platform.NormalizeDialect(target)
	if target != platform.Postgres && target != platform.YugabyteDB {
		return &ptaherr.CapabilityError{Dialect: target, Feature: "foreign key ON DELETE column list", Err: ptaherr.ErrUnsupportedFeature,
			Message: target + " does not support a foreign key ON DELETE column list"}
	}
	return nil
}
