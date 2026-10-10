package builtin

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlindex"
)

// indexBlockSize is the hint the MySQL owner's facet holds, zero for none. A
// facet of another type is refused when the facets are prepared.
func indexBlockSize(facets schemaext.Facets) uint64 {
	size, _, _ := mysqlschema.IndexBlockSize(facets)
	return size
}

// validateIndexBlockSize rejects a hint that a dialect cannot render.
func validateIndexBlockSize(dialect, name string, size uint64) error {
	normalized := platform.NormalizeDialect(dialect)
	if size == 0 {
		return nil
	}
	if normalized == platform.MySQL || normalized == platform.MariaDB {
		if err := mysqlindex.ValidateBlockSize(normalized, size); err != nil {
			return fmt.Errorf("%w: index %q: %w", ptaherr.ErrInvalidSchemaDiff, name, err)
		}
		return nil
	}
	return &ptaherr.CapabilityError{
		Dialect: normalized, Feature: "index KEY_BLOCK_SIZE", Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("index %q declares KEY_BLOCK_SIZE, which %s does not support", name, normalized),
	}
}

// validatePrimaryKeyOptions is for the table's MySQL-family index metadata.
// A Constraint.Comment can also name a PostgreSQL object comment, and is
// validated separately by its existing path.
func validatePrimaryKeyOptions(dialect, table, comment string, size uint64) error {
	if err := validateIndexBlockSize(dialect, table, size); err != nil {
		return err
	}
	normalized := platform.NormalizeDialect(dialect)
	if comment != "" && normalized != platform.MySQL && normalized != platform.MariaDB {
		return fmt.Errorf("%w: primary key of %q declares an index COMMENT, which %s does not support", ptaherr.ErrUnsupportedFeature, table, normalized)
	}
	return nil
}

func validateConstraintBlockSize(dialect, kind, name string, size uint64) error {
	if size != 0 && kind != "PRIMARY KEY" {
		return fmt.Errorf("%w: KEY_BLOCK_SIZE on %s constraint %q is not supported; declare a unique index for a UNIQUE key", ptaherr.ErrUnsupportedFeature, kind, name)
	}
	return validateIndexBlockSize(dialect, name, size)
}
