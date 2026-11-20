// Package temporalkeys holds the one check of what a declaration may say about
// PostgreSQL 18's temporal keys: WITHOUT OVERLAPS on a primary key or unique
// constraint and PERIOD on a foreign key. The Go annotation, YAML and HCL
// readers all declare them, and all refuse the same mistakes with the same
// words. Whether the target can write a temporal key at all is decided later,
// at render time, by [capability.TemporalKeys].
package temporalkeys

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
)

// minimumKeyColumns is the fewest columns a temporal key takes. The columns
// before the last say which rows the last column's ranges are kept apart
// within, and PostgreSQL 18 answers "constraint using WITHOUT OVERLAPS needs at
// least two columns" to a key of one.
const minimumKeyColumns = 2

// CheckConstraint refuses a temporal clause on the wrong kind of constraint and
// on a key with too few columns. It answers nil for a constraint that carries
// neither clause.
func CheckConstraint(constraint schemamodel.Constraint) error {
	switch {
	case constraint.WithoutOverlaps && constraint.Type != "PRIMARY KEY" && constraint.Type != "UNIQUE":
		return fmt.Errorf("constraint %q is a %s, and without_overlaps belongs to a PRIMARY KEY or UNIQUE constraint",
			constraint.Name, constraint.Type)
	case constraint.WithoutOverlaps && len(constraint.Columns) < minimumKeyColumns:
		return fmt.Errorf("constraint %q has without_overlaps and fewer than two columns", constraint.Name)
	case constraint.Period && constraint.Type != "FOREIGN KEY":
		return fmt.Errorf("constraint %q is a %s, and period belongs to a FOREIGN KEY constraint",
			constraint.Name, constraint.Type)
	case constraint.Period && (len(constraint.Columns) < minimumKeyColumns ||
		len(constraint.ForeignColumnsOrDefault()) != len(constraint.Columns)):
		return fmt.Errorf("constraint %q has period and needs the same number of columns, at least two, on each side",
			constraint.Name)
	default:
		return nil
	}
}

// Flag reads the value of a temporal attribute of a Go annotation, such as
// without_overlaps="true". It takes true and false in any case, and an empty
// value as false; anything else is refused. Read as false, a misspelled value
// would declare a plain key, which accepts the overlapping rows its author
// meant to forbid.
func Flag(attribute, value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("%s=%q is not a boolean; write true or false", attribute, value)
	}
}

// CheckPrimaryKey refuses WITHOUT OVERLAPS on a table's primary key of fewer
// than two columns. It answers nil for a table that does not ask for it.
func CheckPrimaryKey(table schemamodel.Table) error {
	if table.PrimaryKeyWithoutOverlaps && len(table.PrimaryKey) < minimumKeyColumns {
		return fmt.Errorf("table %q has primary_key_without_overlaps and a primary_key of fewer than two columns, "+
			"the last one a range or multirange", table.Name)
	}
	return nil
}
