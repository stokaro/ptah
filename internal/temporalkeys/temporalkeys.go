// Package temporalkeys holds the one check of what a declaration may say about
// PostgreSQL 18's temporal keys: WITHOUT OVERLAPS on a primary key or unique
// constraint and PERIOD on a foreign key. The Go annotation, YAML and HCL
// readers all declare them, and all refuse the same mistakes with the same
// words. Whether the target can write a temporal key at all is decided later,
// at render time, by [capability.TemporalKeys].
package temporalkeys

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/referentialaction"
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
	case constraint.Period && !noAction(constraint.OnDelete):
		return periodActionError(constraint.Name, "ON DELETE", constraint.OnDelete)
	case constraint.Period && !noAction(constraint.OnUpdate):
		return periodActionError(constraint.Name, "ON UPDATE", constraint.OnUpdate)
	default:
		return nil
	}
}

// noAction reports a referential action that leaves the check to the end of
// the statement: none written, or NO ACTION in any case, with a space or the
// underscore of an HCL document. It is the only action a PERIOD foreign key
// takes; PostgreSQL 18.6 and 19.0 answer "unsupported ON DELETE action for
// foreign key constraint using PERIOD" to RESTRICT, CASCADE, SET NULL and SET
// DEFAULT, and the same for ON UPDATE.
func noAction(action string) bool {
	action = referentialaction.Spelling(action)
	return action == "" || action == "NO ACTION"
}

func periodActionError(name, clause, action string) error {
	return fmt.Errorf("constraint %q has period and %s %s; a PERIOD foreign key takes only NO ACTION",
		name, clause, referentialaction.Spelling(action))
}

// CheckPrimaryKey refuses WITHOUT OVERLAPS on a table's primary key of fewer
// than two columns, counted in the column list or in the key parts, whichever
// the model carries. It answers nil for a table that does not ask for it.
func CheckPrimaryKey(table schemamodel.Table) error {
	if table.PrimaryKeyWithoutOverlaps && max(len(table.PrimaryKey), len(table.PrimaryKeyParts)) < minimumKeyColumns {
		return fmt.Errorf("table %q has primary_key_without_overlaps and a primary_key of fewer than two columns, "+
			"the last one a range or multirange", table.Name)
	}
	return nil
}
