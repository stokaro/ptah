package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ObservedUniqueConstraintsFeature adds the UNIQUE constraints a table holds
// to Table.constraints, for an engine that keeps such a constraint apart from
// its indexes, as DuckDB does. The list then carries every UNIQUE constraint
// of the table, over one column or several, each named and bound to the table
// like a foreign key or a CHECK, and a column that a UNIQUE over it alone
// covers states ObservedColumn.unique. An index the engine builds to enforce
// one stays out of Table.indexes. Where the engine also lists
// ConstraintChangesFeature, it plans the addition and the removal of a UNIQUE
// constraint on a table the plan keeps as it plans those of a foreign key or
// a CHECK. It is a two-way feature: the provider and the engine list it, and
// the engine needs ObservedConstraintsFeature. Contract 25.31.
const ObservedUniqueConstraintsFeature = "schema.observed-unique-constraints"

// uniqueConstraint is the type of a UNIQUE constraint in Table.constraints.
const uniqueConstraint = "UNIQUE"

func validateObservedUniqueConstraintsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ObservedUniqueConstraintsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ObservedUniqueConstraintsFeature) || !EngineOffers(description, engine.GetId(), ObservedConstraintsFeature) {
		return fmt.Errorf("%w: observed UNIQUE constraints require the provider's feature and observed constraints", ErrProtocol)
	}
	return nil
}

// validateObservedUnique checks a UNIQUE constraint: its ordered columns, a
// foreign key's and a CHECK's attributes absent, and its deferral. A column a
// UNIQUE over it alone covers states that it is unique, as a read of the
// column reports it without the constraint.
func validateObservedUnique(table *protocol.Table, unique *protocol.ObservedConstraint) error {
	name := table.GetIdentity().GetName()
	if len(unique.GetColumnNames()) == 0 {
		return constraintsError(name, unique.GetName(), "is a UNIQUE constraint without its columns")
	}
	if unique.ForeignTable != nil || unique.GetForeignSchema() != "" || unique.ForeignColumn != nil || len(unique.GetForeignColumns()) != 0 ||
		unique.DeleteRule != nil || unique.UpdateRule != nil || unique.GetMatch() != "" || unique.CheckClause != nil {
		return constraintsError(name, unique.GetName(), "is a UNIQUE constraint with foreign key or CHECK attributes")
	}
	if unique.GetInitially() != "" && (!unique.GetDeferrable() || (unique.GetInitially() != "DEFERRED" && unique.GetInitially() != "IMMEDIATE")) {
		return constraintsError(name, unique.GetName(), "has invalid deferral")
	}
	if len(unique.GetColumnNames()) != 1 {
		return nil
	}
	for _, column := range table.GetColumns() {
		if column.GetName() == unique.GetColumnNames()[0] && !column.GetObserved().GetUnique() {
			return constraintsError(name, unique.GetName(), fmt.Sprintf("covers column %q alone, which does not state that it is unique", column.GetName()))
		}
	}
	return nil
}
