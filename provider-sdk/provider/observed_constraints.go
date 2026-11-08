package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ObservedConstraintsFeature enables Table.constraints in inspection: the
// FOREIGN KEY and CHECK constraints a table holds, each named and bound to the
// table. The list is the whole set, so a constraint of those types that it
// does not name is absent and a host can compare the declared constraints
// with the held ones. A foreign key states its rules in the spelling of
// information_schema (NO ACTION, RESTRICT, CASCADE, SET NULL, SET DEFAULT),
// which the host compares as it compares every engine's. It is a two-way
// feature: the provider and each engine that reports constraints list it,
// and the engine needs ObservedColumnsFeature, against whose columns the
// SDK checks each constraint. Contract 25.28.
const ObservedConstraintsFeature = "schema.observed-constraints"

// The constraint types Table.constraints carries.
const (
	foreignKeyConstraint = "FOREIGN KEY"
	checkConstraint      = "CHECK"
)

// referentialRules are the spellings of a foreign key's rule an observed
// constraint may carry: the ones information_schema reports.
var referentialRules = []string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}

func constraintsError(table, constraint, reason string) error {
	return fmt.Errorf("%w: observed constraint %q of table %q %s", ErrProtocol, constraint, table, reason)
}

func validateObservedConstraintsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ObservedConstraintsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ObservedConstraintsFeature) || !EngineOffers(description, engine.GetId(), ObservedColumnsFeature) {
		return fmt.Errorf("%w: observed constraints require the provider's feature and complete column observations", ErrProtocol)
	}
	return nil
}

// validateInspectedConstraints refuses constraints in a snapshot whose session
// did not negotiate them, and checks each constraint of one that did.
func validateInspectedConstraints(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	negotiated := EngineOffers(description, engine.GetId(), ObservedConstraintsFeature) && slices.Contains(snapshot.GetTargetFeatures(), ObservedConstraintsFeature)
	if negotiated && !slices.Contains(snapshot.GetTargetFeatures(), ObservedColumnsFeature) {
		return fmt.Errorf("%w: observed constraints require negotiated column observations", ErrProtocol)
	}
	unique := negotiated && EngineOffers(description, engine.GetId(), ObservedUniqueConstraintsFeature) &&
		slices.Contains(snapshot.GetTargetFeatures(), ObservedUniqueConstraintsFeature)
	for _, table := range snapshot.GetTables() {
		if !negotiated {
			if len(table.GetConstraints()) != 0 {
				return fmt.Errorf("%w: observed constraints were not negotiated", ErrProtocol)
			}
			continue
		}
		if err := validateInspectedTableConstraints(table, unique); err != nil {
			return err
		}
	}
	return nil
}

// validateInspectedTableConstraints checks the constraints of one table of a
// negotiated read. unique admits UNIQUE constraints beside the foreign keys
// and CHECKs.
func validateInspectedTableConstraints(table *protocol.Table, unique bool) error {
	columns := make(map[string]bool, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		columns[column.GetName()] = true
	}
	name := table.GetIdentity().GetName()
	names := make(map[string]bool, len(table.GetConstraints()))
	for _, constraint := range table.GetConstraints() {
		if constraint == nil || constraint.GetName() == "" || names[constraint.GetName()] {
			return fmt.Errorf("%w: observed constraint of table %q requires a name unique within its table", ErrProtocol, name)
		}
		names[constraint.GetName()] = true
		if constraint.GetTableName() != name || constraint.GetSchema() != table.GetIdentity().GetSchema() {
			return constraintsError(name, constraint.GetName(), "has an invalid table binding")
		}
		if constraint.GetColumnName() != "" {
			return constraintsError(name, constraint.GetName(), "names its columns in the singular field; it lists them in column_names")
		}
		if err := validateConstraintColumns(name, constraint, constraint.GetColumnNames(), columns); err != nil {
			return err
		}
		// NULLS [NOT] DISTINCT is a UNIQUE constraint's own; the rest are an
		// index's.
		if (constraint.NullsDistinct != nil && constraint.GetType() != uniqueConstraint) || len(constraint.GetIncludeColumns()) != 0 ||
			constraint.UsingMethod != nil || constraint.GetHashShardBuckets() != 0 {
			return constraintsError(name, constraint.GetName(), "carries index attributes")
		}
		if err := validateComparisonIdentities(constraint.GetRequires()); err != nil {
			return err
		}
		if err := validateConstraintType(table, constraint, unique); err != nil {
			return err
		}
	}
	return nil
}

// validateConstraintType checks a constraint by its type: a FOREIGN KEY, a
// CHECK, and a UNIQUE constraint where unique admits one.
func validateConstraintType(table *protocol.Table, constraint *protocol.ObservedConstraint, unique bool) error {
	name := table.GetIdentity().GetName()
	switch kind := constraint.GetType(); {
	case kind == foreignKeyConstraint:
		return validateObservedForeignKey(name, constraint)
	case kind == checkConstraint:
		return validateObservedCheck(name, constraint)
	case unique && kind == uniqueConstraint:
		return validateObservedUnique(table, constraint)
	case unique:
		return constraintsError(name, constraint.GetName(), "is not a FOREIGN KEY, a CHECK or a UNIQUE constraint")
	default:
		return constraintsError(name, constraint.GetName(), "is neither a FOREIGN KEY nor a CHECK")
	}
}

// validateConstraintColumns refuses a column the table does not hold, and one
// named twice.
func validateConstraintColumns(table string, constraint *protocol.ObservedConstraint, names []string, columns map[string]bool) error {
	seen := make(map[string]bool, len(names))
	for _, column := range names {
		if !columns[column] || seen[column] {
			return constraintsError(table, constraint.GetName(), "names an unknown or repeated column")
		}
		seen[column] = true
	}
	return nil
}

// validateObservedForeignKey checks a foreign key: its ordered local columns,
// the table it references and as many referenced columns, its rules in the
// spelling of information_schema, its MATCH type and its deferral.
func validateObservedForeignKey(table string, key *protocol.ObservedConstraint) error {
	if len(key.GetColumnNames()) == 0 || key.GetForeignTable() == "" || key.ForeignColumn != nil ||
		len(key.GetForeignColumns()) != len(key.GetColumnNames()) || slices.Contains(key.GetForeignColumns(), "") {
		return constraintsError(table, key.GetName(), "requires its columns, the referenced table and as many referenced columns")
	}
	if key.CheckClause != nil {
		return constraintsError(table, key.GetName(), "is a foreign key with a CHECK clause")
	}
	for _, rule := range []*string{key.DeleteRule, key.UpdateRule} {
		if rule != nil && !slices.Contains(referentialRules, *rule) {
			return constraintsError(table, key.GetName(), fmt.Sprintf("has rule %q; a rule is one of %s", *rule, strings.Join(referentialRules, ", ")))
		}
	}
	if key.GetMatch() != "" && key.GetMatch() != "FULL" && key.GetMatch() != "PARTIAL" {
		return constraintsError(table, key.GetName(), fmt.Sprintf("has MATCH type %q; it is FULL, PARTIAL or empty for SIMPLE", key.GetMatch()))
	}
	if key.GetInitially() != "" && (!key.GetDeferrable() || (key.GetInitially() != "DEFERRED" && key.GetInitially() != "IMMEDIATE")) {
		return constraintsError(table, key.GetName(), "has invalid deferral")
	}
	return nil
}

// validateObservedCheck checks a CHECK: its expression, and none of a foreign
// key's attributes. The columns it lists, if any, are the ones its expression
// reads.
func validateObservedCheck(table string, check *protocol.ObservedConstraint) error {
	if strings.TrimSpace(check.GetCheckClause()) == "" {
		return constraintsError(table, check.GetName(), "is a CHECK without its expression")
	}
	if check.ForeignTable != nil || check.GetForeignSchema() != "" || check.ForeignColumn != nil || len(check.GetForeignColumns()) != 0 ||
		check.DeleteRule != nil || check.UpdateRule != nil || check.GetMatch() != "" || check.GetDeferrable() || check.GetInitially() != "" {
		return constraintsError(table, check.GetName(), "is a CHECK with foreign key attributes")
	}
	return nil
}
