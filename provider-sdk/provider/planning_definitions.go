package provider

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateDeclaredColumn(column *protocol.DeclaredColumn) error {
	if !comparisonReason(column.GetName()) || !comparisonReason(column.GetType()) {
		return fmt.Errorf("%w: declared planning column requires name and type", ErrProtocol)
	}
	return nil
}

func validateDeclaredTable(table *protocol.DeclaredTable) error {
	names := make([]string, 0, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		if err := validateDeclaredColumn(column); err != nil {
			return err
		}
		names = append(names, column.GetName())
	}
	if err := validateComparisonNames(names); err != nil {
		return err
	}
	for _, values := range [][]string{table.GetPrimaryKey(), table.GetPrimaryKeyInclude(), table.GetDependsOn()} {
		if err := validateComparisonNames(values); err != nil {
			return err
		}
	}
	for _, value := range table.GetConstraints() {
		if value == nil || !comparisonReason(value.GetType()) {
			return fmt.Errorf("%w: invalid declared planning constraint", ErrProtocol)
		}
		if err := validateComparisonIdentities(value.GetRequires()); err != nil {
			return err
		}
	}
	for _, value := range table.GetIndexes() {
		if value == nil || !comparisonReason(value.GetName()) {
			return fmt.Errorf("%w: invalid declared planning index", ErrProtocol)
		}
		if err := validateComparisonIdentities(value.GetRequires()); err != nil {
			return err
		}
	}
	for _, value := range table.GetEnums() {
		if value == nil || !comparisonReason(value.GetName()) {
			return fmt.Errorf("%w: invalid declared planning enum", ErrProtocol)
		}
	}
	return validatePlanningTriggers(table.GetTriggers())
}

func validateObservedTable(table *protocol.ObservedTable) error {
	names := make([]string, 0, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		if column == nil || !comparisonReason(column.GetName()) {
			return fmt.Errorf("%w: invalid observed planning column", ErrProtocol)
		}
		names = append(names, column.GetName())
	}
	if err := validateComparisonNames(names); err != nil {
		return err
	}
	for _, value := range table.GetConstraints() {
		if value == nil || !comparisonReason(value.GetType()) {
			return fmt.Errorf("%w: invalid observed planning constraint", ErrProtocol)
		}
		if err := validateComparisonIdentities(value.GetRequires()); err != nil {
			return err
		}
	}
	for _, value := range table.GetIndexes() {
		if value == nil || !comparisonReason(value.GetName()) {
			return fmt.Errorf("%w: invalid observed planning index", ErrProtocol)
		}
		if err := validateComparisonIdentities(value.GetRequires()); err != nil {
			return err
		}
		if value.GetManagedBy() != nil {
			if err := ValidateComparisonIdentity(value.GetManagedBy()); err != nil {
				return err
			}
		}
	}
	return validatePlanningTriggers(table.GetTriggers())
}

func validatePlanningTriggers(triggers []*protocol.CapturedTrigger) error {
	names := make([]string, 0, len(triggers))
	for _, value := range triggers {
		names = append(names, value.GetName())
	}
	return validateComparisonNames(names)
}
