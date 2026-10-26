package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ObservedColumnsFeature enables complete common column observations in
// inspection. The observation exclusively supplies the wrapper's common facts.
const ObservedColumnsFeature = "schema.observed-columns"

func validateObservedColumnsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ObservedColumnsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ObservedColumnsFeature) || !slices.Contains(description.GetFeatures(), InspectFeature) ||
		!slices.Contains(description.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: observed columns require inspection and value normalization", ErrProtocol)
	}
	return nil
}

func validateInspectedColumns(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	negotiated := EngineOffers(description, engine.GetId(), ObservedColumnsFeature) && slices.Contains(snapshot.GetTargetFeatures(), ObservedColumnsFeature)
	for _, table := range snapshot.GetTables() {
		for i, column := range table.GetColumns() {
			if (column.GetObserved() != nil) != negotiated {
				return fmt.Errorf("%w: complete column observations must match their negotiation", ErrProtocol)
			}
			if err := validateInspectedColumn(column, i+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateInspectedColumn(column *protocol.Column, ordinal int) error {
	observed := column.GetObserved()
	if observed == nil {
		if strings.TrimSpace(column.GetType()) == "" {
			return fmt.Errorf("%w: inspected column type is missing", ErrProtocol)
		}
		return nil
	}
	if column.GetType() != "" || column.GetNullable() || column.GetPrimaryKey() || column.DefaultExpression != nil {
		return fmt.Errorf("%w: complete column observation conflicts with legacy common fields", ErrProtocol)
	}
	if observed.GetName() != column.GetName() || observed.GetOrdinalPosition() != int64(ordinal) || strings.TrimSpace(observed.GetDataType()) == "" ||
		(observed.GetIsNullable() != "YES" && observed.GetIsNullable() != "NO") {
		return fmt.Errorf("%w: complete column observation has invalid identity, order, type or nullability", ErrProtocol)
	}
	return nil
}
