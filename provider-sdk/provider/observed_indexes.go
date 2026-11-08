package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ObservedIndexesFeature enables the complete common secondary indexes of an
// inspected table. The list is the whole set: an index it does not name is
// absent, so a host can compare declared indexes with the held ones.
const ObservedIndexesFeature = "schema.observed-indexes"

func validateObservedIndexesFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ObservedIndexesFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ObservedIndexesFeature) || !EngineOffers(description, engine.GetId(), ObservedColumnsFeature) {
		return fmt.Errorf("%w: observed indexes require complete column observations", ErrProtocol)
	}
	return nil
}

func validateInspectedIndexes(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	negotiated := EngineOffers(description, engine.GetId(), ObservedIndexesFeature) && slices.Contains(snapshot.GetTargetFeatures(), ObservedIndexesFeature)
	if negotiated && !slices.Contains(snapshot.GetTargetFeatures(), ObservedColumnsFeature) {
		return fmt.Errorf("%w: observed indexes require negotiated column observations", ErrProtocol)
	}
	facets := negotiated && EngineOffers(description, engine.GetId(), ObservedIndexFacetsFeature) &&
		slices.Contains(snapshot.GetTargetFeatures(), ObservedIndexFacetsFeature)
	for _, table := range snapshot.GetTables() {
		if !negotiated {
			if len(table.GetIndexes()) != 0 {
				return fmt.Errorf("%w: observed indexes were not negotiated", ErrProtocol)
			}
			continue
		}
		if err := validateInspectedTableIndexes(table); err != nil {
			return err
		}
		for _, index := range table.GetIndexes() {
			if len(index.GetFacets()) != 0 && !facets {
				return fmt.Errorf("%w: observed index facets were not negotiated", ErrProtocol)
			}
			if err := validateInspectedIndexFacets(table, index); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateInspectedTableIndexes(table *protocol.Table) error {
	columns := make(map[string]bool, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		columns[column.GetName()] = true
	}
	names := make(map[string]bool, len(table.GetIndexes()))
	for _, index := range table.GetIndexes() {
		if index.GetName() == "" || names[index.GetName()] {
			return fmt.Errorf("%w: observed index requires a name unique within its table", ErrProtocol)
		}
		names[index.GetName()] = true
		if index.GetTableName() != table.GetIdentity().GetName() || index.GetSchema() != table.GetIdentity().GetSchema() {
			return fmt.Errorf("%w: observed index %q has an invalid table binding", ErrProtocol, index.GetName())
		}
		if index.GetPrimary() {
			return fmt.Errorf("%w: observed index %q is a primary key, which the table's primary key records", ErrProtocol, index.GetName())
		}
		if len(index.GetColumns()) == 0 && len(index.GetParts()) == 0 {
			return fmt.Errorf("%w: observed index %q names no key", ErrProtocol, index.GetName())
		}
		if err := validateObservedIndexColumns(index, columns); err != nil {
			return err
		}
		if index.GetHashShardBuckets() < 0 {
			return fmt.Errorf("%w: observed index %q has an invalid shard count", ErrProtocol, index.GetName())
		}
		identities := slices.Clone(index.GetRequires())
		if index.GetManagedBy() != nil {
			identities = append(identities, index.GetManagedBy())
		}
		for _, identity := range identities {
			if err := ValidateComparisonIdentity(identity); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateObservedIndexColumns refuses a key or covered column the table does
// not hold, and one named twice.
func validateObservedIndexColumns(index *protocol.ObservedIndex, columns map[string]bool) error {
	seen := make(map[string]bool)
	named := slices.Concat(index.GetColumns(), index.GetIncludeColumns())
	for _, part := range index.GetParts() {
		if part.GetColumn() != "" {
			continue
		}
		if part.GetExpression() == "" {
			return fmt.Errorf("%w: observed index %q has a key part with neither column nor expression", ErrProtocol, index.GetName())
		}
	}
	for _, name := range named {
		if !columns[name] || seen[name] {
			return fmt.Errorf("%w: observed index %q names an unknown or repeated column", ErrProtocol, index.GetName())
		}
		seen[name] = true
	}
	return nil
}
