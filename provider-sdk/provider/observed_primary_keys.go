package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ObservedPrimaryKeysFeature enables complete ordered primary keys in inspected
// tables. A missing record establishes absence, and column membership must agree.
const ObservedPrimaryKeysFeature = "schema.observed-primary-keys"

func validateObservedPrimaryKeysFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ObservedPrimaryKeysFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ObservedPrimaryKeysFeature) || !EngineOffers(description, engine.GetId(), ObservedColumnsFeature) {
		return fmt.Errorf("%w: observed primary keys require complete column observations", ErrProtocol)
	}
	return nil
}

func validateInspectedPrimaryKeys(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	negotiated := EngineOffers(description, engine.GetId(), ObservedPrimaryKeysFeature) && slices.Contains(snapshot.GetTargetFeatures(), ObservedPrimaryKeysFeature)
	for _, table := range snapshot.GetTables() {
		if !negotiated {
			if table.GetPrimaryKey() != nil {
				return fmt.Errorf("%w: observed primary key was not negotiated", ErrProtocol)
			}
			continue
		}
		if !slices.Contains(snapshot.GetTargetFeatures(), ObservedColumnsFeature) {
			return fmt.Errorf("%w: observed primary keys require negotiated column observations", ErrProtocol)
		}
		if err := validateInspectedPrimaryKey(table); err != nil {
			return err
		}
	}
	return nil
}

func validateInspectedPrimaryKey(table *protocol.Table) error {
	key := table.GetPrimaryKey()
	columns := make(map[string]bool, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		columns[column.GetName()] = true
		if column.GetObserved().GetPrimaryKey() != slices.Contains(key.GetColumnNames(), column.GetName()) {
			return fmt.Errorf("%w: observed primary key disagrees with column membership", ErrProtocol)
		}
	}
	if key == nil {
		return nil
	}
	if key.GetType() != "PRIMARY KEY" || key.GetTableName() != table.GetIdentity().GetName() || key.GetSchema() != table.GetIdentity().GetSchema() {
		return fmt.Errorf("%w: observed primary key has invalid type or table binding", ErrProtocol)
	}
	if len(key.GetColumnNames()) == 0 || key.GetColumnName() != "" {
		return fmt.Errorf("%w: observed primary key requires its ordered column list", ErrProtocol)
	}
	seen := make(map[string]bool)
	for _, group := range [][]string{key.GetColumnNames(), key.GetIncludeColumns()} {
		for _, name := range group {
			if !columns[name] || seen[name] {
				return fmt.Errorf("%w: observed primary key names an unknown or repeated column", ErrProtocol)
			}
			seen[name] = true
		}
	}
	return validatePrimaryKeyAttributes(key)
}

func validatePrimaryKeyAttributes(key *protocol.ObservedConstraint) error {
	if key.ForeignTable != nil || key.GetForeignSchema() != "" || key.ForeignColumn != nil || len(key.GetForeignColumns()) != 0 ||
		key.DeleteRule != nil || key.UpdateRule != nil || key.GetMatch() != "" || key.GetNotValid() || key.CheckClause != nil ||
		key.NullsDistinct != nil || len(key.GetRequires()) != 0 {
		return fmt.Errorf("%w: observed primary key carries unrelated constraint attributes", ErrProtocol)
	}
	if key.GetInitially() != "" && (!key.GetDeferrable() || (key.GetInitially() != "DEFERRED" && key.GetInitially() != "IMMEDIATE")) {
		return fmt.Errorf("%w: observed primary key has invalid deferral", ErrProtocol)
	}
	if key.GetHashShardBuckets() < 0 || (key.UsingMethod != nil && key.GetUsingMethod() == "") {
		return fmt.Errorf("%w: observed primary key has invalid method or shard count", ErrProtocol)
	}
	return nil
}
