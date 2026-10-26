package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ColumnsFeature enables offline column comparison, default readback, and
// complete effective columns in table-creation predictions on both peers.
const ColumnsFeature = "schema.columns"

// Columns interprets captured common columns without server access. The host
// owns pairing, domain and generated-expression rules, and migration operations.
// Missing facts must produce an error, never a partial successful receipt.
type Columns interface {
	CompareColumns(context.Context, *protocol.CompareColumnsRequest) (*protocol.ComparedColumns, error)
	ReadColumnDefaults(context.Context, *protocol.ReadColumnDefaultsRequest) (*protocol.ReadColumnDefaultsResult, error)
}

func columnsError(reason string) error {
	return fmt.Errorf("%w: column semantics: %s", ErrProtocol, reason)
}

func validateColumnsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), ColumnsFeature) &&
		(!slices.Contains(description.GetFeatures(), ColumnsFeature) ||
			!slices.Contains(description.GetFeatures(), ProjectTableCreationsFeature) ||
			!slices.Contains(engine.GetFeatures(), ProjectTableCreationsFeature)) {
		return columnsError("an engine requires the column and table-creation services")
	}
	return nil
}

func columnsTarget(description *protocol.Description, engineID string, features []string) (*protocol.Engine, error) {
	if engineID == "" || !slices.Contains(description.GetFeatures(), ColumnsFeature) {
		return nil, columnsError("an explicit engine and negotiated service are required")
	}
	engine, _, err := valueTarget(description, engineID, features)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(engine.GetFeatures(), ColumnsFeature) {
		return nil, columnsError("the engine has no column service")
	}
	return engine, validateColumnsFeature(description, engine)
}

// ValidateColumnsRequest checks complete parent captures and exact desired and
// current column bindings. It never pairs names or infers observed absence.
func ValidateColumnsRequest(description *protocol.Description, request *protocol.CompareColumnsRequest) error {
	engine, err := columnsTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	captured := &protocol.PlanFeaturesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
		Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities()}
	if err := validatePlanningContext(description, captured); err != nil {
		return err
	}
	var currentSubjects []*protocol.ComparisonIdentity
	for _, table := range request.GetTables() {
		if table.GetDesired() == nil || table.GetCurrent() == nil {
			return columnsError("both parent captures are required")
		}
		if err := validateColumnParent(table.GetSubject(), table.GetDesired().GetName(), table.GetDesired().GetSchema(), request.GetIdentifiers()); err != nil {
			return err
		}
		if err := validateColumnParent(table.GetCurrentSubject(), table.GetCurrent().GetName(), table.GetCurrent().GetSchema(), request.GetIdentifiers()); err != nil {
			return err
		}
		if err := validateColumnPairs(table, request.GetIdentifiers()); err != nil {
			return err
		}
		captured.Tables = append(captured.Tables, &protocol.PlanningTable{Subject: table.Subject, Desired: table.Desired, Current: table.Current, Action: protocol.PlanningTable_CONTEXT})
		currentSubjects = append(currentSubjects, table.CurrentSubject)
	}
	if err := validateComparisonIdentities(currentSubjects); err != nil {
		return err
	}
	return validatePlanningTables(description, captured, engine)
}

func validateColumnPairs(table *protocol.ColumnComparisonTable, identifiers *protocol.ComparisonIdentifiers) error {
	var desired, current []*protocol.ComparisonIdentity
	for _, pair := range table.GetPairs() {
		if !slices.ContainsFunc(table.GetDesired().GetColumns(), func(column *protocol.DeclaredColumn) bool { return column.GetName() == pair.GetDesiredColumn() }) ||
			!slices.ContainsFunc(table.GetCurrent().GetColumns(), func(column *protocol.ObservedColumn) bool { return column.GetName() == pair.GetCurrentColumn() }) {
			return columnsError("a pair names a column outside its captured parent")
		}
		if err := validateBoundColumn(pair.GetSubject(), table.GetSubject(), pair.GetDesiredColumn(), identifiers); err != nil {
			return err
		}
		if err := validateBoundColumn(pair.GetCurrentSubject(), table.GetCurrentSubject(), pair.GetCurrentColumn(), identifiers); err != nil {
			return err
		}
		if err := validateColumnAspects(pair.GetAspects()); err != nil {
			return err
		}
		for _, column := range table.GetCurrent().GetColumns() {
			if column.GetName() == pair.GetCurrentColumn() {
				if err := validateComparedColumnFacts(column, pair.GetAspects()); err != nil {
					return err
				}
			}
		}
		desired, current = append(desired, pair.Subject), append(current, pair.CurrentSubject)
	}
	if err := validateComparisonIdentities(desired); err != nil {
		return err
	}
	return validateComparisonIdentities(current)
}

func validateColumnAspects(aspects []protocol.ColumnAspect) error {
	if len(aspects) == 0 {
		return columnsError("a pair needs requested aspects")
	}
	for i, aspect := range aspects {
		if aspect < protocol.ColumnAspect_COLUMN_TYPE || aspect > protocol.ColumnAspect_COLUMN_SEQUENCE || slices.Contains(aspects[:i], aspect) {
			return columnsError("an aspect is unknown or repeated")
		}
	}
	return nil
}

func validateComparedColumnFacts(column *protocol.ObservedColumn, aspects []protocol.ColumnAspect) error {
	if slices.Contains(aspects, protocol.ColumnAspect_COLUMN_TYPE) && !hasObservedColumnType(column) {
		return columnsError("type comparison requires a reported current type")
	}
	if slices.Contains(aspects, protocol.ColumnAspect_COLUMN_NULLABILITY) && column.GetIsNullable() != "YES" && column.GetIsNullable() != "NO" {
		return columnsError("nullability comparison requires known current nullability")
	}
	return nil
}

func hasObservedColumnType(column *protocol.ObservedColumn) bool {
	return column.GetDataType() != "" || column.GetFormattedType() != "" || column.GetColumnType() != "" || column.GetUdtName() != ""
}
