package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// PrepareTablesFeature advertises target-owned offline table preparation.
const PrepareTablesFeature = "schema.prepare-tables"

// TablePreparer resolves comparison inputs without inspecting a server. The
// result may change key membership and declared facet values, not source state.
type TablePreparer interface {
	PrepareTables(context.Context, *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error)
}

// ValidatePreparationRequest checks captured table identity, existence evidence,
// negotiated models, and the target profile before invoking owner code.
func ValidatePreparationRequest(description *protocol.Description, request *protocol.PrepareTablesRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), PrepareTablesFeature) {
		return fmt.Errorf("%w: table preparation is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), PrepareTablesFeature) {
		return fmt.Errorf("%w: engine table preparation is unavailable", ErrProtocol)
	}
	captured := preparationContext(request)
	if err := validatePlanningContext(description, captured); err != nil {
		return err
	}
	for _, table := range request.Tables {
		if table.GetDesired() == nil {
			return fmt.Errorf("%w: preparation requires a declared table", ErrProtocol)
		}
		if err := validatePreparationIdentity(table.Subject, table.Desired.Name, table.Desired.Schema, request.Identifiers.GetDefaultSchema()); err != nil {
			return err
		}
		switch table.GetCurrentKnowledge().GetState() {
		case protocol.ModelKnowledge_COMPLETE:
			if table.Current == nil {
				return fmt.Errorf("%w: complete table knowledge requires a current capture", ErrProtocol)
			}
		case protocol.ModelKnowledge_ABSENT, protocol.ModelKnowledge_UNINSPECTED:
			if table.Current != nil {
				return fmt.Errorf("%w: table existence evidence disagrees with its current capture", ErrProtocol)
			}
		default:
			return fmt.Errorf("%w: invalid table existence knowledge", ErrProtocol)
		}
		if err := validateKnowledge(protocol.ValueInput_OBSERVED, table.CurrentKnowledge); err != nil {
			return err
		}
	}
	return validatePlanningTables(description, captured, engine)
}

func preparationContext(request *protocol.PrepareTablesRequest) *protocol.PlanFeaturesRequest {
	result := &protocol.PlanFeaturesRequest{Engine: request.GetEngine(), Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities(), TargetFeatures: request.GetTargetFeatures()}
	for _, table := range request.GetTables() {
		result.Tables = append(result.Tables, &protocol.PlanningTable{Subject: table.GetSubject(), Desired: table.GetDesired(), Current: table.GetCurrent(), Action: protocol.PlanningTable_CONTEXT})
	}
	return result
}

func validatePreparationIdentity(subject *protocol.ComparisonIdentity, name, namespace, defaultSchema string) error {
	if err := ValidateComparisonIdentity(subject); err != nil {
		return err
	}
	schemaMatches := subject.GetSchema().GetSource() == namespace || (namespace == "" && subject.GetSchema().GetDefaulted() && subject.GetSchema().GetSource() == defaultSchema)
	if subject.Kind != "table" || subject.GetName().GetSource() != name || !schemaMatches {
		return fmt.Errorf("%w: captured table identity differs from its subject", ErrProtocol)
	}
	return nil
}

// ValidatePreparedTables refuses incomplete, reordered, or unbound results.
// A failed batch exposes no prefix, even when earlier tables were valid.
func ValidatePreparedTables(description *protocol.Description, request *protocol.PrepareTablesRequest, result *protocol.PreparedTables) error {
	if err := ValidatePreparationRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.Complete || len(result.Tables) != len(request.Tables) {
		return fmt.Errorf("%w: incomplete table preparation", ErrProtocol)
	}
	for i, output := range result.Tables {
		input := request.Tables[i]
		if !proto.Equal(input.Subject, output.GetSubject()) {
			return fmt.Errorf("%w: preparation changed table identity or order", ErrProtocol)
		}
		if err := validatePreparedKeys(input.Desired, output); err != nil {
			return err
		}
		if err := validateResolvedFacets(description, request, input, output.ResolvedFacets); err != nil {
			return err
		}
	}
	return nil
}

func validatePreparedKeys(input *protocol.DeclaredTable, output *protocol.PreparedTable) error {
	if len(output.ColumnPrimaryKeys) > 0 && !output.ColumnPrimaryKeysPrepared {
		return fmt.Errorf("%w: prepared column keys require a completion receipt", ErrProtocol)
	}
	if err := validateComparisonNames(output.ColumnPrimaryKeys); err != nil {
		return err
	}
	for _, key := range output.ColumnPrimaryKeys {
		if !slices.ContainsFunc(input.Columns, func(column *protocol.DeclaredColumn) bool { return column.Name == key }) {
			return fmt.Errorf("%w: preparation names an undeclared key column", ErrProtocol)
		}
	}
	return nil
}

func validateResolvedFacets(description *protocol.Description, request *protocol.PrepareTablesRequest, input *protocol.TablePreparationInput, records []*protocol.ComparisonRecord) error {
	engine, _, err := valueTarget(description, request.Engine, request.TargetFeatures)
	if err != nil {
		return err
	}
	values := planningValueContext(preparationContext(request))
	service := &protocol.ModelComparison{Placement: protocol.ModelComparison_FACETS, OwnerKinds: []string{"table", "index", "primary key", "constraint"}}
	for _, model := range engine.ValueModels {
		if model.Representation == protocol.ValueInput_DESIRED {
			values.Kinds = append(values.Kinds, model.Kind)
		}
	}
	for _, record := range records {
		if record.GetValue() == nil || len(record.GetTargets()) != 0 {
			return fmt.Errorf("%w: resolved facets require values without target overrides", ErrProtocol)
		}
		index := slices.IndexFunc(input.Desired.GetFeatures().GetFacets().GetRecords(), func(original *protocol.ComparisonRecord) bool {
			return original.GetValue() != nil && original.Kind == record.Kind && proto.Equal(original.Subject, record.Subject)
		})
		if index < 0 {
			return fmt.Errorf("%w: preparation resolved an undeclared or excluded facet", ErrProtocol)
		}
		original := input.Desired.Features.Facets.Records[index].Value
		if !proto.Equal(original.GetValue().GetOwner(), record.Value.GetValue().GetOwner()) || !proto.Equal(original.NormalizedOwner, record.Value.NormalizedOwner) {
			return fmt.Errorf("%w: preparation changed a facet owner", ErrProtocol)
		}
	}
	state := &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}, Records: records}
	return validateComparisonState(description, values, service, protocol.ValueInput_DESIRED, state)
}
