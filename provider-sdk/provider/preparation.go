package provider

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
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

// ValidatePreparedTables refuses incomplete, reordered, or unbound results,
// and index methods that were not negotiated or that do not state one kind
// for each declared index. A failed batch exposes no prefix, even when
// earlier tables were valid.
func ValidatePreparedTables(description *protocol.Description, request *protocol.PrepareTablesRequest, result *protocol.PreparedTables) error {
	if err := ValidatePreparationRequest(description, request); err != nil {
		return err
	}
	return ValidatePreparedTablesReply(description, request, result)
}

// ValidatePreparedTablesReply is [ValidatePreparedTables] for a request that
// [ValidatePreparationRequest] accepted with description and that has not
// changed since: it checks the reply and not the request again. The SDK's
// client and server validate each request once and call it for the reply.
func ValidatePreparedTablesReply(description *protocol.Description, request *protocol.PrepareTablesRequest, result *protocol.PreparedTables) error {
	if result == nil || !result.Complete || len(result.Tables) != len(request.Tables) {
		return fmt.Errorf("%w: incomplete table preparation", ErrProtocol)
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	// The resolved facets of every table are checked in one context, built
	// once: built for each table, it held every table of the request.
	values := &protocol.CompareModelsRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(), Identifiers: request.GetIdentifiers()}
	for _, model := range engine.ValueModels {
		if model.Representation == protocol.ValueInput_DESIRED {
			values.Kinds = append(values.Kinds, model.Kind)
		}
	}
	checks := new(targetChecks)
	for i, output := range result.Tables {
		input := request.Tables[i]
		if !proto.Equal(input.Subject, output.GetSubject()) {
			return fmt.Errorf("%w: preparation changed table identity or order", ErrProtocol)
		}
		if err := validateColumnKeys(input.Desired, output); err != nil {
			return err
		}
		if err := validateResolvedFacets(checks, description, values, input, output.ResolvedFacets); err != nil {
			return err
		}
		if err := validatePreparedIndexes(description, engine, request, input, output); err != nil {
			return err
		}
	}
	return nil
}

type columnKeyResult interface {
	GetColumnPrimaryKeysPrepared() bool
	GetColumnPrimaryKeys() []string
}

func validateColumnKeys(input *protocol.DeclaredTable, result columnKeyResult) error {
	prepared, keys := result.GetColumnPrimaryKeysPrepared(), result.GetColumnPrimaryKeys()
	if len(keys) > 0 && !prepared {
		return fmt.Errorf("%w: prepared column keys require a completion receipt", ErrProtocol)
	}
	if err := validateComparisonNames(keys); err != nil {
		return err
	}
	for _, key := range keys {
		if !slices.ContainsFunc(input.Columns, func(column *protocol.DeclaredColumn) bool { return column.Name == key }) {
			return fmt.Errorf("%w: table result names an undeclared key column", ErrProtocol)
		}
	}
	return nil
}

// validateResolvedFacets checks the facets a preparation resolved for input
// in values, the desired context of the request's engine.
func validateResolvedFacets(checks *targetChecks, description *protocol.Description, values *protocol.CompareModelsRequest, input *protocol.TablePreparationInput,
	records []*protocol.ComparisonRecord,
) error {
	service := &protocol.ModelComparison{Placement: protocol.ModelComparison_FACETS, OwnerKinds: tableFacetOwnerKinds}
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
	return checkComparisonState(checks, description, values, service, protocol.ValueInput_DESIRED, state)
}
