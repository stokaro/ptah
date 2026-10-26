package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// SchemaDeclarationsFeature advertises offline authored creation planning.
// Both peers negotiate the engine's declaration assignments.
const SchemaDeclarationsFeature = "schema.declarations"

// DeclarationPlanner lowers authored objects without reading a database or
// inventing observed absence. The host schedules the complete graph before
// rendering. A completed refusal returns diagnostics without successful output.
type DeclarationPlanner interface {
	PlanDeclarations(context.Context, *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error)
}

func declarationError(reason string) error {
	return fmt.Errorf("%w: declaration planning: %s", ErrProtocol, reason)
}

func validateModelDeclarations(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), SchemaDeclarationsFeature)
	if listed != (len(engine.GetDeclarations()) != 0) || listed &&
		(!slices.Contains(description.GetFeatures(), SchemaDeclarationsFeature) || !slices.Contains(description.GetFeatures(), NormalizeValuesFeature)) {
		return declarationError("assignments require the engine feature and the provider's declaration and normalization services")
	}
	services, assigned := make(map[string]bool), make(map[string]bool)
	for _, service := range engine.GetDeclarations() {
		if !validName(service.GetId()) || services[service.GetId()] || len(service.GetKinds()) == 0 {
			return declarationError("a service needs a unique ID and assigned kinds")
		}
		services[service.GetId()] = true
		for _, kinds := range [][]string{service.GetKinds(), service.GetOperationKinds()} {
			if err := validateComparisonNames(kinds); err != nil {
				return err
			}
		}
		for _, kind := range service.GetKinds() {
			if assigned[kind] || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_DESIRED) == nil {
				return declarationError("a desired kind has no model or is assigned twice")
			}
			assigned[kind] = true
		}
		for _, kind := range service.GetOperationKinds() {
			if findValueModel(description, engine.GetId(), kind, protocol.ValueInput_OPERATION) == nil {
				return declarationError("an operation kind has no model")
			}
		}
	}
	return nil
}

func declarationService(engine *protocol.Engine, id string) *protocol.ModelDeclarations {
	for _, service := range engine.GetDeclarations() {
		if service.GetId() == id {
			return service
		}
	}
	return nil
}

// ValidateDeclarationsRequest checks explicit dispatch, authored models and
// declared context before invoking owner code. It establishes no current state.
func ValidateDeclarationsRequest(description *protocol.Description, request *protocol.PlanDeclarationsRequest) error {
	if err := requireResolvedContext(request); err != nil {
		return err
	}
	if request.GetEngine() == "" || !slices.Contains(description.GetFeatures(), SchemaDeclarationsFeature) {
		return declarationError("an explicit engine and advertised service are required")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if err := validateModelDeclarations(description, engine); err != nil {
		return err
	}
	service := declarationService(engine, request.GetDeclarationId())
	if service == nil {
		return declarationError("the service is unavailable")
	}
	planning := declarationContext(request)
	if err := validatePlanningContext(description, planning); err != nil {
		return err
	}
	if err := validatePlanningCommonObjects(description, engine, planning); err != nil {
		return err
	}
	if err := validateDeclarationTables(request); err != nil {
		return err
	}
	if err := validatePlanningTables(description, planning, engine); err != nil {
		return err
	}
	var identities []*protocol.ComparisonIdentity
	var values []*protocol.NormalizedValue
	for _, object := range request.GetObjects() {
		if !slices.Contains(service.GetKinds(), object.GetKind()) || object.GetValue() == nil || object.GetValue().GetValue().GetKind() != object.GetKind() || object.GetSubject().GetKind() != object.GetKind() {
			return declarationError("an object needs an assigned desired value and its model kind as subject kind")
		}
		if !declarationIdentityMatches(request, object) {
			return declarationError("an object's subject differs from its value owner")
		}
		if err := validateComparisonNames(object.GetTargets()); err != nil {
			return err
		}
		identities = append(identities, object.GetSubject())
		values = append(values, object.GetValue())
	}
	if err := validateComparisonIdentities(identities); err != nil {
		return err
	}
	return validateComparisonValues(description, planningValueContext(planning), protocol.ValueInput_DESIRED, values)
}

func declarationContext(request *protocol.PlanDeclarationsRequest) *protocol.PlanFeaturesRequest {
	planning := &protocol.PlanFeaturesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(), Identifiers: request.GetIdentifiers(),
		Capabilities: request.GetCapabilities(), DesiredTypes: request.GetTypes(), CommonSteps: request.GetCommonSteps()}
	for _, table := range request.GetTables() {
		planning.Tables = append(planning.Tables, &protocol.PlanningTable{Subject: table.GetSubject(), Desired: table.GetDeclaration(), Action: protocol.PlanningTable_CONTEXT})
	}
	return planning
}

// ValidatePlannedDeclarations requires complete object receipts and accounted
// operations. A diagnostic batch carries no successful prefix.
func ValidatePlannedDeclarations(description *protocol.Description, request *protocol.PlanDeclarationsRequest, result *protocol.PlannedDeclarations) error {
	if err := requireResolvedContext(result); err != nil {
		return err
	}
	if err := ValidateDeclarationsRequest(description, request); err != nil {
		return err
	}
	if !result.GetComplete() {
		return declarationError("incomplete result")
	}
	if len(result.GetDiagnostics()) != 0 {
		return validateDeclarationDiagnostics(request, result)
	}
	engine, _, _ := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	service := declarationService(engine, request.GetDeclarationId())
	steps, err := validatePlanningContributions(description, declarationContext(request), &protocol.ModelPlanning{OperationKinds: service.GetOperationKinds()}, result.GetContributions())
	if err != nil {
		return err
	}
	if len(result.GetDeclarations()) != len(request.GetObjects()) {
		return declarationError("each object needs one receipt in input order")
	}
	for i, receipt := range result.GetDeclarations() {
		if !proto.Equal(receipt.GetSubject(), request.GetObjects()[i].GetSubject()) || !comparisonReason(receipt.GetStrategy()) {
			return declarationError("a receipt changes its subject or omits its strategy")
		}
		seen := make(map[planningStepKey]bool)
		for _, step := range receipt.GetSteps() {
			key := stepKey(step)
			if _, present := steps[key]; !present || seen[key] {
				return declarationError("a receipt names a missing or repeated step")
			}
			steps[key], seen[key] = true, true
		}
	}
	for _, accounted := range steps {
		if !accounted {
			return declarationError("an operation has no declaration receipt")
		}
	}
	return validateDeclarationDependencies(request, result, steps)
}

func validateDeclarationDependencies(request *protocol.PlanDeclarationsRequest, result *protocol.PlannedDeclarations, steps map[planningStepKey]bool) error {
	known := make(map[planningStepKey]bool)
	for key := range steps {
		known[key] = true
	}
	for _, step := range request.GetCommonSteps() {
		known[stepKey(step.GetId())] = true
	}
	for _, contribution := range result.GetContributions() {
		for _, edge := range contribution.GetDependencies() {
			if !known[stepKey(edge.GetBefore())] || !known[stepKey(edge.GetAfter())] {
				return declarationError("a dependency names a step outside the supplied and contributed graph")
			}
		}
	}
	return nil
}

func validateDeclarationDiagnostics(request *protocol.PlanDeclarationsRequest, result *protocol.PlannedDeclarations) error {
	if len(result.GetDeclarations())+len(result.GetContributions()) != 0 {
		return declarationError("a refused batch contains successful output")
	}
	for _, diagnostic := range result.GetDiagnostics() {
		problem := diagnostic.GetProblem()
		switch problem.GetCode() {
		case protocol.SchemaDiagnostic_INVALID_SCHEMA, protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE, protocol.SchemaDiagnostic_OMITTED_DECLARATION:
		default:
			return declarationError("an unknown diagnostic code")
		}
		if !comparisonReason(problem.GetKind()) || !comparisonReason(problem.GetMessage()) {
			return declarationError("a diagnostic needs its kind and message")
		}
		if diagnostic.Object != nil && (uint64(diagnostic.GetObject()) >= uint64(len(request.GetObjects())) || problem.GetKind() != request.GetObjects()[diagnostic.GetObject()].GetKind()) {
			return declarationError("a diagnostic index or kind differs from its input object")
		}
	}
	return nil
}

func validateDeclarationTables(request *protocol.PlanDeclarationsRequest) error {
	for _, table := range request.GetTables() {
		if table.GetDeclaration() == nil {
			return declarationError("a table needs its declaration")
		}
		if err := validatePreparationIdentity(table.GetSubject(), table.GetDeclaration().GetName(), table.GetDeclaration().GetSchema(), request.GetIdentifiers().GetDefaultSchema()); err != nil {
			return err
		}
	}
	return nil
}
