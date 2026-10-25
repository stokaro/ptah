package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ValidateSchemaFeature advertises target-owned offline schema validation.
const ValidateSchemaFeature = "schema.validate"

// SchemaValidator checks the complete captured declaration without loading
// source documents, opening sessions, or producing executable operations.
type SchemaValidator interface {
	ValidateSchema(context.Context, *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error)
}

// ValidateSchemaRequest checks the negotiated service, captured declarations,
// identities, and model provenance before invoking provider validation.
func ValidateSchemaRequest(description *protocol.Description, request *protocol.ValidateSchemaRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), ValidateSchemaFeature) {
		return fmt.Errorf("%w: schema validation is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, request.Engine, request.TargetFeatures)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), ValidateSchemaFeature) {
		return fmt.Errorf("%w: engine schema validation is unavailable", ErrProtocol)
	}
	captured := validationContext(request)
	if err := validatePlanningContext(description, captured); err != nil {
		return err
	}
	for _, table := range request.Tables {
		if table.GetDeclaration() == nil {
			return fmt.Errorf("%w: schema validation requires a table declaration", ErrProtocol)
		}
		if err := validatePreparationIdentity(table.Subject, table.Declaration.Name, table.Declaration.Schema, request.Identifiers.GetDefaultSchema()); err != nil {
			return err
		}
	}
	if err := validatePlanningTables(description, captured, engine); err != nil {
		return err
	}
	if err := validateNamespaceDeclarations(request.Namespaces); err != nil {
		return err
	}
	if err := validateValidationCommonObjects(engine, request); err != nil {
		return err
	}
	for _, declared := range request.Enums {
		if !comparisonReason(declared.GetName()) {
			return fmt.Errorf("%w: missing declared enum name", ErrProtocol)
		}
	}
	for _, limit := range request.NotDescribed {
		if !comparisonReason(limit.GetKind()) {
			return fmt.Errorf("%w: invalid schema coverage limit", ErrProtocol)
		}
	}
	for name, dependencies := range request.Dependencies {
		if !comparisonReason(name) || dependencies == nil {
			return fmt.Errorf("%w: invalid schema dependency capture", ErrProtocol)
		}
		if err := validateComparisonNames(dependencies.Tables); err != nil {
			return err
		}
	}
	return validateCapturedFeatures(description, captured, engine, protocol.ValueInput_DESIRED, request.Features)
}

func validationContext(request *protocol.ValidateSchemaRequest) *protocol.PlanFeaturesRequest {
	result := &protocol.PlanFeaturesRequest{Engine: request.Engine, Identifiers: request.Identifiers, Capabilities: request.Capabilities, TargetFeatures: request.TargetFeatures}
	for _, table := range request.Tables {
		result.Tables = append(result.Tables, &protocol.PlanningTable{Subject: table.GetSubject(), Desired: table.GetDeclaration(), Action: protocol.PlanningTable_CONTEXT})
	}
	return result
}

func validateNamespaceDeclarations(namespaces []*protocol.DeclaredNamespace) error {
	identities := make([]*protocol.ComparisonIdentity, 0, len(namespaces))
	for _, namespace := range namespaces {
		subject := namespace.GetSubject()
		if subject.GetKind() != "schema" || !comparisonReason(namespace.GetName()) || subject.GetName().GetSource() != namespace.GetName() {
			return fmt.Errorf("%w: namespace declaration differs from its subject", ErrProtocol)
		}
		identities = append(identities, subject)
	}
	return validateComparisonIdentities(identities)
}

// ValidateSchemaResult requires a completed check with typed diagnostics.
// A malformed suffix invalidates the entire result, including earlier messages.
func ValidateSchemaResult(description *protocol.Description, request *protocol.ValidateSchemaRequest, result *protocol.SchemaValidation) error {
	if err := ValidateSchemaRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.Complete {
		return fmt.Errorf("%w: schema validation did not complete", ErrProtocol)
	}
	for _, diagnostic := range result.Diagnostics {
		switch diagnostic.GetCode() {
		case protocol.SchemaDiagnostic_INVALID_SCHEMA, protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE, protocol.SchemaDiagnostic_OMITTED_DECLARATION:
		default:
			return fmt.Errorf("%w: unknown schema diagnostic code", ErrProtocol)
		}
		if !comparisonReason(diagnostic.GetKind()) || !comparisonReason(diagnostic.GetMessage()) {
			return fmt.Errorf("%w: schema diagnostic requires kind and message", ErrProtocol)
		}
	}
	return nil
}

// validateValidationCommonObjects checks the declared common objects a
// validation carries, on an engine that carries them.
func validateValidationCommonObjects(engine *protocol.Engine, request *protocol.ValidateSchemaRequest) error {
	if request.GetCommonObjects() == nil {
		return nil
	}
	kinds := engineCommonKinds(engine)
	if len(kinds) == 0 {
		return commonObjectError("the engine carries no common object")
	}
	return validateDeclaredCommonObjects(request.GetIdentifiers(), kinds, request.GetCommonObjects())
}
