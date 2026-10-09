package featureplan

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// DeclarationRequest lowers authored standalone objects into creation operations.
// It describes a schema document, without asserting inspected absence in a live
// database. Tables supplies declared dependencies; CommonSteps describes the
// host's creation operations. Process adapters map these local types to explicit
// protocol records, rather than serializing the common Go schema model.
type DeclarationRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Objects      []schemaext.Object
	Tables       []schemacapture.TableDeclaration
	CommonSteps  []CommonStep
}

// DeclarationPlan accounts for one input object, in input order. Strategy
// explains how its definition is created. Steps names contributed operations;
// an empty list explicitly relies on common creation operations. Every
// contributed operation must be named by at least one declaration receipt.
type DeclarationPlan struct {
	Subject  objectidentity.ID
	Strategy string
	Steps    []plangraph.StepID
}

// DeclarationDiagnostic is a completed refusal. Object optionally identifies a
// zero-based input index; its Problem.Kind must match that object's model kind.
type DeclarationDiagnostic struct {
	Problem schemavalidation.Diagnostic
	Object  *int
}

// Clone returns independent diagnostic data.
func (d DeclarationDiagnostic) Clone() DeclarationDiagnostic {
	if d.Object != nil {
		d.Object = new(*d.Object)
	}
	return d
}

// DeclarationResult accounts for a complete authored batch. Any refusal carries
// only diagnostics, never a successful prefix. Contributions must join the
// complete host graph before any payload is rendered or executed.
type DeclarationResult struct {
	Complete      bool
	Contributions []plangraph.Contribution[Operation]
	Declarations  []DeclarationPlan
	Diagnostics   []DeclarationDiagnostic
}

// ValidateOutcome checks completion and refusal data. The selected runtime also
// validates codecs, ownership, and every successful declaration receipt.
func (r DeclarationResult) ValidateOutcome(request DeclarationRequest) error {
	if !r.Complete {
		return fmt.Errorf("%w: declaration planning did not complete", schemaext.ErrInvalidValue)
	}
	if len(r.Diagnostics) != 0 && (len(r.Contributions) != 0 || len(r.Declarations) != 0) {
		return fmt.Errorf("%w: refused declaration batch contains output", schemaext.ErrInvalidValue)
	}
	for _, diagnostic := range r.Diagnostics {
		if err := (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{diagnostic.Problem}}).Validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
		if diagnostic.Object != nil && (*diagnostic.Object < 0 || *diagnostic.Object >= len(request.Objects)) {
			return fmt.Errorf("%w: declaration diagnostic object is outside the batch", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

// Err converts a validated completed refusal into a local schema error. A nil
// error still requires runtime receipt validation and complete graph scheduling.
func (r DeclarationResult) Err(request DeclarationRequest) error {
	if err := r.ValidateOutcome(request); err != nil {
		return err
	}
	problems := make([]schemavalidation.Diagnostic, len(r.Diagnostics))
	for i, diagnostic := range r.Diagnostics {
		problems[i] = diagnostic.Problem
	}
	return (schemavalidation.Result{Complete: true, Diagnostics: problems}).Err(request.Target)
}

// DeclarationService plans one authored batch without I/O or input mutation.
// Implementations are safe for concurrent calls and honor cancellation. Errors
// mean the service could not finish; semantic refusals use complete diagnostics.
type DeclarationService interface {
	PlanDeclarations(context.Context, DeclarationRequest) (DeclarationResult, error)
}

// DeclarationRuntime combines explicit owner dispatch with local model codecs.
// A missing owner is an error; implementations never select a built-in fallback.
type DeclarationRuntime interface {
	schemaext.ModelRuntime
	DeclarationService
}
