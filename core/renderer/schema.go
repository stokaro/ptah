package renderer

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
)

// SchemaRequest renders one complete captured declaration for a target. Schema
// and its nested values are read-only. The service owns target preparation and
// dependency ordering; it must not inspect a server or load source documents.
type SchemaRequest struct {
	Target       string
	Capabilities capability.Capabilities
	Identifiers  identifier.Semantics
	Schema       *schemamodel.Database
}

// SchemaResult contains dependency-ordered SQL and recorded omissions from one
// completed schema render. Statements retain the owner's statement grouping;
// they do not imply transaction boundaries. An empty completed result is valid
// for an empty declaration. A failed render exposes neither SQL nor omissions.
type SchemaResult struct {
	Complete   bool
	Statements []string
	Omissions  []Omission
	// Diagnostics describes a completed refusal. A refused render carries no SQL
	// or omissions; failures to perform rendering are returned as service errors.
	Diagnostics []schemavalidation.Diagnostic
}

// Validate refuses an incomplete render, empty output entries, and malformed
// omission records. It does not prove that SQL implements the input semantics or
// that the provider's omission reporting covers every unsupported property.
func (r SchemaResult) Validate() error {
	if !r.Complete {
		return fmt.Errorf("%w: schema rendering did not complete", ErrInvalidResult)
	}
	if err := (schemavalidation.Result{Complete: true, Diagnostics: r.Diagnostics}).Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if len(r.Diagnostics) != 0 && (len(r.Statements) != 0 || len(r.Omissions) != 0) {
		return fmt.Errorf("%w: refused schema render contains output", ErrInvalidResult)
	}
	for _, statement := range r.Statements {
		if strings.TrimSpace(statement) == "" {
			return fmt.Errorf("%w: empty schema statement", ErrInvalidResult)
		}
	}
	return validateOmissions(r.Omissions)
}

// SchemaService lowers and renders a captured schema in one contextual batch.
// Unsupported schemas return completed diagnostics; service errors mean that
// rendering could not be performed. Accepted schemas may record omissions.
// Implementations honor cancellation and are safe for concurrent calls. This
// boundary is independent of AST rendering: registering one grants no other.
type SchemaService interface {
	RenderSchema(context.Context, SchemaRequest) (SchemaResult, error)
}

// RenderSchema invokes an explicitly selected service and validates its reply.
// It copies the outer schema, target-fact containers, and reply slices. Nested
// schema data remains read-only. Missing services, errors, malformed replies,
// and cancellation return no result; there is no built-in fallback.
func RenderSchema(ctx context.Context, service SchemaService, request SchemaRequest) (SchemaResult, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return SchemaResult{}, err
	}
	if request.Schema == nil {
		return SchemaResult{}, fmt.Errorf("%w: cannot render a nil database schema", ptaherr.ErrInvalidSchemaDiff)
	}
	request.Schema = new(*request.Schema)
	request.Capabilities = request.Capabilities.Clone()
	request.Identifiers = request.Identifiers.Clone()
	result, err := service.RenderSchema(ctx, request)
	if err != nil {
		return SchemaResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SchemaResult{}, err
	}
	if err := result.Validate(); err != nil {
		return SchemaResult{}, err
	}
	if len(result.Diagnostics) != 0 {
		return SchemaResult{}, &SchemaRefusalError{Target: request.Target, Diagnostics: slices.Clone(result.Diagnostics)}
	}
	result.Statements = slices.Clone(result.Statements)
	result.Omissions = slices.Clone(result.Omissions)
	return result, nil
}

// SchemaRefusalError preserves completed provider diagnostics for callers that
// need to distinguish a schema refusal from failure to perform rendering. Wire
// adapters transport the diagnostic data, not this Go error value.
type SchemaRefusalError struct {
	Target      string
	Diagnostics []schemavalidation.Diagnostic
}

// Error states the provider's schema diagnostics.
func (e *SchemaRefusalError) Error() string {
	if err := e.Unwrap(); err != nil {
		return err.Error()
	}
	return "schema rendering refused"
}

// Unwrap exposes the typed schema and capability errors behind the refusal.
func (e *SchemaRefusalError) Unwrap() error {
	return (schemavalidation.Result{Complete: true, Diagnostics: e.Diagnostics}).Err(e.Target)
}
