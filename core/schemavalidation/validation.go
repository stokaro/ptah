// Package schemavalidation defines target-owned validation of captured schema
// declarations. Validation is offline and does not render an executable plan.
package schemavalidation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// ErrInvalidResult marks an incomplete or malformed validation reply.
var ErrInvalidResult = errors.New("invalid schema validation result")

// Code classifies a schema diagnostic independently of its human-readable text.
// Provider failures are returned as errors, never encoded as a diagnostic.
type Code string

const (
	// InvalidSchema identifies a declaration that is not valid for its target.
	InvalidSchema Code = "invalid-schema"
	// UnsupportedFeature identifies semantics the target cannot implement.
	UnsupportedFeature Code = "unsupported-feature"
	// OmittedDeclaration identifies requested semantics the target would omit.
	OmittedDeclaration Code = "omitted-declaration"
)

// Diagnostic describes a declared-schema problem. It contains only data so a
// transport adapter can preserve it without serializing a Go error or AST node.
type Diagnostic struct {
	Code    Code
	Kind    string
	Object  string
	Feature string
	Message string
}

// Request validates one complete captured declaration against explicit target
// facts. Schema and its nested values are read-only. No service may mutate the
// declaration, inspect a server, or load missing facts from a source document.
type Request struct {
	Target       string
	Capabilities capability.Capabilities
	Identifiers  identifier.Semantics
	Schema       *schemamodel.Database
	// NoSkipped also diagnoses declarations the target would omit. It does
	// not restore declarations excluded by the document's target scope.
	NoSkipped bool
}

// Result separates a completed validation from failure to perform validation.
// Complete must be true even when there are no diagnostics. Diagnostics are
// ordered deterministically; an empty list means the requested checks passed.
// No successful validation claims that the schema was applied or inspected.
type Result struct {
	Complete    bool
	Diagnostics []Diagnostic
}

// Validate refuses an absent completion receipt or malformed diagnostic.
func (r Result) Validate() error {
	if !r.Complete {
		return fmt.Errorf("%w: validation did not complete", ErrInvalidResult)
	}
	for _, diagnostic := range r.Diagnostics {
		switch diagnostic.Code {
		case InvalidSchema, UnsupportedFeature, OmittedDeclaration:
		default:
			return fmt.Errorf("%w: unknown diagnostic code %q", ErrInvalidResult, diagnostic.Code)
		}
		if strings.TrimSpace(diagnostic.Kind) == "" || strings.TrimSpace(diagnostic.Message) == "" {
			return fmt.Errorf("%w: diagnostic requires kind and message", ErrInvalidResult)
		}
	}
	return nil
}

// Err converts validated diagnostics into the public schema and capability
// errors consumed by planning callers. A nonempty completed result returns a
// RefusalError; an incomplete or malformed result never does.
// Report consumers can present Diagnostics directly without making this conversion.
func (r Result) Err(target string) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if len(r.Diagnostics) == 0 {
		return nil
	}
	return &RefusalError{target: target, diagnostics: slices.Clone(r.Diagnostics)}
}

// RefusalError records completed schema diagnostics, distinct from failure to
// perform validation. Result.Err creates it only after validating the receipt.
// Wire adapters carry the diagnostic data, not this Go error value.
type RefusalError struct {
	target      string
	diagnostics []Diagnostic
}

// Target returns the target whose schema validation completed.
func (e *RefusalError) Target() string { return e.target }

// Diagnostics returns an independent copy in the provider's report order.
func (e *RefusalError) Diagnostics() []Diagnostic { return slices.Clone(e.diagnostics) }

// Error reports the completed schema diagnostics. The zero value has no
// diagnostics and reports only that validation refused the schema.
func (e *RefusalError) Error() string {
	if err := e.Unwrap(); err != nil {
		return err.Error()
	}
	return "schema validation refused"
}

// Unwrap retains the schema and capability error identities for errors.Is and
// errors.As. The zero value unwraps to nil.
func (e *RefusalError) Unwrap() error {
	failures := make([]error, 0, len(e.diagnostics))
	for _, diagnostic := range e.diagnostics {
		if diagnostic.Code == UnsupportedFeature {
			failures = append(failures, &ptaherr.CapabilityError{
				Dialect: e.target, Feature: diagnostic.Feature, Err: ptaherr.ErrUnsupportedFeature, Message: diagnostic.Message,
			})
			continue
		}
		failures = append(failures, &ptaherr.RenderError{
			Dialect: e.target, Err: ptaherr.ErrInvalidSchemaDiff, Message: diagnostic.Message,
		})
	}
	return errors.Join(failures...)
}

// Service validates a whole schema in one contextual batch. Ordinary schema
// refusals are diagnostics in a complete result. Errors mean that validation
// could not be performed, including provider failure and cancellation, and
// must discard all diagnostics. Implementations are safe for concurrent calls.
type Service interface {
	ValidateSchema(context.Context, Request) (Result, error)
}

// Runtime adds explicit target selection for callers that scope a declaration
// before common structural checks and owner validation.
type Runtime interface {
	Service
	schemaext.TargetResolver
}

// Validate invokes the selected service once and validates its completion
// receipt. It copies target-fact maps, the outer schema value, and the reply's
// diagnostic slice. Nested schema data remains read-only under the service
// contract. Errors and cancellation expose no partial diagnostics. A missing
// service is refused without selecting a built-in implementation.
func Validate(ctx context.Context, service Service, request Request) (Result, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return Result{}, err
	}
	if request.Schema == nil {
		return Result{}, &ptaherr.RenderError{Dialect: request.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: "cannot validate a nil database schema"}
	}
	request.Capabilities = request.Capabilities.Clone()
	request.Identifiers = request.Identifiers.Clone()
	request.Schema = new(*request.Schema)
	result, err := service.ValidateSchema(ctx, request)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	result.Diagnostics = slices.Clone(result.Diagnostics)
	return result, nil
}
