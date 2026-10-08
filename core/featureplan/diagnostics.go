package featureplan

import (
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// Diagnostic records a completed refusal using transportable data. Change or
// Parent identifies a zero-based request index; neither means a batch-wide
// problem. Both cannot be set. A parent diagnostic requires a parent action.
// For an indexed diagnostic, Problem.Kind names the change kind or parent model
// kind. The selected runtime validates that assignment before returning it.
type Diagnostic struct {
	Problem schemavalidation.Diagnostic
	Change  *int
	Parent  *int
}

// Clone returns an independent diagnostic, including its optional indexes.
func (d Diagnostic) Clone() Diagnostic {
	if d.Change != nil {
		d.Change = new(*d.Change)
	}
	if d.Parent != nil {
		d.Parent = new(*d.Parent)
	}
	return d
}

// ValidateOutcome checks completion, diagnostic data and input indexes, and the
// absence of output from a refused batch. The selected runtime separately checks
// successful operation codecs, ownership, change receipts, and parent receipts.
// Malformed outcomes wrap schemaext.ErrInvalidValue.
func (r Result) ValidateOutcome(request Request) error {
	if !r.Complete {
		return fmt.Errorf("%w: planning did not complete", schemaext.ErrInvalidValue)
	}
	if len(r.Diagnostics) != 0 && (len(r.Contributions) != 0 || len(r.Changes) != 0 || len(r.Parents) != 0 || len(r.Rewrites) != 0) {
		return fmt.Errorf("%w: refused planning batch contains output", schemaext.ErrInvalidValue)
	}
	for _, diagnostic := range r.Diagnostics {
		if err := (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{diagnostic.Problem}}).Validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
		if diagnostic.Change != nil && diagnostic.Parent != nil {
			return fmt.Errorf("%w: planning diagnostic names both a change and a parent", schemaext.ErrInvalidValue)
		}
		if diagnostic.Change != nil && (*diagnostic.Change < 0 || *diagnostic.Change >= len(request.Changes)) {
			return fmt.Errorf("%w: planning diagnostic change is outside the batch", schemaext.ErrInvalidValue)
		}
		if diagnostic.Parent != nil && (*diagnostic.Parent < 0 || *diagnostic.Parent >= len(request.Tables) || request.Tables[*diagnostic.Parent].Action == "") {
			return fmt.Errorf("%w: planning diagnostic parent has no requested action", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

// Err validates the outcome and converts a completed refusal into a local typed
// error. Report and transport consumers may use Diagnostics directly instead.
// A successful outcome returns nil; it still needs runtime receipt validation
// and scheduling with the host graph before any operation is used.
func (r Result) Err(request Request) error {
	if err := r.ValidateOutcome(request); err != nil {
		return err
	}
	if len(r.Diagnostics) == 0 {
		return nil
	}
	return &RefusalError{target: request.Target, diagnostics: cloneDiagnostics(r.Diagnostics)}
}

// RefusalError preserves a completed planning refusal independently of provider
// execution failures. Result.Err creates it after outcome validation. The error
// is local; a process boundary carries Diagnostic data instead.
type RefusalError struct {
	target      string
	diagnostics []Diagnostic
}

// Target returns the selected planning target.
func (e *RefusalError) Target() string { return e.target }

// Diagnostics returns independent records in the provider's report order.
func (e *RefusalError) Diagnostics() []Diagnostic { return cloneDiagnostics(e.diagnostics) }

// Error describes the completed refusal. The zero value has no diagnostics.
func (e *RefusalError) Error() string {
	if err := e.Unwrap(); err != nil {
		return err.Error()
	}
	return "feature planning refused"
}

// Unwrap retains the public schema and capability error identities.
func (e *RefusalError) Unwrap() error {
	problems := make([]schemavalidation.Diagnostic, 0, len(e.diagnostics))
	for _, diagnostic := range e.diagnostics {
		problems = append(problems, diagnostic.Problem)
	}
	return (schemavalidation.Result{Complete: true, Diagnostics: problems}).Err(e.target)
}

func cloneDiagnostics(diagnostics []Diagnostic) []Diagnostic {
	result := slices.Clone(diagnostics)
	for i := range result {
		result[i] = result[i].Clone()
	}
	return result
}
