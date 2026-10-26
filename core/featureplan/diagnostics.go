package featureplan

import (
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// Diagnostic records a completed refusal using transportable data. Change,
// Parent, or ContextObject identifies a zero-based request index; no index
// means a batch-wide problem. At most one index can be set. A parent diagnostic
// requires a parent action.
// For an indexed diagnostic, Problem.Kind names the change kind or parent model
// kind. The selected runtime validates that assignment before returning it.
type Diagnostic struct {
	Problem schemavalidation.Diagnostic
	Change  *int
	Parent  *int
	// ContextObject identifies an entry in Objects.AllSubjects().
	ContextObject *int
}

// Clone returns an independent diagnostic, including its optional indexes.
func (d Diagnostic) Clone() Diagnostic {
	if d.Change != nil {
		d.Change = new(*d.Change)
	}
	if d.Parent != nil {
		d.Parent = new(*d.Parent)
	}
	if d.ContextObject != nil {
		d.ContextObject = new(*d.ContextObject)
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
	if len(r.Diagnostics) != 0 && (len(r.Contributions) != 0 || len(r.Changes) != 0 || len(r.Parents) != 0 || len(r.Rewrites) != 0 || len(r.Objects) != 0) {
		return fmt.Errorf("%w: refused planning batch contains output", schemaext.ErrInvalidValue)
	}
	for _, diagnostic := range r.Diagnostics {
		if err := (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{diagnostic.Problem}}).Validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
		if err := diagnostic.validateInput(request); err != nil {
			return err
		}
	}
	return nil
}

func (d Diagnostic) validateInput(request Request) error {
	count := 0
	for _, index := range []*int{d.Change, d.Parent, d.ContextObject} {
		if index != nil {
			count++
		}
	}
	if count > 1 {
		return fmt.Errorf("%w: planning diagnostic names more than one input", schemaext.ErrInvalidValue)
	}
	if d.ContextObject != nil && (*d.ContextObject < 0 || *d.ContextObject >= len(request.Objects.AllSubjects())) {
		return fmt.Errorf("%w: planning context object is outside the batch", schemaext.ErrInvalidValue)
	}
	if d.Change != nil && (*d.Change < 0 || *d.Change >= len(request.Changes)) {
		return fmt.Errorf("%w: planning diagnostic change is outside the batch", schemaext.ErrInvalidValue)
	}
	if d.Parent != nil && (*d.Parent < 0 || *d.Parent >= len(request.Tables) || request.Tables[*d.Parent].Action == "") {
		return fmt.Errorf("%w: planning diagnostic parent has no requested action", schemaext.ErrInvalidValue)
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
