package schemadiff

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

// ErrIncompleteComparison means source knowledge could not establish every
// requested state. A non-reporting comparison returns no diff in this case.
var ErrIncompleteComparison = errors.New("schema comparison is incomplete")

// Diagnostics retains common-object and feature-model knowledge limits from
// one comparison. Feature subjects use structured identities, including the
// model kind when a diagnostic concerns its whole table-owned namespace.
type Diagnostics struct {
	Common   []coverage.Object           `json:"common,omitempty"`
	Features []schemaext.UndecidedChange `json:"features,omitempty"`
}

// Empty reports whether the comparison established every requested state.
func (d Diagnostics) Empty() bool { return len(d.Common) == 0 && len(d.Features) == 0 }

// IsZero reports whether an encoded report can omit these diagnostics.
func (d Diagnostics) IsZero() bool { return d.Empty() }

// Count counts knowledge limits, including whole feature namespaces.
// It is not a count of absent objects or executable changes.
func (d Diagnostics) Count() int { return len(d.Common) + len(d.Features) }

// Clone returns diagnostics whose slices can be retained or edited independently.
func (d Diagnostics) Clone() Diagnostics {
	return Diagnostics{Common: slices.Clone(d.Common), Features: slices.Clone(d.Features)}
}

// Err returns nil for complete evidence, or an error retaining the structured
// diagnostics. Callers that render partial reports must display the limits.
func (d Diagnostics) Err() error {
	if d.Empty() {
		return nil
	}
	return &IncompleteComparisonError{Diagnostics: d.Clone()}
}

// IncompleteComparisonError exposes knowledge limits to callers through
// errors.As while errors.Is recognizes ErrIncompleteComparison.
type IncompleteComparisonError struct{ Diagnostics Diagnostics }

// Unwrap returns ErrIncompleteComparison.
func (*IncompleteComparisonError) Unwrap() error { return ErrIncompleteComparison }

// Error formats identities for display; these strings are never comparison keys.
func (e *IncompleteComparisonError) Error() string {
	descriptions := make([]string, 0, len(e.Diagnostics.Common)+len(e.Diagnostics.Features))
	for _, object := range e.Diagnostics.Common {
		descriptions = append(descriptions, fmt.Sprintf("%s %q", object.Kind, object.Name))
	}
	for _, diagnostic := range e.Diagnostics.Features {
		ref := diagnostic.Subject
		descriptions = append(descriptions, fmt.Sprintf("%s (catalog %q, schema %q, parent %q, %s %q, signature %q): %s", diagnostic.Kind,
			ref.Catalog.Source, ref.Schema.Source, ref.Parent.Source, ref.Kind, ref.Name.Source, ref.Signature, diagnostic.Reason))
	}
	return ErrIncompleteComparison.Error() + ": " + strings.Join(descriptions, "; ")
}

func completeComparison(diff *difftypes.SchemaDiff, diagnostics Diagnostics, err error) (*difftypes.SchemaDiff, error) {
	if err != nil {
		return nil, err
	}
	if err := diagnostics.Err(); err != nil {
		return nil, err
	}
	return diff, nil
}
