package schemaext

import (
	"context"
	"errors"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// ErrIrreversible reports that the selected owner cannot reconstruct the prior
// schema definition. No reverse changes may be published after this error.
// Recoverable definitions with unrecoverable data instead carry Limitations.
var ErrIrreversible = errors.New("feature change cannot be reversed")

// ReversalRequest asks owners to reconstruct the inverse of accepted changes.
// Every change must capture the state needed by its owner; no service may read
// a live database or recover missing operands from a mutable desired schema.
// Capabilities and identifier semantics describe the same target as the forward
// plan. They do not assert that the forward plan has already executed.
type ReversalRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Changes      []ChangeRecord
}

// Reversal is one self-contained reverse change and its recovery limits.
// Change preserves the input's subject and change kind. Its operands describe
// the reverse direction, including any state predicted after forward execution.
// Such predictions never constitute inspection evidence.
type Reversal struct {
	Change ChangeRecord
	// ForwardState describes the modeled state left by the accepted forward
	// change at this subject. Each entry replaces one complete typed value or
	// establishes its absence. Unlisted values and siblings remain unchanged.
	// These predictions are planning inputs, never inspection evidence.
	ForwardState []ProjectedValue
	// Strategy is an owner-defined description of how the reverse restores the
	// definition, such as changing settings in place or recreating an object.
	Strategy string
	// Limitations describes state this reversal cannot recover, such as deleted
	// records or consumed messages. An empty list makes no claim about changes
	// made outside this plan or ordinary runtime activity between directions.
	// Callers must preserve and report every limitation when publishing a plan.
	Limitations []string
}

// Placement distinguishes an independently named object from a facet attached
// to the change's subject. Their identity spaces must never be interchanged.
type Placement string

const (
	// ObjectPlacement projects the independently named subject itself.
	ObjectPlacement Placement = "object"
	// FacetPlacement projects one feature value attached to the subject.
	FacetPlacement Placement = "facet"
)

// ProjectedValue captures one complete model value after an accepted change.
// Kind names the model, independently of the change kind. A nil Value explicitly
// means absence; a typed nil is invalid. A present value uses the Observed
// representation so default handling remains with its owner. The owner may
// leave genuinely unobservable attributes unknown in that representation.
// A projection does not claim that the forward plan has executed.
type ProjectedValue struct {
	Placement Placement
	Kind      Kind
	Value     Value
}

// ReversalService reconstructs an ordered batch without mutating its inputs.
// It returns exactly one result per input. A complex inverse stays in its
// owner's typed change payload. Unsupported reversal returns an error rather
// than an empty successful change; failure or cancellation discards all results.
type ReversalService interface {
	ReverseChanges(context.Context, ReversalRequest) ([]Reversal, error)
}

// ReversalRuntime provides selected reversal services and local change codecs.
// Other stages compose their own narrow contracts with this one as needed.
type ReversalRuntime interface {
	ModelRuntime
	ReversalService
}
