// Package schemaprojection defines target-owned predictions of accepted schema
// changes. Predictions are planning operands, never evidence of execution.
package schemaprojection

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// ErrInvalid identifies an incomplete or conflicting projection contract.
var ErrInvalid = errors.New("invalid schema projection")

// TableState captures common table state relevant to constraint side effects.
// Other owned children remain with their feature projectors. A service unable
// to account for an effect outside this capture must report Unavailable.
type TableState struct {
	Table       catalog.Table
	Indexes     []catalog.Index
	Constraints []catalog.Constraint
}

// Clone returns a capture with independent mutable definitions.
func (s TableState) Clone() TableState {
	s.Table = s.Table.Clone()
	s.Indexes = slices.Clone(s.Indexes)
	for i := range s.Indexes {
		s.Indexes[i] = s.Indexes[i].Clone()
	}
	s.Constraints = slices.Clone(s.Constraints)
	for i := range s.Constraints {
		s.Constraints[i] = s.Constraints[i].Clone()
	}
	return s
}

// ConstraintChange captures a resolved lifecycle transition. Nil means known
// absence, never uninspected state. Replacements carry both complete operands.
// Both operands name the same table-qualified constraint.
type ConstraintChange struct {
	Before *catalog.Constraint
	After  *catalog.Constraint
}

// Clone returns a change with independent operands.
func (c ConstraintChange) Clone() ConstraintChange {
	if c.Before != nil {
		c.Before = new(c.Before.Clone())
	}
	if c.After != nil {
		c.After = new(c.After.Clone())
	}
	return c
}

// ConstraintRequest asks the target to complete intrinsic constraint changes
// with its backing-index and column effects. After already includes accepted
// common column, index, and constraint changes, including policy filtering.
// Before is the captured input. Services must not consult a desired document,
// read a database, mutate inputs, or infer execution from either capture.
type ConstraintRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Before       TableState
	After        TableState
	Changes      []ConstraintChange
}

// Clone returns an independent request, including target facts.
func (r ConstraintRequest) Clone() ConstraintRequest {
	r.Identifiers = r.Identifiers.Clone()
	r.Capabilities = r.Capabilities.Clone()
	r.Before = r.Before.Clone()
	r.After = r.After.Clone()
	r.Changes = slices.Clone(r.Changes)
	for i := range r.Changes {
		r.Changes[i] = r.Changes[i].Clone()
	}
	return r
}

// ConstraintResult supplies exactly one outcome: complete State, or a nonempty
// Unavailable reason with no State. Unavailable does not mean unchanged state.
// The caller must refuse any operation that requires the missing prediction.
// Service errors and cancellation discard both outcomes.
type ConstraintResult struct {
	State       *TableState
	Unavailable string
}

// Validate checks that exactly one outcome was supplied. The selected runtime
// also validates state identities against the request before publishing it.
func (r ConstraintResult) Validate() error {
	if r.State == nil && strings.TrimSpace(r.Unavailable) == "" {
		return fmt.Errorf("%w: missing constraint projection outcome", ErrInvalid)
	}
	if r.State != nil && r.Unavailable != "" {
		return fmt.Errorf("%w: unavailable projection carries state", ErrInvalid)
	}
	return nil
}

// ConstraintService predicts the target's complete common constraint effects.
// It is safe for concurrent use and performs no database I/O. Execution-order
// dependent or uncaptured effects require an Unavailable result, never a guess.
type ConstraintService interface {
	ProjectConstraints(context.Context, ConstraintRequest) (ConstraintResult, error)
}
