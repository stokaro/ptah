package dbschema

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
)

// UnitExecutor executes complete ordered batches on one selected session or
// transaction. Native fragments retain their captured owner data. Callers own
// approval, target verification, and transaction policy; this interface does not
// infer those decisions from a payload. Implementations never retry mutations.
type UnitExecutor interface {
	catalog.SchemaExecutor
	// ValidateUnits checks the complete batch without writing or starting a
	// transaction. It must honor cancellation and must not mutate its inputs.
	ValidateUnits(context.Context, []renderer.Fragment) error
	// ExecuteUnits repeats validation before dispatch. A failed preflight sends
	// no prefix. A dispatched failure preserves its confirmed prefix and any
	// ErrOutcomeUnknown cause. A dry-run executor validates but writes nothing.
	ExecuteUnits(context.Context, []renderer.Fragment) error
}

// UnitInputPreparer discovers value-free references before the caller asks for
// approval. Returned fragments preserve every body and its order. Discovery
// performs no writes, reads no input values, and returns independent snapshots.
// Callers retain the references with the exact units they approve.
type UnitInputPreparer interface {
	PrepareUnitInputs(context.Context, []renderer.Fragment) ([]renderer.Fragment, error)
}

// UnitExecutionError retains the acknowledged prefix of a dispatched batch.
// Applied confirms execution within the selected transaction, not its commit.
// Unconfirmed units may have taken effect; this error never authorizes retry.
// Err retains outcome uncertainty through core/ptaherr.ErrOutcomeUnknown.
type UnitExecutionError struct {
	Err     error
	Applied int
	Total   int
}

// Error reports progress without including SQL, native payloads, or addresses.
func (e *UnitExecutionError) Error() string {
	return fmt.Sprintf("provider execution failed (%d of %d units confirmed): %v", e.Applied, e.Total, e.Err)
}

// Unwrap preserves cancellation, provider refusal, and uncertain-outcome causes.
func (e *UnitExecutionError) Unwrap() error { return e.Err }
