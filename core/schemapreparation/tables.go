// Package schemapreparation defines target-owned normalization of captured
// tables before comparison. Authored state and prepared state remain separate.
package schemapreparation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

// ErrInvalid identifies an incomplete or malformed preparation exchange.
var ErrInvalid = errors.New("invalid schema preparation")

// Table carries a declared table and any captured current counterpart. Current
// knowledge describes existence only: Complete requires a current capture,
// Absent establishes a missing table, and Uninspected means its existence is
// unknown. Feature coverage inside each capture retains its independent limits.
// A missing capture without knowledge never establishes absence.
type Table struct {
	Subject          objectidentity.ID
	Desired          schemacapture.TableDeclaration
	Current          schemacapture.TableObservation
	CurrentKnowledge schemaext.Knowledge
	// ColumnPrimaryKeysPrepared makes Fields.Primary authoritative for column
	// comparison instead of deriving membership from common table constraints.
	ColumnPrimaryKeysPrepared bool
}

// Clone returns independent common state and immutable feature snapshots.
func (t Table) Clone() Table {
	t.Desired = t.Desired.Clone()
	t.Current = t.Current.Clone()
	return t
}

// Request prepares all declared tables in a single selected-target batch. The
// caller has already resolved target scope and identities. Services must not
// inspect a database, load source files, or mutate these captured inputs.
type Request struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Tables       []Table
}

// Clone returns an independent batch, including mutable target facts.
func (r Request) Clone() Request {
	r.Identifiers = r.Identifiers.Clone()
	r.Capabilities = r.Capabilities.Clone()
	r.Tables = cloneTables(r.Tables)
	return r
}

// Result contains target-normalized comparison inputs in request order. Services
// may resolve desired Fields.Primary and set ColumnPrimaryKeysPrepared. All
// other captured state, including observations and coverage, stays unchanged.
// Complete is required even for an empty batch. Errors discard the whole result.
type Result struct {
	Complete bool
	Tables   []Table
}

// Capture preserves the accepted source and prepared batches independently.
// Neither establishes execution. Diff report JSON is not a durable encoding of
// these captures; artifact codecs must retain both sides and their feature data.
type Capture struct {
	Source   []Table
	Prepared []Table
}

// Clone returns an independent copy of both captured batches.
func (c Capture) Clone() Capture {
	c.Source = cloneTables(c.Source)
	c.Prepared = cloneTables(c.Prepared)
	return c
}

func cloneTables(tables []Table) []Table {
	result := slices.Clone(tables)
	for i := range result {
		result[i] = result[i].Clone()
	}
	return result
}

// Service normalizes target representations before common and owned comparison.
// Common object lifecycles are decided by comparison, not by preparation. The
// service is safe for concurrent calls. Failure and cancellation expose no
// partial prepared state, and a successful reply explicitly sets Complete.
type Service interface {
	PrepareTables(context.Context, Request) (Result, error)
}

// Runtime selects preparation and feature comparison using the same target
// registry and codecs. It does not select a built-in implementation implicitly.
type Runtime interface {
	schemaext.ComparisonRuntime
	Service
}

// Identity explicitly selects unchanged table representations. Providers whose
// common data already has comparison semantics may register this service; an
// absent preparation service does not imply identity normalization.
type Identity struct{}

// PrepareTables returns independent unchanged captures. Nil context wraps
// ErrInvalid; cancellation returns the context error and no partial result.
func (Identity) PrepareTables(ctx context.Context, request Request) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("%w: preparation requires a context", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	result := Result{Complete: true, Tables: cloneTables(request.Tables)}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return result, nil
}

// Accept validates a complete reply and returns independent source and prepared
// captures. Table order, identities, observations, and all declaration state
// outside column-primary-key flags must match. Model codec validation remains
// the selected runtime's responsibility. An invalid reply returns ErrInvalid
// and no capture. Neither input is mutated.
func Accept(request Request, result Result) (Capture, error) {
	if !result.Complete || len(request.Tables) != len(result.Tables) {
		return Capture{}, fmt.Errorf("%w: preparation did not complete the captured batch", ErrInvalid)
	}
	for i, table := range request.Tables {
		if err := samePreparedTable(table, result.Tables[i]); err != nil {
			return Capture{}, err
		}
	}
	return (Capture{Source: request.Tables, Prepared: result.Tables}).Clone(), nil
}

func samePreparedTable(before, after Table) error {
	before, after = before.Clone(), after.Clone()
	if !retainEqualFeatures(&before, &after) {
		return fmt.Errorf("%w: preparation changed captured features", ErrInvalid)
	}
	if !reflect.DeepEqual(before.Subject, after.Subject) || before.CurrentKnowledge != after.CurrentKnowledge ||
		len(before.Desired.Fields) != len(after.Desired.Fields) || !reflect.DeepEqual(before.Current, after.Current) {
		return fmt.Errorf("%w: preparation changed identity, captured state, or knowledge", ErrInvalid)
	}
	unchanged := after.Desired.Clone()
	for i := range unchanged.Fields {
		if unchanged.Fields[i].Primary != before.Desired.Fields[i].Primary && !after.ColumnPrimaryKeysPrepared {
			return fmt.Errorf("%w: prepared column keys have no completion receipt", ErrInvalid)
		}
		unchanged.Fields[i].Primary = before.Desired.Fields[i].Primary
	}
	if !reflect.DeepEqual(before.Desired, unchanged) {
		return fmt.Errorf("%w: preparation changed state outside column key membership", ErrInvalid)
	}
	return nil
}
