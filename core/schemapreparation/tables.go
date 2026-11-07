// Package schemapreparation defines target-owned normalization before
// comparison: of captured tables, and of a whole desired schema into the shape
// the target's reader reports it in. Authored state and prepared state remain
// separate.
package schemapreparation

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
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
	// ResolvedFacets contains prepared values for facets already declared on
	// this table or its indexes, keyed by the common object's identity.
	// It is output-only; the source declaration stays in Desired.
	// Applying these values retains the declaration's original target scopes.
	ResolvedFacets []schemaext.FacetRecord
	// ColumnPrimaryKeysPrepared makes Fields.Primary authoritative for column
	// comparison instead of deriving membership from common table constraints.
	ColumnPrimaryKeysPrepared bool
	// IndexMethods holds the kind the target reads each declared index as,
	// keyed by the index's declared name and spelled as the target's reader
	// spells [ptah.run/catalog.Index.Method]. It is output-only, and a target
	// that states no kind leaves it nil. The index comparison hands the
	// target's index rules a declared index with its stated kind in Type
	// ([schemacomparison.Indexes.OptionsEqual]); the source declaration in
	// Desired keeps the type it was written with.
	IndexMethods map[string]string
	// IndexNulls holds the NULLS order the target gives each key part of each
	// declared index, keyed by the index's declared name: one entry for each
	// of the index's parts, or for each of its fields when it lists no parts,
	// in key order. An entry is [schemamodel.NullsOrderFirst] or
	// [schemamodel.NullsOrderLast] where the order differs from the target's
	// default for the part's direction, and empty where it is the default, the
	// form the target's reader reports a held index's parts in. It is
	// output-only, and a target that states no order leaves it nil. The index
	// comparison hands the target's index rules a declared index with these
	// orders in its parts ([schemacomparison.Indexes.OptionsEqual]); the
	// source declaration in Desired keeps the orders it was written with.
	IndexNulls map[string][]string
	// IndexConditions holds the predicate of each declared index as the
	// target holds it, keyed by the index's declared name, in the spelling the
	// target's reader reports [ptah.run/catalog.Index.Condition] in, without
	// the WHERE keyword. An entry is empty exactly when the index declares no
	// predicate, and has no surrounding white space. It is output-only, and a
	// target that states no predicate leaves it nil. The index comparison
	// compares a stated predicate exactly with the held one, in place of the
	// shared comparison of predicates; the source declaration in Desired keeps
	// the predicate it was written with.
	IndexConditions map[string]string
}

// Clone returns independent common state and immutable feature snapshots.
func (t Table) Clone() Table {
	t.Desired = t.Desired.Clone()
	t.Current = t.Current.Clone()
	t.ResolvedFacets = slices.Clone(t.ResolvedFacets)
	t.IndexMethods = maps.Clone(t.IndexMethods)
	t.IndexConditions = maps.Clone(t.IndexConditions)
	if t.IndexNulls != nil {
		nulls := make(map[string][]string, len(t.IndexNulls))
		for name, orders := range t.IndexNulls {
			nulls[name] = slices.Clone(orders)
		}
		t.IndexNulls = nulls
	}
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
	r.Tables = cloneTables(r.Tables)
	return r
}

// Result contains target-normalized comparison inputs in request order. Services
// may resolve desired Fields.Primary and set ColumnPrimaryKeysPrepared. All
// other captured state, including observations and coverage, stays unchanged.
// ResolvedFacets may carry resolved values for existing declared facet kinds,
// IndexMethods the kinds of declared indexes, IndexNulls the NULLS orders of
// their key parts, and IndexConditions their predicates.
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

// Runtime selects preparation, desired lowering, comparison rules and feature
// comparison using the same target registry and codecs. Source properties are
// decoded before preparation captures declarations. It does not select a
// built-in implementation implicitly.
type Runtime interface {
	schemaext.ComparisonRuntime
	schemaproperties.Runtime
	schemacomparison.Runtime
	Service
	Lowering
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
		if err := samePreparedTable(table, result.Tables[i], request.Identifiers); err != nil {
			return Capture{}, err
		}
	}
	return (Capture{Source: request.Tables, Prepared: result.Tables}).Clone(), nil
}

func samePreparedTable(before, after Table, semantics identifier.Semantics) error {
	if err := acceptResolvedFacets(before, after.ResolvedFacets, semantics); err != nil {
		return err
	}
	if err := acceptIndexMethods(before, after.IndexMethods); err != nil {
		return err
	}
	if err := acceptIndexNulls(before, after.IndexNulls); err != nil {
		return err
	}
	if err := acceptIndexConditions(before, after.IndexConditions); err != nil {
		return err
	}
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

// acceptIndexNulls refuses NULLS orders for an index the table does not
// declare, orders that do not number the index's key parts, an order other
// than FIRST, LAST or empty, and an input that already states orders.
func acceptIndexNulls(source Table, nulls map[string][]string) error {
	if len(source.IndexNulls) != 0 {
		return fmt.Errorf("%w: preparation input already states index NULLS orders", ErrInvalid)
	}
	for name, orders := range nulls {
		position := slices.IndexFunc(source.Desired.Indexes, func(index schemamodel.Index) bool { return index.Name == name })
		if position < 0 {
			return fmt.Errorf("%w: preparation states the NULLS orders of index %q, which the table does not declare", ErrInvalid, name)
		}
		index := source.Desired.Indexes[position]
		parts := len(index.Parts)
		if parts == 0 {
			parts = len(index.Fields)
		}
		if len(orders) != parts {
			return fmt.Errorf("%w: preparation states %d NULLS orders for the %d key parts of index %q", ErrInvalid, len(orders), parts, name)
		}
		for _, order := range orders {
			if order != "" && order != schemamodel.NullsOrderFirst && order != schemamodel.NullsOrderLast {
				return fmt.Errorf("%w: preparation states NULLS order %q for index %q", ErrInvalid, order, name)
			}
		}
	}
	return nil
}

// acceptIndexConditions refuses a predicate for an index the table does not
// declare, a predicate stated for an index that declares none or none for one
// that declares one, a predicate with surrounding white space, and an input
// that already states predicates.
func acceptIndexConditions(source Table, conditions map[string]string) error {
	if len(source.IndexConditions) != 0 {
		return fmt.Errorf("%w: preparation input already states index predicates", ErrInvalid)
	}
	for name, condition := range conditions {
		position := slices.IndexFunc(source.Desired.Indexes, func(index schemamodel.Index) bool { return index.Name == name })
		if position < 0 {
			return fmt.Errorf("%w: preparation states the predicate of index %q, which the table does not declare", ErrInvalid, name)
		}
		if (strings.TrimSpace(source.Desired.Indexes[position].Condition) == "") != (condition == "") {
			return fmt.Errorf("%w: preparation states a predicate for index %q exactly when the index declares one", ErrInvalid, name)
		}
		if strings.TrimSpace(condition) != condition {
			return fmt.Errorf("%w: preparation states the predicate of index %q with surrounding white space", ErrInvalid, name)
		}
	}
	return nil
}

// acceptIndexMethods refuses a stated kind for an index the table does not
// declare, and an input that already states kinds.
func acceptIndexMethods(source Table, methods map[string]string) error {
	if len(source.IndexMethods) != 0 {
		return fmt.Errorf("%w: preparation input already states index kinds", ErrInvalid)
	}
	for name := range methods {
		if !slices.ContainsFunc(source.Desired.Indexes, func(index schemamodel.Index) bool { return index.Name == name }) {
			return fmt.Errorf("%w: preparation states the kind of index %q, which the table does not declare", ErrInvalid, name)
		}
	}
	return nil
}
