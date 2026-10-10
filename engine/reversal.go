package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Reversal assigns change kinds on one target to their owning reverse service.
// The provider must own each change codec. Registration grants no ability to
// recover data or to recreate an unsupported prior definition.
type Reversal struct {
	Target  string
	Kinds   []schemaext.Kind
	Service schemaext.ReversalService
}

func (r *Runtime) registerReversal(owner string, declaration Reversal) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete reversal registration for %q", ErrInvalidRegistration, declaration.Target)
	}
	for _, kind := range declaration.Kinds {
		if !r.ownsCodec(owner, kind, schemaext.Change) {
			return fmt.Errorf("%w: %q does not own change %q", ErrInvalidRegistration, owner, kind)
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, exists := r.reversals[key]; exists {
			return fmt.Errorf("%w: duplicate reversal for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.reversals[key] = len(r.reversalServices)
	}
	r.reversalServices = append(r.reversalServices, declaration.Service)
	return nil
}

// ReverseChanges dispatches one batch per selected owner after validating the
// entire request. Results retain input order and source identities. All replies
// are cloned and validated before publication; errors return no partial result.
func (r *Runtime) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return nil, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	changes, err := r.codecs.SnapshotChanges(ctx, request.Changes)
	if err != nil {
		return nil, err
	}
	batches, err := r.reversalBatches(target.name, changes)
	if err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, len(changes))
	for service, indices := range batches {
		if len(indices) == 0 {
			continue
		}
		selected := make([]schemaext.ChangeRecord, len(indices))
		for i, index := range indices {
			selected[i] = changes[index]
		}
		// Keep the validation ledger separate from the service's mutable inputs.
		sent, err := r.codecs.SnapshotChanges(ctx, selected)
		if err != nil {
			return nil, err
		}
		replies, err := r.reversalServices[service].ReverseChanges(ctx, schemaext.ReversalRequest{
			Target: target.name, Identifiers: request.Identifiers.Clone(), Capabilities: request.Capabilities.Clone(), Changes: sent,
		})
		if err != nil {
			return nil, err
		}
		replies, err = r.validateReversals(ctx, selected, replies)
		if err != nil {
			return nil, err
		}
		for i, index := range indices {
			result[index] = replies[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := uniqueReversalState(result); err != nil {
		return nil, err
	}
	return result, nil
}

func uniqueReversalState(results []schemaext.Reversal) error {
	type key struct {
		subject   objectidentity.Key
		placement schemaext.Placement
		kind      schemaext.Kind
	}
	seen := make(map[key]bool)
	for _, result := range results {
		for _, projected := range result.ForwardState {
			identity := key{result.Change.Subject.Key(), projected.Placement, projected.Kind}
			if seen[identity] {
				return fmt.Errorf("%w: competing state projections for %s/%q", schemaext.ErrInvalidValue, result.Change.Subject, projected.Kind)
			}
			seen[identity] = true
		}
	}
	return nil
}

func (r *Runtime) reversalBatches(target string, changes []schemaext.ChangeRecord) ([][]int, error) {
	batches := make([][]int, len(r.reversalServices))
	type changeKey struct {
		subject objectidentity.Key
		kind    schemaext.Kind
	}
	seen := make(map[changeKey]bool)
	for index, change := range changes {
		key := changeKey{subject: change.Subject.Key(), kind: change.Value.Kind()}
		if seen[key] {
			return nil, fmt.Errorf("%w: duplicate reverse input for %s/%q", schemaext.ErrInvalidValue, change.Subject, key.kind)
		}
		seen[key] = true
		service, found := r.reversals[conversionKey{target: target, kind: key.kind}]
		if !found {
			return nil, fmt.Errorf("%w: no reversal for %q/%q", ptaherr.ErrUnsupportedFeature, target, key.kind)
		}
		batches[service] = append(batches[service], index)
	}
	return batches, nil
}

func (r *Runtime) validateReversals(ctx context.Context, inputs []schemaext.ChangeRecord, replies []schemaext.Reversal) ([]schemaext.Reversal, error) {
	if len(replies) != len(inputs) {
		return nil, fmt.Errorf("%w: reversal changed the result count", schemaext.ErrInvalidValue)
	}
	var changes []schemaext.ChangeRecord
	for i, reply := range replies {
		if reply.Change.Subject != inputs[i].Subject || !reversalText(reply.Strategy) {
			return nil, fmt.Errorf("%w: reversal changed its subject or omitted its strategy", schemaext.ErrInvalidValue)
		}
		for _, limitation := range reply.Limitations {
			if !reversalText(limitation) {
				return nil, fmt.Errorf("%w: invalid reversal limitation", schemaext.ErrInvalidValue)
			}
		}
		if reply.Change.Value == nil {
			// A reverse with no statement must say what it leaves behind.
			if len(reply.Limitations) == 0 {
				return nil, fmt.Errorf("%w: a reversal without a change must state its limitations", schemaext.ErrInvalidValue)
			}
			continue
		}
		changes = append(changes, reply.Change)
	}
	captured, err := r.codecs.SnapshotChanges(ctx, changes)
	if err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, len(replies))
	for i, reply := range replies {
		change := schemaext.ChangeRecord{Subject: reply.Change.Subject}
		if reply.Change.Value != nil {
			change, captured = captured[0], captured[1:]
			if change.Value.Kind() != inputs[i].Value.Kind() {
				return nil, fmt.Errorf("%w: reversal changed the ordered change kind", schemaext.ErrInvalidValue)
			}
		}
		state, err := r.snapshotReversalState(ctx, inputs[i], change.Subject, reply.ForwardState)
		if err != nil {
			return nil, err
		}
		result[i] = schemaext.Reversal{Change: change, ForwardState: state, Strategy: reply.Strategy, Limitations: slices.Clone(reply.Limitations)}
	}
	return result, nil
}

// snapshotReversalState validates the projections a reversal of input makes
// at subject. The input's change kind names the owner, which the reply keeps
// or, for a reverse without a statement, leaves out.
func (r *Runtime) snapshotReversalState(ctx context.Context, input schemaext.ChangeRecord, subject objectidentity.ID, projections []schemaext.ProjectedValue) ([]schemaext.ProjectedValue, error) {
	owner := ""
	for _, definition := range r.codecs.Definitions() {
		if definition.Kind == input.Value.Kind() && definition.Representation == schemaext.Change {
			owner = definition.Owner
			break
		}
	}
	result := make([]schemaext.ProjectedValue, len(projections))
	type projectionKey struct {
		placement schemaext.Placement
		kind      schemaext.Kind
	}
	seen := make(map[projectionKey]bool)
	for i, projection := range projections {
		if projection.Placement != schemaext.ObjectPlacement && projection.Placement != schemaext.FacetPlacement {
			return nil, fmt.Errorf("%w: unknown reversal state placement", schemaext.ErrInvalidValue)
		}
		if projection.Placement == schemaext.ObjectPlacement && objectidentity.Kind(projection.Kind) != subject.Kind {
			return nil, fmt.Errorf("%w: projected object kind disagrees with its subject", schemaext.ErrInvalidValue)
		}
		key := projectionKey{projection.Placement, projection.Kind}
		if seen[key] || !r.ownsCodec(owner, projection.Kind, schemaext.Observed) {
			return nil, fmt.Errorf("%w: duplicate or unowned reversal state for %q", schemaext.ErrInvalidValue, projection.Kind)
		}
		seen[key] = true
		result[i] = projection
		if projection.Value == nil {
			continue
		}
		values, err := r.codecs.SnapshotValues(ctx, schemaext.Observed, []schemaext.Value{projection.Value})
		if err != nil {
			return nil, err
		}
		if values[0].Kind() != projection.Kind {
			return nil, fmt.Errorf("%w: projected value disagrees with its model kind", schemaext.ErrInvalidValue)
		}
		result[i].Value = values[0]
	}
	return result, nil
}

func reversalText(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && !strings.ContainsFunc(value, func(r rune) bool {
		return unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
	})
}
