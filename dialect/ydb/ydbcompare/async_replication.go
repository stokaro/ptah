package ydbcompare

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
)

// AsyncReplicationService compares YDB async replications. A replication both
// sides hold is compared through [ydbreplication.ReplicationsEqual]: each side
// resolved as YDB keeps it, so a declaration naming a default and one leaving
// it out are the same replication, and the items compare by the tables they
// replicate rather than by how a directory item was written.
//
// A replication is a creation only where the read established its absence,
// since CREATE ASYNC REPLICATION carries no guard, and a removal only where the
// desired source claims to describe the replication. An incomplete observation
// never establishes absence or a destructive change. Whether YDB can make a
// change in place, which depends on the replication's state, is the planner's
// to decide.
type AsyncReplicationService struct{}

// TransferService compares YDB transfers, as [AsyncReplicationService]
// compares replications, through [ydbreplication.TransfersEqual].
type TransferService struct{}

// replicationKind is what the comparison of one kind needs from its owner:
// its models, how YDB keeps it, and the key a statement on it needs.
type replicationKind[D, O interface {
	schemaext.Value
	comparable
}, C schemaext.ChangeValue] struct {
	kind              schemaext.Kind
	family, namespace string
	codecs            []schemaext.Codec
	coverage          func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error)
	equal             func(D, O) bool
	kept              func(O) D
	change            func(before O, after D) C
	key               capability.Capability
}

// replicationPair is one object of a kind, as each side holds it.
type replicationPair[D, O any] struct {
	ref     objectidentity.ID
	desired D
	current O
}

var replicationRules = replicationKind[*ydbreplication.DesiredReplication, *ydbreplication.ObservedReplication, *ydbdiff.AsyncReplication]{
	kind: ydbreplication.ReplicationKind, family: "async replication", namespace: "async replications",
	codecs: ydbreplication.ReplicationCodecs(), coverage: ydbreplication.ReplicationCoverage,
	equal: (*ydbreplication.DesiredReplication).Matches, kept: (*ydbreplication.ObservedReplication).Desired,
	change: ydbdiff.NewAsyncReplication, key: capability.AsyncReplication,
}

var transferRules = replicationKind[*ydbreplication.DesiredTransfer, *ydbreplication.ObservedTransfer, *ydbdiff.Transfer]{
	kind: ydbreplication.TransferKind, family: "transfer", namespace: "transfers",
	codecs: ydbreplication.TransferCodecs(), coverage: ydbreplication.TransferCoverage,
	equal: (*ydbreplication.DesiredTransfer).Matches, kept: (*ydbreplication.ObservedTransfer).Desired,
	change: ydbdiff.NewTransfer, key: capability.Transfers,
}

// CompareObjects returns one change per replication the two sides hold
// differently. A target without the async_replication capability is refused
// only when the comparison plans a statement.
func (AsyncReplicationService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	return replicationRules.compare(ctx, request)
}

// CompareObjects returns one change per transfer the two sides hold
// differently. A target without the transfers capability is refused only when
// the comparison plans a statement.
func (TransferService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	return transferRules.compare(ctx, request)
}

// compare is the comparison of one kind on the shared standalone skeleton.
func (r replicationKind[D, O, C]) compare(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	pairs, err := captureStandaloneInputs(ctx, request, r.kind, r.family, r.collect)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	var noDesired D
	var noCurrent O
	presence := make([]standalonePresence, 0, len(pairs))
	for _, pair := range pairs {
		presence = append(presence, standalonePresence{ref: pair.ref, desired: pair.desired != noDesired, current: pair.current != noCurrent})
	}
	coverage, err := standaloneCoverage(request, presence, r.kind, r.namespace, r.coverage)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result, err := completeStandaloneComparison(ctx, request, pairs, coverage, r.kind, r.family,
		func(pair replicationPair[D, O]) objectidentity.ID { return pair.ref },
		func(request schemaext.ObjectComparisonRequest, pair replicationPair[D, O], result *schemaext.ObjectComparisonResult) error {
			return decideStandalone(request, result, standaloneDecision{kind: r.kind, family: r.family, ref: pair.ref,
				desired: pair.desired != noDesired, current: pair.current != noCurrent,
				equal:  func() bool { return r.equal(pair.desired, pair.current) },
				kept:   func() schemaext.Value { return r.kept(pair.current) },
				change: func() schemaext.ChangeValue { return r.change(pair.current, pair.desired) },
			})
		})
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	// The changes come out in path order, so the refusal names the first
	// object a statement would touch. The planner holds each statement to the
	// rest of the target's rules.
	if len(result.Changes) > 0 && !request.Capabilities.Has(r.key) {
		ref := result.Changes[0].Subject
		return schemaext.ObjectComparisonResult{}, (&ydbreplication.Refusal{
			Subject: r.family + " " + ydbreplication.Reference(ref.Schema.Source, ref.Name.Source), Key: r.key}).Err(request.Target)
	}
	return result, nil
}

// collect captures the kind's objects of one side into pairs.
func (r replicationKind[D, O, C]) collect(ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation,
	pairs map[objectidentity.Key]replicationPair[D, O],
) error {
	var noDesired D
	var noCurrent O
	return collectStandalone(ctx, state, direction, r.kind, r.family, r.codecs, ydbreplication.ValidateIdentity,
		func(ref objectidentity.ID, desired D, current O) {
			pair := pairs[ref.Key()]
			pair.ref = ref
			if desired != noDesired {
				pair.desired = desired
			}
			if current != noCurrent {
				pair.current = current
			}
			pairs[ref.Key()] = pair
		})
}

// standaloneDecision is one object of a standalone kind both sides were
// captured for, with the owner's equality and the values it keeps or changes.
type standaloneDecision struct {
	kind             schemaext.Kind
	family           string
	ref              objectidentity.ID
	desired, current bool
	// equal reports whether both sides describe one object; it is asked only
	// when both are present.
	equal func() bool
	// kept is the observation as a declaration, which an unclaimed object
	// keeps.
	kept func() schemaext.Value
	// change is the change from the observation to the declaration.
	change func() schemaext.ChangeValue
}

// decideStandalone records what the comparison decides for one object: an
// undecided change where either side could not establish the object or its
// absence and the desired source makes a claim, the observation kept where
// the desired source makes none, and a change where the claimed sides differ.
func decideStandalone(request schemaext.ObjectComparisonRequest, result *schemaext.ObjectComparisonResult, decision standaloneDecision) error {
	if decision.desired && standaloneLimited(request.Desired.Coverage, decision.kind, decision.ref) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: decision.kind, Subject: decision.ref,
			Reason: fmt.Sprintf("the desired source cannot describe the %s", decision.family)})
		return nil
	}
	// A desired source that neither declares the object nor describes its
	// namespace asks for nothing here, so an unread object has nothing to
	// decide; one read in full is kept.
	claimed := decision.desired || !unknown(request.Desired.Coverage.Lookup(decision.kind, decision.ref))
	currentKnowledge := request.Current.Coverage.Lookup(decision.kind, decision.ref)
	if standaloneLimited(request.Current.Coverage, decision.kind, decision.ref) || (!decision.current && unknown(currentKnowledge)) {
		if claimed {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: decision.kind, Subject: decision.ref,
				Reason: fmt.Sprintf("the %s or its absence was not established: %s", decision.family, currentKnowledge.Reason)})
		}
		return nil
	}
	switch {
	case !claimed && decision.current:
		var err error
		result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: decision.ref, Value: decision.kept()})
		return err
	case !claimed, !decision.desired && !decision.current:
		return nil
	case decision.desired && decision.current && decision.equal():
		return nil
	default:
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: decision.ref, Value: decision.change()})
		return nil
	}
}
