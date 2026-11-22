package chcompare

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

// RowPolicyService compares ClickHouse row policies by database, table and
// name. Its zero value is ready for concurrent use without database access.
//
// The identity is resolved under the request's identifier rules: a
// declaration that leaves the database out names the connection's, which is
// where a read reports the policy. Filters are compared in the spelling the
// server stores where a live comparison attached it, and otherwise by tokens,
// so spacing never reads as a change. A policy whose table the plan creates or
// removes belongs to that table's transition, and is not compared here.
//
// ClickHouse binds a policy to a table name and keeps it when the table is
// dropped, so a read may report a policy below a table it does not hold. Such
// a detached policy is compared here, whatever the plan does to a table of
// the name: one the declaration leaves out is dropped, and a declared one is
// altered to the declaration even where both agree, because the plan cannot
// create a policy the server already holds (code 493,
// ACCESS_ENTITY_ALREADY_EXISTS). A policy the read records as one it cannot
// describe, on a relation it does not hold as a table, is kept and reported
// as undecided.
type RowPolicyService struct{}

type rowPolicyPair struct {
	desiredRef, currentRef objectidentity.ID
	desired                *chschema.DesiredRowPolicy
	current                *chschema.ObservedRowPolicy
}

// CompareObjects preserves observed policies a source cannot describe and
// never treats an unread namespace as empty: a declared policy whose presence
// was not established is undecided, and a policy the desired source could not
// describe is kept rather than dropped, because dropping one changes what its
// users read. It counts the policies it compares: the ones both sides hold,
// the creations and the drops. A policy it keeps, leaves undecided or leaves
// to its table's transition is counted on neither side. Inputs remain
// unchanged.
func (RowPolicyService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: row policy comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if request.Target != platform.ClickHouse {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: ClickHouse row policy comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{chschema.RowPolicyKind}) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported ClickHouse row policy comparison kinds", schemaext.ErrInvalidValue)
	}
	pairs, err := rowPolicyPairs(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	parents := make(map[objectidentity.Key]schemaext.ParentState, len(request.Parents))
	for _, parent := range request.Parents {
		parents[rekeyTable(request.Identifiers, parent.Subject).Key()] = parent
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: request.Desired.Objects},
		Counts: []schemaext.ObjectCount{{Kind: chdiff.RowPolicyKind}}}
	var adopted []schemaext.SubjectCoverage
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		parent, found := parents[rekeyTable(request.Identifiers, chschema.RowPolicyTable(pairRowPolicyRef(pair))).Key()]
		if pair.current != nil && (!found || !parent.Current) {
			if err := compareDetachedRowPolicy(request, pair, &result); err != nil {
				return schemaext.ObjectComparisonResult{}, err
			}
			continue
		}
		if found && (!parent.Desired || !parent.Current) {
			continue
		}
		record, err := compareRowPolicy(request, pair, &result)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		if record != nil {
			adopted = append(adopted, *record)
		}
	}
	reportUndescribedDetached(request, parents, &result)
	result.Desired.Coverage, err = rowPolicyCoverage(request, adopted)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

func compareRowPolicy(request schemaext.ObjectComparisonRequest, pair rowPolicyPair, result *schemaext.ObjectComparisonResult) (*schemaext.SubjectCoverage, error) {
	subject := pairRowPolicyRef(pair)
	if pair.desired != nil && rowPolicyLimited(request.Desired.Coverage, pair.desiredRef) {
		undecidedRowPolicy(result, subject, "the desired source cannot describe the complete row policy")
		return nil, nil
	}
	currentKnowledge := request.Current.Coverage.Lookup(chschema.RowPolicyKind, subject)
	if pair.current != nil && rowPolicyLimited(request.Current.Coverage, pair.currentRef) ||
		pair.current == nil && unknownKnowledge(currentKnowledge) {
		if pair.desired != nil {
			undecidedRowPolicy(result, subject, "the current row policy or its absence was not established: "+currentKnowledge.Reason)
		}
		return nil, nil
	}
	if pair.desired == nil {
		if unknownKnowledge(request.Desired.Coverage.Lookup(chschema.RowPolicyKind, pair.currentRef)) {
			// A description that could not express a row policy has not asked
			// for one to be dropped, and dropping it changes what its users
			// read.
			var err error
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: pair.currentRef, Value: pair.current.Desired()})
			if err != nil {
				return nil, err
			}
			return &schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: pair.currentRef,
				Knowledge: schemaext.Knowledge{State: schemaext.Complete}}, nil
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.currentRef, Value: chdiff.NewRowPolicy(pair.current, nil)})
		result.Counts[0].Removed++
		return nil, nil
	}
	if pair.current == nil {
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.desiredRef, Value: chdiff.NewRowPolicy(nil, pair.desired)})
		result.Counts[0].Added++
		return nil, nil
	}
	result.Counts[0].Matched++
	if chsql.SameRowPolicy(pair.desired, pair.current) {
		return nil, nil
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.desiredRef, Value: chdiff.NewRowPolicy(pair.current, pair.desired)})
	return nil, nil
}

// compareDetachedRowPolicy compares a policy the current side holds below a
// table it does not hold; see [RowPolicyService]. The server applies such a
// policy to the next table created under the name, so it is never left in
// place unseen: one the declaration does not state is dropped, and a declared
// one is altered in place to the declaration. A description that cannot
// state row policies has not asked for the drop, and the policy cannot be
// kept as declared beside a table the declaration does not hold, so it is
// reported as undecided.
func compareDetachedRowPolicy(request schemaext.ObjectComparisonRequest, pair rowPolicyPair, result *schemaext.ObjectComparisonResult) error {
	table := pair.currentRef.Parent.Source
	if pair.desired == nil {
		if knowledge := request.Desired.Coverage.Lookup(chschema.RowPolicyKind, pair.currentRef); unknownKnowledge(knowledge) {
			undecidedRowPolicy(result, pair.currentRef, fmt.Sprintf("the database holds this policy for table %q, which it does not hold, and applies it to a table "+
				"created under that name; the desired source does not describe row policies, so the plan neither drops nor keeps it", table))
			return nil
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.currentRef, Value: chdiff.NewRowPolicy(pair.current, nil)})
		result.Counts[0].Removed++
		return nil
	}
	if rowPolicyLimited(request.Desired.Coverage, pair.desiredRef) {
		undecidedRowPolicy(result, pair.desiredRef, "the desired source cannot describe the complete row policy")
		return nil
	}
	if pair.desiredRef.Key() != pair.currentRef.Key() {
		return fmt.Errorf("%w: declared row policy %s and the policy %s the database holds for a table it does not hold spell one policy two ways",
			schemaext.ErrInvalidValue, pair.desiredRef, pair.currentRef)
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.desiredRef, Value: chdiff.NewRowPolicy(pair.current, pair.desired)})
	result.Counts[0].Matched++
	return nil
}

// reportUndescribedDetached reports as undecided each policy the read could
// not describe below a table the current side does not hold, such as one on
// a view: the comparison keeps it, and saying nothing would hide a filter
// that still applies. A declaration of the same policy is the transition of
// a table the plan creates, which the database refuses to create over the
// relation that holds the name.
func reportUndescribedDetached(request schemaext.ObjectComparisonRequest, parents map[objectidentity.Key]schemaext.ParentState, result *schemaext.ObjectComparisonResult) {
	for _, record := range request.Current.Coverage.SubjectRecords() {
		if record.Kind != chschema.RowPolicyKind || !unknownKnowledge(record.Knowledge) || chschema.ValidateRowPolicyRef(record.Subject) != nil {
			continue
		}
		if parent, found := parents[rekeyTable(request.Identifiers, chschema.RowPolicyTable(record.Subject)).Key()]; found && parent.Current {
			continue
		}
		undecidedRowPolicy(result, record.Subject, record.Knowledge.Reason)
	}
}

func rowPolicyPairs(ctx context.Context, request schemaext.ObjectComparisonRequest) ([]rowPolicyPair, error) {
	pairs := make(map[objectidentity.Key]*rowPolicyPair)
	for _, source := range []struct {
		state     schemaext.ObjectState
		direction schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		objects, err := source.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := chschema.ValidateRowPolicyRef(object.Ref); err != nil {
				return nil, err
			}
			key := chschema.ResolveRowPolicyRef(request.Identifiers, object.Ref).Key()
			pair := pairs[key]
			if pair == nil {
				pair = &rowPolicyPair{}
				pairs[key] = pair
			}
			if err := captureRowPolicy(pair, object, source.direction); err != nil {
				return nil, err
			}
		}
	}
	result := make([]rowPolicyPair, 0, len(pairs))
	for _, key := range slices.SortedFunc(maps.Keys(pairs), func(a, b objectidentity.Key) int {
		return schemaext.CompareRefs(pairRowPolicyRef(*pairs[a]), pairRowPolicyRef(*pairs[b]))
	}) {
		result = append(result, *pairs[key])
	}
	return result, nil
}

func captureRowPolicy(pair *rowPolicyPair, object schemaext.Object, direction schemaext.Representation) error {
	switch value := object.Value.(type) {
	case *chschema.DesiredRowPolicy:
		if direction != schemaext.Desired || pair.desired != nil {
			return fmt.Errorf("%w: duplicate or misplaced ClickHouse row policy declaration %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if err := chschema.ValidateDesiredRowPolicy(value); err != nil {
			return err
		}
		pair.desiredRef, pair.desired = object.Ref, value
	case *chschema.ObservedRowPolicy:
		if direction != schemaext.Observed || pair.current != nil {
			return fmt.Errorf("%w: duplicate or misplaced ClickHouse row policy observation %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if err := chschema.ValidateObservedRowPolicy(value); err != nil {
			return err
		}
		pair.currentRef, pair.current = object.Ref, value
	default:
		return fmt.Errorf("%w: unexpected ClickHouse row policy operand %T", schemaext.ErrInvalidValue, object.Value)
	}
	return nil
}

func pairRowPolicyRef(pair rowPolicyPair) objectidentity.ID {
	if pair.desired != nil {
		return pair.desiredRef
	}
	return pair.currentRef
}

// rekeyTable resolves a table identity under the comparison's rules, as
// [chschema.ResolveRowPolicyRef] does for a policy, so a policy finds its
// parent whichever side spelled the database.
func rekeyTable(semantics identifier.Semantics, ref objectidentity.ID) objectidentity.ID {
	database := ref.Schema.Source
	if ref.Schema.Defaulted {
		database = ""
	}
	return objectidentity.NewBuilder(semantics).TablePartsVerbatim(database, ref.Name.Source)
}

// rowPolicyCoverage keeps the source's own claims and records the observed
// policies it adopted, so a later stage knows they are known rather than
// declared.
func rowPolicyCoverage(request schemaext.ObjectComparisonRequest, adopted []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if !slices.ContainsFunc(kinds, func(record schemaext.KindCoverage) bool { return record.Model.Kind == chschema.RowPolicyKind }) {
		enrolled, err := chschema.RowPolicyCoverage(schemaext.Desired,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe ClickHouse row policies"}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = append(kinds, enrolled.KindRecords()...)
	}
	records := make(map[objectidentity.Key]schemaext.SubjectCoverage)
	for _, record := range request.Desired.Coverage.SubjectRecords() {
		records[record.Subject.Key()] = record
	}
	for _, record := range adopted {
		records[record.Subject.Key()] = record
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, slices.Collect(maps.Values(records)))
}

func rowPolicyLimited(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(chschema.RowPolicyKind, ref)
	return found && unknownKnowledge(knowledge)
}

func unknownKnowledge(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

func undecidedRowPolicy(result *schemaext.ObjectComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: chschema.RowPolicyKind, Subject: subject, Reason: reason})
}
