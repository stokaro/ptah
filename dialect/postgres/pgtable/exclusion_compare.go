package pgtable

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

type exclusionPair struct {
	key                    objectidentity.ID
	desiredRef, currentRef objectidentity.ID
	desired                *DesiredExclusion
	current                *ObservedExclusion
}

// CompareObjects compares constraints on surviving tables by structured
// identity. Creation and removal travel with their parent table. Unknown
// observations produce diagnostics; undeclared observations are preserved when
// the desired source cannot describe them. Errors and cancellation return no
// partial result, and the input snapshots remain unchanged.
func (ExclusionService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if err := exclusionContext(ctx, request.Target); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{ExclusionKind}) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported exclusion comparison kinds", schemaext.ErrInvalidValue)
	}
	if len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: exclusion comparison accepts no change requests", schemaext.ErrInvalidValue)
	}
	parents, err := exclusionParents(request.Parents)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	pairs, err := exclusionPairs(ctx, request, parents)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: request.Desired.Objects}}
	var adopted []schemaext.SubjectCoverage
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		parent := parents[ExclusionTable(pair.key).Key()]
		if !parent.Desired || !parent.Current {
			continue
		}
		record, err := compareExclusion(request, pair, &result)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		if record != nil {
			adopted = append(adopted, *record)
		}
	}
	undecidedExclusionNamespaces(request, &result)
	result.Desired.Coverage, err = comparedExclusionCoverage(request, adopted)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

func compareExclusion(request schemaext.ObjectComparisonRequest, pair exclusionPair, result *schemaext.ObjectComparisonResult) (*schemaext.SubjectCoverage, error) {
	if pair.desired != nil && limitedExclusion(request.Desired.Coverage, pair.desiredRef) {
		undecidedExclusion(result, pair.key, "the desired source cannot describe the complete exclusion constraint")
		return nil, nil
	}
	currentRef := pair.currentRef
	if pair.current == nil {
		currentRef = pair.key
	}
	currentKnowledge := request.Current.Coverage.Lookup(ExclusionKind, currentRef)
	if pair.current != nil && limitedExclusion(request.Current.Coverage, pair.currentRef) || pair.current == nil && unknownExclusion(currentKnowledge) {
		if pair.desired != nil {
			undecidedExclusion(result, pair.key, "the current exclusion constraint or its absence was not established: "+currentKnowledge.Reason)
		}
		return nil, nil
	}
	if pair.desired == nil {
		return dropOrAdoptExclusion(request, pair, result)
	}
	change := &ExclusionChange{After: pair.desired}
	if pair.current != nil {
		found := exclusionDifference(pair.desired, pair.current)
		if !found.definition && !found.comment {
			return nil, nil
		}
		change = &ExclusionChange{Before: pair.current, After: pair.desired, CommentOnly: !found.definition}
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.desiredRef, Value: change})
	return nil, nil
}

// dropOrAdoptExclusion answers an observed exclusion constraint nothing declares. A source that could
// not describe it has not asked for it to be dropped, so it is kept as a
// declaration of what the server holds; otherwise its absence is a request to
// drop it.
func dropOrAdoptExclusion(request schemaext.ObjectComparisonRequest, pair exclusionPair, result *schemaext.ObjectComparisonResult) (*schemaext.SubjectCoverage, error) {
	if slices.ContainsFunc(request.Required, func(ref objectidentity.ID) bool { return ref.Key() == pair.currentRef.Key() }) {
		undecidedExclusion(result, pair.currentRef, "the declared schema requires this exclusion constraint but provides no definition")
		return nil, nil
	}

	desiredKnowledge := request.Desired.Coverage.Lookup(ExclusionKind, pair.currentRef)
	if desiredKnowledge.State == schemaext.Defaulted {
		return nil, fmt.Errorf("%w: exclusion constraint %s requires a definition; a table has no default exclusion constraint", schemaext.ErrInvalidValue, pair.currentRef)
	}
	if unknownExclusion(desiredKnowledge) {
		converted, err := convertExclusion(schemaext.Observed, pair.current)
		if err != nil {
			return nil, err
		}
		result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: pair.currentRef, Value: converted})
		if err != nil {
			return nil, err
		}
		return &schemaext.SubjectCoverage{Kind: ExclusionKind, Subject: pair.currentRef,
			Knowledge: schemaext.Knowledge{State: schemaext.Complete}}, nil
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.currentRef,
		Value: &ExclusionChange{Before: pair.current}})
	return nil, nil
}

// exclusionDifferenceResult separates definition changes from comments.
type exclusionDifferenceResult struct {
	definition bool
	comment    bool
}

func exclusionDifference(declared *DesiredExclusion, observed *ObservedExclusion) exclusionDifferenceResult {
	return exclusionDifferenceResult{
		definition: ExclusionComparisonKey(comparedExclusion(declared)) != ExclusionComparisonKey(Exclusion(*observed)),
		comment:    declared.Comment != observed.Comment,
	}
}

// exclusionParents indexes the table lifecycles by identity.
func exclusionParents(states []schemaext.ParentState) (map[objectidentity.Key]schemaext.ParentState, error) {
	parents := make(map[objectidentity.Key]schemaext.ParentState, len(states))
	for _, parent := range states {
		if parent.Subject.Kind != objectidentity.KindTable || parent.Subject.Name.Empty() || (!parent.Desired && !parent.Current) {
			return nil, fmt.Errorf("%w: invalid exclusion constraint parent %s", schemaext.ErrInvalidValue, parent.Subject)
		}
		if _, found := parents[parent.Subject.Key()]; found {
			return nil, fmt.Errorf("%w: duplicate exclusion constraint parent %s", schemaext.ErrDuplicate, parent.Subject)
		}
		parents[parent.Subject.Key()] = parent
	}
	return parents, nil
}

// exclusionPairs pairs declarations with observations by identity, ordered by
// identity. An exclusion constraint whose table its own side does
// not hold is refused: an exclusion constraint cannot exist without its table.
func exclusionPairs(ctx context.Context, request schemaext.ObjectComparisonRequest, parents map[objectidentity.Key]schemaext.ParentState) ([]exclusionPair, error) {
	pairs := make(map[objectidentity.Key]*exclusionPair)
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
			if err := ValidateExclusionRef(object.Ref); err != nil {
				return nil, err
			}
			key := object.Ref
			parent := parents[ExclusionTable(key).Key()]
			if source.direction == schemaext.Desired && !parent.Desired || source.direction == schemaext.Observed && !parent.Current {
				return nil, fmt.Errorf("%w: exclusion constraint %s is on a table its %s schema does not hold", ptaherr.ErrInvalidSchemaDiff, object.Ref, source.direction)
			}
			pair := pairs[key.Key()]
			if pair == nil {
				pair = &exclusionPair{key: key}
				pairs[key.Key()] = pair
			}
			if err := captureExclusionPair(pair, object, source.direction); err != nil {
				return nil, err
			}
		}
	}
	result := make([]exclusionPair, 0, len(pairs))
	for _, key := range slices.SortedFunc(maps.Keys(pairs), func(a, b objectidentity.Key) int {
		return schemaext.CompareRefs(pairs[a].key, pairs[b].key)
	}) {
		result = append(result, *pairs[key])
	}
	return result, nil
}

func captureExclusionPair(pair *exclusionPair, object schemaext.Object, direction schemaext.Representation) error {
	switch value := object.Value.(type) {
	case *DesiredExclusion:
		if direction != schemaext.Desired {
			return fmt.Errorf("%w: an exclusion constraint declaration among the observations: %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if pair.desired != nil {
			return fmt.Errorf("%w: exclusion constraints %s and %s are one exclusion constraint on this target", ptaherr.ErrInvalidSchemaDiff, pair.desiredRef, object.Ref)
		}
		if err := ValidateDesiredExclusion(value); err != nil {
			return err
		}
		pair.desiredRef, pair.desired = object.Ref, value
	case *ObservedExclusion:
		if direction != schemaext.Observed {
			return fmt.Errorf("%w: an exclusion constraint observation among the declarations: %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if pair.current != nil {
			return fmt.Errorf("%w: observed exclusion constraints %s and %s are one exclusion constraint on this target", schemaext.ErrInvalidValue, pair.currentRef, object.Ref)
		}
		if err := ValidateObservedExclusion(value); err != nil {
			return err
		}
		pair.currentRef, pair.current = object.Ref, value
	default:
		return fmt.Errorf("%w: unexpected exclusion constraint operand %T", schemaext.ErrInvalidValue, object.Value)
	}
	return nil
}

// undecidedExclusionNamespaces reports each surviving table that declares exclusion constraints the
// read did not enumerate: whether each exists, and so whether to create or
// change it, is unknown. A table that declares none plans no removal there,
// because nothing in the unread namespace was observed, so a read that cannot
// enumerate exclusion constraints leaves the ones it did not see in place.
func undecidedExclusionNamespaces(request schemaext.ObjectComparisonRequest, result *schemaext.ObjectComparisonResult) {
	declared := make(map[objectidentity.Key]bool)
	for _, ref := range request.Desired.Objects.Refs() {
		declared[ExclusionTable(ref).Key()] = true
	}
	for _, parent := range request.Parents {
		if !parent.Desired || !parent.Current || !declared[parent.Subject.Key()] {
			continue
		}
		// A table subject reads the table's namespace claim, and the kind's
		// claim where the table has none.
		current := request.Current.Coverage.Lookup(ExclusionKind, parent.Subject)
		if unknownExclusion(current) {
			undecidedExclusion(result, parent.Subject, "the table's exclusion constraints were not enumerated: "+current.Reason)
		}
	}
}

// comparedExclusionCoverage keeps the source's claims and records the observed exclusion constraints
// the comparison adopted, so a later stage knows they are known rather than
// declared.
func comparedExclusionCoverage(request schemaext.ObjectComparisonRequest, adopted []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if len(kinds) == 0 {
		enrolled, err := ExclusionCoverage(schemaext.Desired,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe exclusion constraints"}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = enrolled.KindRecords()
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

func limitedExclusion(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(ExclusionKind, ref)
	return found && unknownExclusion(knowledge)
}

func unknownExclusion(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

func undecidedExclusion(result *schemaext.ObjectComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ExclusionKind, Subject: subject, Reason: reason})
}
