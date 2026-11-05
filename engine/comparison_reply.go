package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func (r *Runtime) validateComparisonReply(ctx context.Context, service int, request schemaext.ObjectComparisonRequest, result schemaext.ObjectComparisonResult) (schemaext.ObjectComparisonResult, error) {
	if !result.Complete {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: object comparison did not complete", schemaext.ErrInvalidValue)
	}
	var err error
	result.Desired, err = r.codecs.SnapshotObjectState(ctx, schemaext.Desired, result.Desired)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := comparisonDesired(request, result.Desired); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result.Changes, err = r.codecs.SnapshotChanges(ctx, result.Changes)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	seen := make(map[objectidentity.Key]bool)
	for _, change := range result.Changes {
		if !slices.Contains(r.comparisonServices[service].ChangeKinds, change.Value.Kind()) || !comparisonSubject(request, change.Subject) || seen[change.Subject.Key()] {
			return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison returned an unrelated or duplicate change for %s", schemaext.ErrInvalidValue, change.Subject)
		}
		if parent, found := comparisonParent(request.Parents, change.Subject); found && (!parent.Desired || !parent.Current) {
			return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: parent transition already owns change for %s", schemaext.ErrInvalidValue, change.Subject)
		}
		seen[change.Subject.Key()] = true
	}
	result.Undecided = slices.Clone(result.Undecided)
	type diagnosticKey struct {
		kind    schemaext.Kind
		subject objectidentity.Key
	}
	diagnostics := make(map[diagnosticKey]bool)
	for _, diagnostic := range result.Undecided {
		key := diagnosticKey{diagnostic.Kind, diagnostic.Subject.Key()}
		if !slices.Contains(request.Kinds, diagnostic.Kind) ||
			strings.TrimSpace(diagnostic.Reason) == "" || !comparisonDiagnosticSubject(request, diagnostic) ||
			diagnostics[key] || seen[diagnostic.Subject.Key()] {
			return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: invalid comparison diagnostic for %s", schemaext.ErrInvalidValue, diagnostic.Subject)
		}
		diagnostics[key] = true
	}
	result.Resolved, err = comparisonResolved(request, result.Resolved)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result.Counts, err = comparisonCounts(r.comparisonServices[service].ChangeKinds, result.Changes, result.Counts)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

// comparisonCounts checks the counts a comparer reports against its changes:
// each names a change kind of its service once, and no count is negative.
// Every addition and every removal it counts is a change of the kind of its
// own, since one change either adds an object, removes one, or changes one
// both sides hold, so the additions and the removals together are never more
// than the changes, and the changes never more than the three counts together.
func comparisonCounts(kinds []schemaext.Kind, changes []schemaext.ChangeRecord, counts []schemaext.ObjectCount) ([]schemaext.ObjectCount, error) {
	counted := make(map[schemaext.Kind]bool, len(counts))
	for _, count := range counts {
		if !slices.Contains(kinds, count.Kind) || counted[count.Kind] {
			return nil, fmt.Errorf("%w: comparison counted an unrelated or duplicate change kind %q", schemaext.ErrInvalidValue, count.Kind)
		}
		counted[count.Kind] = true
		changed := 0
		for _, change := range changes {
			if change.Value.Kind() == count.Kind {
				changed++
			}
		}
		if count.Matched < 0 || count.Added < 0 || count.Removed < 0 || count.Added+count.Removed > changed ||
			changed > count.Matched+count.Added+count.Removed {
			return nil, fmt.Errorf("%w: comparison counted %d matched, %d added and %d removed objects of change kind %q, which its %d changes of the kind contradict",
				schemaext.ErrInvalidValue, count.Matched, count.Added, count.Removed, count.Kind, changed)
		}
	}
	return slices.Clone(counts), nil
}

// objectCounts sums the counts of the services one comparison ran, by change
// kind. A kind is reported only when every service that ran and owns changes
// of the kind counted it: the count of part of a kind's objects would read as
// all of them.
type objectCounts struct {
	sums      map[schemaext.Kind]schemaext.ObjectCount
	uncounted map[schemaext.Kind]bool
}

// add takes the counts of a service that owns changes of kinds.
func (o *objectCounts) add(kinds []schemaext.Kind, counts []schemaext.ObjectCount) {
	o.prepare()
	for _, kind := range kinds {
		index := slices.IndexFunc(counts, func(count schemaext.ObjectCount) bool { return count.Kind == kind })
		if index < 0 {
			o.uncounted[kind] = true
			continue
		}
		sum := o.sums[kind]
		sum.Kind = kind
		sum.Matched += counts[index].Matched
		sum.Added += counts[index].Added
		sum.Removed += counts[index].Removed
		o.sums[kind] = sum
	}
}

// join takes the counts of the other comparison of one run. A kind an object
// service and a facet service both own is reported only when both counted it,
// for the reason [objectCounts] gives.
func (o *objectCounts) join(other objectCounts) {
	o.prepare()
	maps.Copy(o.uncounted, other.uncounted)
	for kind, count := range other.sums {
		sum := o.sums[kind]
		sum.Kind = kind
		sum.Matched += count.Matched
		sum.Added += count.Added
		sum.Removed += count.Removed
		o.sums[kind] = sum
	}
}

func (o *objectCounts) prepare() {
	if o.sums == nil {
		o.sums, o.uncounted = make(map[schemaext.Kind]schemaext.ObjectCount), make(map[schemaext.Kind]bool)
	}
}

// result returns the kinds every service counted, sorted by kind, and nil
// when there is none.
func (o *objectCounts) result() []schemaext.ObjectCount {
	var counts []schemaext.ObjectCount
	for _, kind := range slices.Sorted(maps.Keys(o.sums)) {
		if !o.uncounted[kind] {
			counts = append(counts, o.sums[kind])
		}
	}
	return counts
}

// comparisonResolved checks the written requests a comparer says it
// recognized: each a written request of its batch, answered once, naming a
// desired object the batch holds.
func comparisonResolved(request schemaext.ObjectComparisonRequest, resolved []schemaext.ResolvedRequest) ([]schemaext.ResolvedRequest, error) {
	answered := make(map[int]bool, len(resolved))
	for _, answer := range resolved {
		if answer.Request < 0 || answer.Request >= len(request.Requests) || request.Requests[answer.Request].Written == "" || answered[answer.Request] {
			return nil, fmt.Errorf("%w: comparison resolved no written request of its batch, or one twice", schemaext.ErrInvalidValue)
		}
		answered[answer.Request] = true
		_, found, err := request.Desired.Objects.Get(answer.Subject)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%w: comparison resolved %q to %s, which it compares no desired object as", schemaext.ErrInvalidValue,
				request.Requests[answer.Request].Written, answer.Subject)
		}
	}
	return slices.Clone(resolved), nil
}

func comparisonDesired(request schemaext.ObjectComparisonRequest, result schemaext.ObjectState) error {
	for _, ref := range request.Desired.Objects.Refs() {
		original, _, err := request.Desired.Objects.Get(ref)
		if err != nil {
			return err
		}
		returned, found, err := result.Objects.Get(ref)
		if err != nil {
			return err
		}
		if !found || !original.Value.Equal(returned.Value) {
			return fmt.Errorf("%w: comparison discarded an explicit declaration for %s", schemaext.ErrInvalidValue, ref)
		}
	}
	for _, ref := range result.Objects.Refs() {
		if !slices.Contains(request.Kinds, schemaext.Kind(ref.Kind)) || !comparisonSubject(request, ref) {
			return fmt.Errorf("%w: comparison invented an unrelated desired object %s", schemaext.ErrInvalidValue, ref)
		}
		if parent, found := comparisonParent(request.Parents, ref); found && !parent.Desired {
			return fmt.Errorf("%w: comparison preserved an object below a removed parent %s", schemaext.ErrInvalidValue, ref)
		}
	}
	return comparisonCoverage(request, result.Coverage)
}

func comparisonCoverage(request schemaext.ObjectComparisonRequest, result schemaext.Coverage) error {
	return validateComparisonCoverage(comparisonCoverageInput{
		kinds: request.Kinds, desired: request.Desired.Coverage, current: request.Current.Coverage,
		related: func(record schemaext.SubjectCoverage) bool { return comparisonCoverageSubject(request, record) },
		observedValue: func(_ schemaext.Kind, subject objectidentity.ID) bool {
			return slices.ContainsFunc(request.Current.Objects.Refs(), func(ref objectidentity.ID) bool { return ref.Key() == subject.Key() })
		},
	}, result)
}

func comparisonCoverageSubject(request schemaext.ObjectComparisonRequest, record schemaext.SubjectCoverage) bool {
	if !slices.Contains(request.Kinds, record.Kind) {
		return false
	}
	if record.Subject.Kind == objectidentity.Kind(record.Kind) {
		return comparisonSubject(request, record.Subject)
	}
	return slices.ContainsFunc(request.Parents, func(parent schemaext.ParentState) bool { return parent.Subject.Key() == record.Subject.Key() })
}

func comparisonDiagnosticSubject(request schemaext.ObjectComparisonRequest, diagnostic schemaext.UndecidedChange) bool {
	subject := diagnostic.Subject
	if subject == (objectidentity.ID{}) {
		for _, state := range []schemaext.ObjectState{request.Desired, request.Current} {
			knowledge := state.Coverage.Lookup(diagnostic.Kind, subject)
			if knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable {
				return true
			}
		}
		return false
	}
	if subject.Kind != objectidentity.KindTable && subject.Kind != objectidentity.Kind(diagnostic.Kind) {
		return false
	}
	return slices.ContainsFunc(request.Parents, func(parent schemaext.ParentState) bool { return parent.Subject.Key() == subject.Key() }) || comparisonSubject(request, subject)
}

func comparisonSubject(request schemaext.ObjectComparisonRequest, subject objectidentity.ID) bool {
	if !slices.Contains(request.Kinds, schemaext.Kind(subject.Kind)) {
		return false
	}
	if !subject.Parent.Empty() {
		if _, found := comparisonParent(request.Parents, subject); !found {
			return false
		}
	}
	for _, state := range []schemaext.ObjectState{request.Desired, request.Current} {
		if slices.ContainsFunc(state.Objects.Refs(), func(ref objectidentity.ID) bool { return ref.Key() == subject.Key() }) {
			return true
		}
		if _, found := state.Coverage.SubjectKnowledge(schemaext.Kind(subject.Kind), subject); found {
			return true
		}
	}
	return false
}

func comparisonParent(parents []schemaext.ParentState, subject objectidentity.ID) (schemaext.ParentState, bool) {
	if subject.Parent.Empty() {
		return schemaext.ParentState{}, false
	}
	ref := objectidentity.ID{Kind: objectidentity.KindTable, Database: subject.Database, Namespace: subject.Namespace, Catalog: subject.Catalog, Schema: subject.Schema, Name: subject.Parent}
	for _, parent := range parents {
		if parent.Subject.Key() == ref.Key() {
			return parent, true
		}
	}
	return schemaext.ParentState{}, false
}
