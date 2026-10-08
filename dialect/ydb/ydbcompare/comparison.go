// Package ydbcompare owns contextual comparison of named YDB schema features.
package ydbcompare

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbchangefeed"
)

// Service compares changefeeds individually, preserving omitted inspected
// streams when the desired source cannot describe their namespace. It does not
// execute statements or infer absence from an empty collection.
type Service struct{}

type stream struct {
	ref     objectidentity.ID
	desired *ydbschema.DesiredChangefeed
	current *ydbschema.ObservedChangefeed
}

// CompareObjects resolves target defaults only for semantic comparison. Its
// returned desired objects retain declaration spelling and complete adopted
// observation state. Changes below new or removed tables belong to table captures.
func (Service) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	streams, parents, err := comparisonInputs(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Coverage, err = effectiveCoverage(request, streams)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	ordered := slices.Collect(maps.Values(streams))
	slices.SortFunc(ordered, func(a, b stream) int { return schemaext.CompareRefs(a.ref, b.ref) })
	for _, value := range ordered {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		parent := parents[parentRef(value.ref).Key()]
		if !parent.Desired || !parent.Current {
			continue
		}
		if err := compareStream(request, value, &result); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
	}
	for _, parent := range request.Parents {
		if !parent.Desired || !parent.Current {
			continue
		}
		knowledge := request.Current.Coverage.Lookup(ydbschema.ChangefeedKind, parent.Subject)
		if unknown(knowledge) {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbschema.ChangefeedKind, Subject: parent.Subject,
				Reason: "changefeed namespace was not fully inspected: " + knowledge.Reason})
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

func compareStream(request schemaext.ObjectComparisonRequest, value stream, result *schemaext.ObjectComparisonResult) error {
	desiredKnowledge := request.Desired.Coverage.Lookup(ydbschema.ChangefeedKind, value.ref)
	if desiredKnowledge.State == schemaext.Defaulted {
		return fmt.Errorf("%w: changefeed %s requires a definition; the namespace has no default stream", schemaext.ErrInvalidValue, value.ref)
	}
	if value.desired == nil && unknown(desiredKnowledge) && value.current != nil {
		var err error
		result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: value.ref, Value: &ydbschema.DesiredChangefeed{Spec: value.current.Spec.Clone()}})
		if err != nil {
			return err
		}
		if !objectLimited(request.Current.Coverage, value.ref) {
			return nil
		}
	}
	if objectLimited(request.Desired.Coverage, value.ref) && value.desired != nil {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbschema.ChangefeedKind, Subject: value.ref, Reason: "the desired source cannot describe the complete changefeed"})
		return nil
	}
	currentKnowledge := request.Current.Coverage.Lookup(ydbschema.ChangefeedKind, value.ref)
	if objectLimited(request.Current.Coverage, value.ref) || (value.current == nil && unknown(currentKnowledge)) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbschema.ChangefeedKind, Subject: value.ref, Reason: "the current changefeed or its absence was not established: " + currentKnowledge.Reason})
		return nil
	}
	if value.desired == nil && unknown(desiredKnowledge) {
		return nil
	}
	if value.desired == nil && value.current == nil {
		return nil
	}
	if value.desired != nil && value.current != nil && ydbchangefeed.Equal(value.desired.Spec, value.current.Spec) {
		return nil
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: value.ref, Value: &ydbdiff.Changefeed{Before: value.current, After: value.desired}})
	return nil
}

func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

func objectLimited(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(ydbschema.ChangefeedKind, ref)
	return found && unknown(knowledge)
}

func parentRef(ref objectidentity.ID) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
}
