package ydbcompare

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// StreamingService compares standalone queries without requiring table state.
// Defaults affect equality only; complete raw operands survive in each change.
type StreamingService struct{}

type streamingQuery struct {
	ref     objectidentity.ID
	desired *ydbstreaming.Desired
	current *ydbstreaming.Observed
}

// CompareObjects preserves known observations omitted by an incomplete source.
// An incomplete observation never establishes absence or a destructive change.
func (StreamingService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	queries, err := streamingInputs(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	coverage, err := streamingCoverage(request, queries)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return completeStandaloneComparison(ctx, request, queries, coverage, ydbstreaming.Kind, "streaming-query",
		func(value streamingQuery) objectidentity.ID { return value.ref }, compareStreamingQuery)
}

func compareStreamingQuery(request schemaext.ObjectComparisonRequest, query streamingQuery, result *schemaext.ObjectComparisonResult) error {
	desiredKnowledge := request.Desired.Coverage.Lookup(ydbstreaming.Kind, query.ref)
	if query.desired != nil && streamingLimited(request.Desired.Coverage, query.ref) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbstreaming.Kind, Subject: query.ref,
			Reason: "the desired source cannot describe the complete streaming query"})
		return nil
	}
	currentKnowledge := request.Current.Coverage.Lookup(ydbstreaming.Kind, query.ref)
	if streamingLimited(request.Current.Coverage, query.ref) || (query.current == nil && unknown(currentKnowledge)) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbstreaming.Kind, Subject: query.ref,
			Reason: "the current streaming query or its absence was not established: " + currentKnowledge.Reason})
		return nil
	}
	if query.desired == nil && unknown(desiredKnowledge) {
		if query.current != nil {
			var err error
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: query.ref, Value: query.current.Desired()})
			return err
		}
		return nil
	}
	if query.desired == nil && query.current == nil {
		return nil
	}
	if query.desired != nil && query.current != nil && ydbstreaming.Equal(query.desired.Spec, query.current.Spec) {
		return nil
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: query.ref, Value: &ydbdiff.StreamingQuery{Before: query.current, After: query.desired}})
	return nil
}

func streamingLimited(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(ydbstreaming.Kind, ref)
	return found && unknown(knowledge)
}

func streamingInputs(ctx context.Context, request schemaext.ObjectComparisonRequest) (map[objectidentity.Key]streamingQuery, error) {
	return standaloneInputs(ctx, request, ydbstreaming.Kind, capability.StreamingQueries, "streaming", "streaming queries", collectStreamingQueries)
}

func collectStreamingQueries(ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation, nodes map[objectidentity.Key]streamingQuery) error {
	err := captureStandalone(ctx, state, direction, ydbstreaming.Kind, ydbstreaming.Codecs(), ydbstreaming.ValidateIdentity,
		func(ref objectidentity.ID, desired *ydbstreaming.Desired, current *ydbstreaming.Observed) {
			node := nodes[ref.Key()]
			node.ref = ref
			if desired != nil {
				node.desired = desired
			}
			if current != nil {
				node.current = current
			}
			nodes[ref.Key()] = node
		})
	if err != nil {
		return err
	}
	return collectStreamingCoverage(state.Coverage, nodes)
}

func collectStreamingCoverage(coverage schemaext.Coverage, queries map[objectidentity.Key]streamingQuery) error {
	for _, record := range coverage.SubjectRecords() {
		if record.Kind != ydbstreaming.Kind || record.Knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: streaming coverage cannot declare another kind or a default object", schemaext.ErrInvalidValue)
		}
		if err := ydbstreaming.ValidateIdentity(record.Subject); err != nil {
			return err
		}
		query := queries[record.Subject.Key()]
		query.ref = record.Subject
		queries[record.Subject.Key()] = query
	}
	return nil
}
