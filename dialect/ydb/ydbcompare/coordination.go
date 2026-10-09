package ydbcompare

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
)

// CoordinationService compares standalone nodes without requiring table state.
// Defaults affect equality only; complete raw operands survive in each change.
type CoordinationService struct{}

type coordinationNode struct {
	ref     objectidentity.ID
	desired *ydbcoordination.Desired
	current *ydbcoordination.Observed
}

// CompareObjects preserves known observations omitted by an incomplete source.
// An incomplete observation never establishes absence or a destructive change.
func (CoordinationService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	nodes, err := coordinationInputs(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	coverage, err := coordinationCoverage(request, nodes)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return completeStandaloneComparison(ctx, request, nodes, coverage, ydbcoordination.Kind, "coordination-node",
		func(value coordinationNode) objectidentity.ID { return value.ref }, compareCoordinationNode)
}

func compareCoordinationNode(request schemaext.ObjectComparisonRequest, node coordinationNode, result *schemaext.ObjectComparisonResult) error {
	desiredKnowledge := request.Desired.Coverage.Lookup(ydbcoordination.Kind, node.ref)
	if node.desired != nil && coordinationLimited(request.Desired.Coverage, node.ref) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbcoordination.Kind, Subject: node.ref,
			Reason: "the desired source cannot describe the complete coordination node"})
		return nil
	}
	currentKnowledge := request.Current.Coverage.Lookup(ydbcoordination.Kind, node.ref)
	if coordinationLimited(request.Current.Coverage, node.ref) || (node.current == nil && unknown(currentKnowledge)) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbcoordination.Kind, Subject: node.ref,
			Reason: "the current coordination node or its absence was not established: " + currentKnowledge.Reason})
		return nil
	}
	if node.desired == nil && unknown(desiredKnowledge) {
		if node.current != nil {
			var err error
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: node.ref, Value: node.current.Desired()})
			return err
		}
		return nil
	}
	if node.desired == nil && node.current == nil {
		return nil
	}
	if node.desired != nil && node.current != nil && ydbcoordination.Changes(node.desired.Spec, node.current.Spec).IsZero() {
		return nil
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: node.ref, Value: &ydbdiff.CoordinationNode{Before: node.current, After: node.desired}})
	return nil
}

func coordinationLimited(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(ydbcoordination.Kind, ref)
	return found && unknown(knowledge)
}

func coordinationInputs(ctx context.Context, request schemaext.ObjectComparisonRequest) (map[objectidentity.Key]coordinationNode, error) {
	return standaloneInputs(ctx, request, ydbcoordination.Kind, capability.CoordinationNodes, "coordination", "coordination nodes", collectCoordinationNodes)
}

func collectCoordinationNodes(ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation, nodes map[objectidentity.Key]coordinationNode) error {
	err := captureStandalone(ctx, state, direction, ydbcoordination.Kind, ydbcoordination.Codecs(), ydbcoordination.ValidateRef,
		func(ref objectidentity.ID, desired *ydbcoordination.Desired, current *ydbcoordination.Observed) {
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
	return collectCoordinationCoverage(state.Coverage, nodes)
}

func collectCoordinationCoverage(coverage schemaext.Coverage, nodes map[objectidentity.Key]coordinationNode) error {
	for _, record := range coverage.SubjectRecords() {
		if record.Kind != ydbcoordination.Kind || record.Knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: coordination coverage cannot declare another kind or a default object", schemaext.ErrInvalidValue)
		}
		if err := ydbcoordination.ValidateIdentity(record.Subject); err != nil {
			return err
		}
		if !unknown(record.Knowledge) {
			if err := ydbcoordination.ValidateRef(record.Subject); err != nil {
				return err
			}
		}
		node := nodes[record.Subject.Key()]
		node.ref = record.Subject
		nodes[record.Subject.Key()] = node
	}
	return nil
}
