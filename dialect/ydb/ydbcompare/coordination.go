package ydbcompare

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
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
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: request.Desired.Objects, Coverage: coverage}}
	ordered := slices.Collect(maps.Values(nodes))
	slices.SortFunc(ordered, func(a, b coordinationNode) int { return schemaext.CompareRefs(a.ref, b.ref) })
	for _, node := range ordered {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		if err := compareCoordinationNode(request, node, &result); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
	}
	if knowledge := request.Current.Coverage.Lookup(ydbcoordination.Kind, objectidentity.ID{}); unknown(knowledge) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbcoordination.Kind,
			Reason: "coordination-node namespace was not fully inspected: " + knowledge.Reason})
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
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
	if ctx == nil {
		return nil, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != "ydb" {
		return nil, fmt.Errorf("%w: YDB comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !request.Identifiers.Equal(identifier.ForDialect("ydb")) || !slices.Equal(request.Kinds, []schemaext.Kind{ydbcoordination.Kind}) {
		return nil, fmt.Errorf("%w: invalid coordination comparison vocabulary or identifiers", schemaext.ErrInvalidValue)
	}
	nodes := make(map[objectidentity.Key]coordinationNode)
	for _, source := range []struct {
		state     schemaext.ObjectState
		direction schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		if err := collectCoordinationNodes(ctx, source.state, source.direction, nodes); err != nil {
			return nil, err
		}
	}
	if len(nodes) > 0 && !request.Capabilities.Has(capability.CoordinationNodes) {
		return nil, fmt.Errorf("%w: coordination nodes require %s", ptaherr.ErrUnsupportedFeature, capability.CoordinationNodes)
	}
	return nodes, nil
}

func collectCoordinationNodes(ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation, nodes map[objectidentity.Key]coordinationNode) error {
	objects, err := state.Objects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ydbcoordination.ValidateRef(object.Ref); err != nil {
			return err
		}
		if knowledge := state.Coverage.Lookup(ydbcoordination.Kind, object.Ref); knowledge.State == schemaext.Absent || knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: a present coordination node cannot be absent or defaulted", schemaext.ErrInvalidValue)
		}
		node := nodes[object.Ref.Key()]
		var spec ydbcoordination.Spec
		switch value := object.Value.(type) {
		case *ydbcoordination.Desired:
			if direction != schemaext.Desired || value == nil {
				return fmt.Errorf("%w: unexpected desired coordination node", schemaext.ErrInvalidValue)
			}
			node.desired, spec = value, value.Spec
		case *ydbcoordination.Observed:
			if direction != schemaext.Observed || value == nil {
				return fmt.Errorf("%w: unexpected observed coordination node", schemaext.ErrInvalidValue)
			}
			node.current, spec = value, value.Spec
		default:
			return fmt.Errorf("%w: expected a coordination node, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		if err := ydbcoordination.Validate(spec); err != nil {
			return err
		}
		node.ref = object.Ref
		nodes[object.Ref.Key()] = node
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
