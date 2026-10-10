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
)

// Complete standalone comparisons share deterministic ordering and coverage
// receipts. Each owner still decides equality and constructs its typed changes.
func completeStandaloneComparison[T any](ctx context.Context, request schemaext.ObjectComparisonRequest,
	nodes map[objectidentity.Key]T, coverage schemaext.Coverage, kind schemaext.Kind, namespace string,
	ref func(T) objectidentity.ID, compare func(schemaext.ObjectComparisonRequest, T, *schemaext.ObjectComparisonResult) error,
) (schemaext.ObjectComparisonResult, error) {
	result, err := compareStandaloneNodes(ctx, request, nodes, coverage, ref, compare)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if knowledge := request.Current.Coverage.Lookup(kind, objectidentity.ID{}); unknown(knowledge) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: kind,
			Reason: namespace + " namespace was not fully inspected: " + knowledge.Reason})
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

// Owners whose omission policy preserves every unnamed object need only the
// named operands. Namespace-wide deletion policies also require the receipt
// checked by completeStandaloneComparison.
func compareStandaloneNodes[T any](ctx context.Context, request schemaext.ObjectComparisonRequest,
	nodes map[objectidentity.Key]T, coverage schemaext.Coverage,
	ref func(T) objectidentity.ID, compare func(schemaext.ObjectComparisonRequest, T, *schemaext.ObjectComparisonResult) error,
) (schemaext.ObjectComparisonResult, error) {
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: request.Desired.Objects, Coverage: coverage}}
	ordered := slices.Collect(maps.Values(nodes))
	slices.SortFunc(ordered, func(a, b T) int { return schemaext.CompareRefs(ref(a), ref(b)) })
	for _, node := range ordered {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		if err := compare(request, node, &result); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

type standalonePresence struct {
	ref              objectidentity.ID
	desired, current bool
}

func standaloneCoverage(request schemaext.ObjectComparisonRequest, nodes []standalonePresence, kind schemaext.Kind, namespace string,
	enroll func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error),
) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if len(kinds) == 0 {
		coverage, err := enroll(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe " + namespace}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = coverage.KindRecords()
	}
	records := make(map[objectidentity.Key]schemaext.SubjectCoverage)
	for _, record := range request.Desired.Coverage.SubjectRecords() {
		records[record.Subject.Key()] = record
	}
	for _, node := range nodes {
		if node.desired || !unknown(request.Desired.Coverage.Lookup(kind, node.ref)) {
			continue
		}
		knowledge := request.Current.Coverage.Lookup(kind, node.ref)
		if node.current && !standaloneLimited(request.Current.Coverage, kind, node.ref) {
			knowledge = schemaext.Knowledge{State: schemaext.Complete}
		}
		records[node.ref.Key()] = schemaext.SubjectCoverage{Kind: kind, Subject: node.ref, Knowledge: knowledge}
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, slices.Collect(maps.Values(records)))
}

func standaloneLimited(coverage schemaext.Coverage, kind schemaext.Kind, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(kind, ref)
	return found && unknown(knowledge)
}

func standaloneInputs[T any](ctx context.Context, request schemaext.ObjectComparisonRequest,
	kind schemaext.Kind, key capability.Capability, family, label string,
	collect func(context.Context, schemaext.ObjectState, schemaext.Representation, map[objectidentity.Key]T) error,
) (map[objectidentity.Key]T, error) {
	nodes, err := captureStandaloneInputs(ctx, request, kind, family, collect)
	if err != nil {
		return nil, err
	}
	if len(nodes) > 0 && !request.Capabilities.Has(key) {
		return nil, fmt.Errorf("%w: %s require %s", ptaherr.ErrUnsupportedFeature, label, key)
	}
	return nodes, nil
}

func captureStandaloneInputs[T any](ctx context.Context, request schemaext.ObjectComparisonRequest,
	kind schemaext.Kind, family string,
	collect func(context.Context, schemaext.ObjectState, schemaext.Representation, map[objectidentity.Key]T) error,
) (map[objectidentity.Key]T, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != "ydb" {
		return nil, fmt.Errorf("%w: YDB comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !request.Identifiers.Equal(identifier.ForDialect("ydb")) || !slices.Equal(request.Kinds, []schemaext.Kind{kind}) {
		return nil, fmt.Errorf("%w: invalid %s comparison vocabulary or identifiers", schemaext.ErrInvalidValue, family)
	}
	nodes := make(map[objectidentity.Key]T)
	for _, source := range []struct {
		state     schemaext.ObjectState
		direction schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		if err := collect(ctx, source.state, source.direction, nodes); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

// collectStandalone captures one kind's objects, then gives every subject its
// coverage records a place of its own, so a path recorded only as unread still
// reaches the comparison. A subject record must name the kind and cannot
// declare a default object.
func collectStandalone[D, O schemaext.Value](ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation,
	kind schemaext.Kind, family string, codecs []schemaext.Codec, validate func(objectidentity.ID) error, capture func(objectidentity.ID, D, O),
) error {
	if err := captureStandalone(ctx, state, direction, kind, codecs, validate, capture); err != nil {
		return err
	}
	for _, record := range state.Coverage.SubjectRecords() {
		if record.Kind != kind || record.Knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: %s coverage cannot declare another kind or a default object", schemaext.ErrInvalidValue, family)
		}
		if err := validate(record.Subject); err != nil {
			return err
		}
		var desired D
		var current O
		capture(record.Subject, desired, current)
	}
	return nil
}

// Capture validates identity, positive evidence, and the selected model codec
// before the owner's comparison sees a value. Desired and observed types remain
// separate, and cloned settings cannot be shared with a provider's request.
func captureStandalone[D, O schemaext.Value](ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation,
	kind schemaext.Kind, codecs []schemaext.Codec, validate func(objectidentity.ID) error, capture func(objectidentity.ID, D, O),
) error {
	var source schemaext.Codec
	for _, codec := range codecs {
		if codec.Representation == direction {
			source = codec
		}
	}
	if source.Prototype == nil {
		return fmt.Errorf("%w: invalid standalone representation", schemaext.ErrInvalidValue)
	}
	objects, err := state.Objects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validate(object.Ref); err != nil {
			return err
		}
		if knowledge := state.Coverage.Lookup(kind, object.Ref); knowledge.State == schemaext.Absent || knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: a present %s cannot be absent or defaulted", schemaext.ErrInvalidValue, kind)
		}
		snapshot, err := source.Clone(object.Value)
		if err != nil {
			return err
		}
		var desired D
		var observed O
		var ok bool
		if direction == schemaext.Desired {
			desired, ok = snapshot.(D)
		} else {
			observed, ok = snapshot.(O)
		}
		if !ok {
			return fmt.Errorf("%w: unexpected %s comparison operand %T", schemaext.ErrInvalidValue, kind, snapshot)
		}
		capture(object.Ref, desired, observed)
	}
	return nil
}
