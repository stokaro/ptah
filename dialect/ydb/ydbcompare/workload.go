package ydbcompare

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
)

// PoolService compares database-wide pools while preserving omitted objects.
type PoolService struct{}

// ClassifierService compares database-wide routing without assuming ownership
// of classifiers omitted by an application's source.
type ClassifierService struct{}

// CompareObjects keeps unset and zero limits distinct and retains both operands.
func (PoolService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	return compareWorkload(ctx, request, workloadFamily{
		kind: ydbworkload.PoolKind, label: "resource pools", codecs: ydbworkload.PoolCodecs(),
		validate: validatePoolSnapshot,
		equal: func(desired, current schemaext.Value) bool {
			return ydbworkload.PoolsEqual(desired.(*ydbworkload.DesiredPool).Spec, current.(*ydbworkload.ObservedPool).Spec)
		},
		declare: func(current schemaext.Value) schemaext.Value { return current.(*ydbworkload.ObservedPool).Desired() },
		change: func(node workloadNode) schemaext.ChangeValue {
			change := &ydbdiff.ResourcePool{After: node.desired.(*ydbworkload.DesiredPool)}
			if node.current != nil {
				change.Before, _ = node.current.(*ydbworkload.ObservedPool)
			}
			return change
		},
	})
}

// CompareObjects requires inspected settings before changing routing or rank.
// Namespace completeness never authorizes removal of an omitted classifier.
func (ClassifierService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	return compareWorkload(ctx, request, workloadFamily{
		kind: ydbworkload.ClassifierKind, label: "resource pool classifiers", codecs: ydbworkload.ClassifierCodecs(),
		validate: func(ref objectidentity.ID, _ schemaext.Value) error {
			return ydbworkload.ValidateIdentity(ref, ydbworkload.ClassifierKind)
		},
		equal: func(desired, current schemaext.Value) bool {
			return desired.(*ydbworkload.DesiredClassifier).Spec == current.(*ydbworkload.ObservedClassifier).Spec
		},
		declare: func(current schemaext.Value) schemaext.Value {
			return current.(*ydbworkload.ObservedClassifier).Desired()
		},
		change: func(node workloadNode) schemaext.ChangeValue {
			change := &ydbdiff.ResourcePoolClassifier{After: node.desired.(*ydbworkload.DesiredClassifier)}
			if node.current != nil {
				change.Before, _ = node.current.(*ydbworkload.ObservedClassifier)
			}
			return change
		},
	})
}

type workloadNode struct {
	ref              objectidentity.ID
	desired, current schemaext.Value
}

type workloadFamily struct {
	kind     schemaext.Kind
	label    string
	codecs   []schemaext.Codec
	validate func(objectidentity.ID, schemaext.Value) error
	equal    func(schemaext.Value, schemaext.Value) bool
	declare  func(schemaext.Value) schemaext.Value
	change   func(workloadNode) schemaext.ChangeValue
}

func validatePoolSnapshot(ref objectidentity.ID, value schemaext.Value) error {
	switch node := value.(type) {
	case *ydbworkload.DesiredPool:
		return ydbworkload.ValidatePoolRef(ref, node.Spec)
	case *ydbworkload.ObservedPool:
		return ydbworkload.ValidatePoolRef(ref, node.Spec)
	default:
		return fmt.Errorf("%w: unexpected pool snapshot %T", schemaext.ErrInvalidValue, value)
	}
}

func compareWorkload(ctx context.Context, request schemaext.ObjectComparisonRequest, family workloadFamily) (schemaext.ObjectComparisonResult, error) {
	nodes, err := standaloneInputs(ctx, request, family.kind, capability.ResourcePools, family.label, family.label,
		func(ctx context.Context, state schemaext.ObjectState, representation schemaext.Representation, nodes map[objectidentity.Key]workloadNode) error {
			return captureWorkload(ctx, state, representation, nodes, family)
		})
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	coverage, err := workloadCoverage(request, nodes, family)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return completeStandaloneComparison(ctx, request, nodes, coverage, family.kind, family.label,
		func(node workloadNode) objectidentity.ID { return node.ref },
		func(request schemaext.ObjectComparisonRequest, node workloadNode, result *schemaext.ObjectComparisonResult) error {
			return compareWorkloadNode(request, node, result, family)
		})
}

func compareWorkloadNode(request schemaext.ObjectComparisonRequest, node workloadNode, result *schemaext.ObjectComparisonResult, family workloadFamily) error {
	var reason string
	switch {
	case node.desired == nil && node.current != nil && request.Desired.Coverage.Lookup(family.kind, node.ref).State == schemaext.Absent:
		reason = "removing a database-wide workload object requires an explicit operation, not source omission"
	case node.desired != nil && standaloneLimited(request.Desired.Coverage, family.kind, node.ref):
		reason = "the source cannot describe the complete workload object"
	case standaloneLimited(request.Current.Coverage, family.kind, node.ref):
		reason = "the current workload object's settings were not fully inspected"
	case node.current == nil && unknown(request.Current.Coverage.Lookup(family.kind, node.ref)):
		reason = "the current workload object or its absence was not established"
	case node.desired != nil && node.current == nil && family.kind == ydbworkload.PoolKind && node.ref.Name.Source == ydbworkload.DefaultPool:
		reason = "the database's default pool must be inspected before changing its settings"
	}
	if reason != "" {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: family.kind, Subject: node.ref, Reason: reason})
		return nil
	}
	if node.desired == nil {
		if node.current != nil {
			var err error
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: node.ref, Value: family.declare(node.current)})
			return err
		}
		return nil
	}
	if node.current == nil || !family.equal(node.desired, node.current) {
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: node.ref, Value: family.change(node)})
	}
	return nil
}

func captureWorkload(ctx context.Context, state schemaext.ObjectState, representation schemaext.Representation,
	nodes map[objectidentity.Key]workloadNode, family workloadFamily,
) error {
	validate := func(ref objectidentity.ID) error { return ydbworkload.ValidateIdentity(ref, family.kind) }
	err := captureStandalone(ctx, state, representation, family.kind, family.codecs, validate,
		func(ref objectidentity.ID, desired, current schemaext.Value) {
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
	for _, node := range nodes {
		for _, value := range []schemaext.Value{node.desired, node.current} {
			if value != nil {
				if err := family.validate(node.ref, value); err != nil {
					return err
				}
			}
		}
	}
	for _, record := range state.Coverage.SubjectRecords() {
		if record.Kind != family.kind || record.Knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: workload coverage cannot declare another kind or a default object", schemaext.ErrInvalidValue)
		}
		if err := validate(record.Subject); err != nil {
			return err
		}
		node := nodes[record.Subject.Key()]
		node.ref = record.Subject
		nodes[record.Subject.Key()] = node
	}
	return nil
}

func workloadCoverage(request schemaext.ObjectComparisonRequest, nodes map[objectidentity.Key]workloadNode, family workloadFamily) (schemaext.Coverage, error) {
	presence := make([]standalonePresence, 0, len(nodes))
	for _, node := range nodes {
		presence = append(presence, standalonePresence{ref: node.ref, desired: node.desired != nil, current: node.current != nil})
	}
	return standaloneCoverage(request, presence, family.kind, family.label,
		func(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
			return ydbworkload.Coverage(family.kind, representation, knowledge, subjects)
		})
}
