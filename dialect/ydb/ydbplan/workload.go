package ydbplan

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
)

// WorkloadService plans pools and classifiers together. Captured classifier
// ranks determine which moves require replacement. All settings and optional
// limits come from the supplied operands; planning performs no inspection.
type WorkloadService struct{}

type workloadPhase int

const (
	classifierRemoval workloadPhase = iota
	poolRemoval
	poolSettings
	classifierSettings
	classifierCreation
)

type workloadPayload interface {
	ast.ExtensionPayload
	Effect() schemaext.Effect
}

type workloadOperation struct {
	input   int
	phase   workloadPhase
	ref     objectidentity.ID
	payload workloadPayload
	effects []plangraph.Effect
}

// PlanFeatures returns explicit rank handoffs and principal ordering. An
// invalid batch returns no operations. Equal operands have a no-op receipt.
// Workload DDL cannot run in a SQL transaction. The zero value is ready for
// concurrent use, and returned operands share no mutable settings with inputs.
func (WorkloadService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := workloadScope(ctx, request); err != nil {
		return featureplan.Result{}, err
	}
	operations, diagnostics, err := workloadOperations(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if len(request.Changes) > 0 && !request.Capabilities.Has(capability.ResourcePools) {
		diagnostics = append(diagnostics, featureplan.Diagnostic{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: string(request.Changes[0].Value.Kind()), Feature: string(capability.ResourcePools),
			Message: "this target does not support resource pools and classifiers",
		}})
	}
	if len(diagnostics) != 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	slices.SortFunc(operations, func(a, b workloadOperation) int {
		if order := cmp.Compare(a.phase, b.phase); order != 0 {
			return order
		}
		return schemaext.CompareRefs(a.ref, b.ref)
	})
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for index, record := range request.Changes {
		result.Changes[index] = featureplan.ChangePlan{Subject: record.Subject, Kind: record.Value.Kind(),
			Strategy: "apply captured workload settings after releasing occupied classifier ranks"}
	}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	for index, operation := range operations {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("workload/%06d", index)}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: operation.payload}, Effects: operation.effects,
			Transaction: plangraph.TransactionForbidden, Impact: operation.payload.Effect(),
		})
		if index > 0 {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: contribution.Steps[index-1].ID, After: id})
		}
		contribution.Dependencies = append(contribution.Dependencies, workloadPrincipalDependencies(id, request.CommonSteps)...)
		result.Changes[operation.input].Steps = append(result.Changes[operation.input].Steps, id)
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func workloadScope(ctx context.Context, request featureplan.Request) error {
	if ctx == nil {
		return fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.Target != "ydb" {
		return fmt.Errorf("%w: YDB planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !request.Identifiers.Equal(identifier.ForDialect("ydb")) || len(request.ParentKinds) != 0 {
		return fmt.Errorf("%w: invalid standalone workload planning scope", schemaext.ErrInvalidValue)
	}
	return nil
}

func workloadOperations(ctx context.Context, request featureplan.Request) ([]workloadOperation, []featureplan.Diagnostic, error) {
	ranks, diagnostics := workloadRanks(request.Changes)
	var operations []workloadOperation
	seen := make(map[objectidentity.Key]bool)
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		cloned, err := record.Clone()
		if err != nil {
			return nil, nil, err
		}
		if seen[cloned.Subject.Key()] {
			return nil, nil, fmt.Errorf("%w: duplicate workload change", schemaext.ErrInvalidValue)
		}
		seen[cloned.Subject.Key()] = true
		var lowered []workloadOperation
		switch change := cloned.Value.(type) {
		case *ydbdiff.ResourcePool:
			lowered, err = lowerWorkloadPool(cloned.Subject, change)
		case *ydbdiff.ResourcePoolClassifier:
			lowered, err = lowerWorkloadClassifier(cloned.Subject, change, ranks)
		default:
			return nil, nil, fmt.Errorf("%w: unexpected workload change %T", schemaext.ErrInvalidValue, cloned.Value)
		}
		if err != nil {
			diagnostics = append(diagnostics, workloadDiagnostic(index, record, err))
			continue
		}
		for _, operation := range lowered {
			operation.input = index
			operations = append(operations, operation)
		}
	}
	return operations, diagnostics, nil
}

func workloadDiagnostic(index int, record schemaext.ChangeRecord, err error) featureplan.Diagnostic {
	return featureplan.Diagnostic{Change: new(index), Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.InvalidSchema, Kind: string(record.Value.Kind()), Object: record.Subject.String(), Message: err.Error(),
	}}
}

// Only modified classifiers need replacement to exchange occupied ranks.
// Explicit removals free their ranks before every in-place move and creation.
func workloadRanks(changes []schemaext.ChangeRecord) (map[int64]objectidentity.Key, []featureplan.Diagnostic) {
	before, after := make(map[int64]objectidentity.Key), make(map[int64]objectidentity.ID)
	observed := make(map[int64]objectidentity.ID)
	var diagnostics []featureplan.Diagnostic
	for index, record := range changes {
		change, ok := record.Value.(*ydbdiff.ResourcePoolClassifier)
		if !ok || change == nil {
			continue
		}
		if change.Before != nil {
			if other, found := observed[change.Before.Spec.Rank]; found {
				diagnostics = append(diagnostics, workloadDiagnostic(index, record, fmt.Errorf("captured classifiers %s and %s share rank %d", other, record.Subject, change.Before.Spec.Rank)))
			}
			observed[change.Before.Spec.Rank] = record.Subject
		}
		if change.Before != nil && change.After != nil {
			before[change.Before.Spec.Rank] = record.Subject.Key()
		}
		if change.After != nil {
			if other, found := after[change.After.Spec.Rank]; found {
				diagnostics = append(diagnostics, workloadDiagnostic(index, record, fmt.Errorf("classifiers %s and %s cannot share rank %d", other, record.Subject, change.After.Spec.Rank)))
			}
			after[change.After.Spec.Rank] = record.Subject
		}
	}
	return before, diagnostics
}

func lowerWorkloadPool(ref objectidentity.ID, change *ydbdiff.ResourcePool) ([]workloadOperation, error) {
	if err := change.Validate(); err != nil {
		return nil, err
	}
	if err := ydbworkload.ValidateIdentity(ref, ydbworkload.PoolKind); err != nil {
		return nil, err
	}
	if ref.Name.Source == ydbworkload.DefaultPool && (change.Before == nil || change.After == nil) {
		return nil, fmt.Errorf("%w: the default pool cannot be created or dropped", schemaext.ErrInvalidValue)
	}
	operation := &ydbast.ResourcePool{Name: ref.Name.Source}
	phase, action := poolSettings, plangraph.Alter
	if change.Before != nil {
		operation.Previous = new(change.Before.Spec.Clone())
	}
	if change.After != nil {
		operation.Spec = new(change.After.Spec.Clone())
	}
	switch {
	case change.Before == nil:
		operation.Operation, action = ydbast.PoolCreate, plangraph.Create
	case change.After == nil:
		operation.Operation, operation.Previous, phase, action = ydbast.PoolDrop, nil, poolRemoval, plangraph.Drop
	default:
		operation.Operation = ydbast.PoolAlter
	}
	if err := operation.Validate(); err != nil {
		return nil, err
	}
	if change.Before != nil && change.After != nil && ydbworkload.PoolsEqual(change.Before.Spec, change.After.Spec) {
		return nil, nil
	}
	return []workloadOperation{{phase: phase, ref: ref, payload: operation, effects: []plangraph.Effect{{Subject: ref, Action: action}}}}, nil
}

func lowerWorkloadClassifier(ref objectidentity.ID, change *ydbdiff.ResourcePoolClassifier, ranks map[int64]objectidentity.Key) ([]workloadOperation, error) {
	if err := change.Validate(); err != nil {
		return nil, err
	}
	if err := ydbworkload.ValidateIdentity(ref, ydbworkload.ClassifierKind); err != nil {
		return nil, err
	}
	operation := &ydbast.ResourcePoolClassifier{Name: ref.Name.Source}
	if change.Before != nil {
		operation.Previous = new(change.Before.Spec)
	}
	if change.After != nil {
		operation.Spec = new(change.After.Spec)
	}
	switch {
	case change.Before == nil:
		operation.Operation = ydbast.PoolCreate
	case change.After == nil:
		operation.Operation, operation.Previous = ydbast.PoolDrop, nil
	default:
		operation.Operation = ydbast.PoolAlter
	}
	if err := operation.Validate(); err != nil {
		return nil, err
	}
	if change.Before != nil && change.After != nil {
		if change.Before.Spec == change.After.Spec {
			return nil, nil
		}
		if holder, occupied := ranks[change.After.Spec.Rank]; occupied && holder != ref.Key() {
			drop := &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolDrop, Name: ref.Name.Source}
			create := &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: ref.Name.Source, Spec: operation.Spec}
			return []workloadOperation{
				classifierWorkloadOperation(ref, drop, new(change.Before.Spec.Rank), nil),
				classifierWorkloadOperation(ref, create, nil, new(change.After.Spec.Rank)),
			}, nil
		}
	}
	var before, after *int64
	if change.Before != nil {
		before = new(change.Before.Spec.Rank)
	}
	if change.After != nil {
		after = new(change.After.Spec.Rank)
	}
	return []workloadOperation{classifierWorkloadOperation(ref, operation, before, after)}, nil
}

func classifierWorkloadOperation(ref objectidentity.ID, payload *ydbast.ResourcePoolClassifier, before, after *int64) workloadOperation {
	phase, action := classifierSettings, plangraph.Alter
	if before == nil {
		phase, action = classifierCreation, plangraph.Create
	} else if after == nil {
		phase, action = classifierRemoval, plangraph.Drop
	}
	effects := []plangraph.Effect{{Subject: ref, Action: action}}
	switch {
	case before != nil && after != nil && *before == *after:
		effects = append(effects, plangraph.Effect{Subject: workloadRankRef(*before), Action: plangraph.Alter})
	default:
		if before != nil {
			effects = append(effects, plangraph.Effect{Subject: workloadRankRef(*before), Action: plangraph.Drop})
		}
		if after != nil {
			effects = append(effects, plangraph.Effect{Subject: workloadRankRef(*after), Action: plangraph.Create})
		}
	}
	// Pool and member names may refer to absent objects: YDB uses fallback
	// routing. A Read effect would incorrectly require their existence.
	return workloadOperation{phase: phase, ref: ref, payload: payload, effects: effects}
}

func workloadRankRef(rank int64) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts("ptah.run/ydb/classifier-rank", "", strconv.FormatInt(rank, 10))
}

func workloadPrincipalDependencies(id plangraph.StepID, common []featureplan.CommonStep) []plangraph.Dependency {
	var edges []plangraph.Dependency
	for _, step := range common {
		for _, effect := range step.Effects {
			if effect.Subject.Kind != objectidentity.KindRole || effect.Action == plangraph.Read {
				continue
			}
			edge := plangraph.Dependency{Before: step.ID, After: id}
			if effect.Action == plangraph.Drop {
				edge = plangraph.Dependency{Before: id, After: step.ID}
			}
			edges = append(edges, edge)
		}
	}
	return edges
}
