package ydbplan

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
)

// PlanDeclarations creates named pools and classifiers. The default pool is
// already part of YDB; its declaration sets only named limits and preserves the
// rest. It never invents an observation to derive a full-state alteration.
func (s WorkloadService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	planning := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers,
		Capabilities: request.Capabilities, CommonSteps: request.CommonSteps}
	if err := workloadScope(ctx, planning); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	batch, err := workloadDeclarationChanges(request.Objects)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	planning.Changes = batch.changes
	reply, err := s.PlanFeatures(ctx, planning)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true, Contributions: reply.Contributions,
		Declarations: make([]featureplan.DeclarationPlan, len(request.Objects))}
	if len(reply.Diagnostics) > 0 {
		result.Contributions, result.Declarations = nil, nil
		for _, diagnostic := range reply.Diagnostics {
			problem := diagnostic.Clone().Problem
			var object *int
			input := batch.indexes[0]
			if diagnostic.Change != nil {
				input = batch.indexes[*diagnostic.Change]
				object = new(input)
			}
			problem.Kind = string(request.Objects[input].Value.Kind())
			result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Object: object, Problem: problem})
		}
		return result, nil
	}
	if refusal := workloadDeclarationRouting(request.Objects); refusal != nil {
		return featureplan.DeclarationResult{Complete: true, Diagnostics: []featureplan.DeclarationDiagnostic{{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.InvalidSchema, Kind: string(ydbworkload.ClassifierKind), Object: refusal.Subject, Message: refusal.Reason,
		}}}}, nil
	}
	for index, plan := range reply.Changes {
		result.Declarations[batch.indexes[index]] = featureplan.DeclarationPlan{Subject: plan.Subject, Steps: plan.Steps,
			Strategy: "create the declared workload object after its dependencies"}
	}
	if batch.defaultIndex != nil {
		result, err = declareDefaultPool(request, result, *batch.defaultIndex)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	return result, nil
}

// A creation document declares its routing destinations. Unlike a captured
// migration, it cannot silently rely on an undeclared pool already existing.
func workloadDeclarationRouting(objects []schemaext.Object) *ydbworkload.Refusal {
	var pools []string
	var classifiers []ydbworkload.Classifier
	for _, object := range objects {
		switch value := object.Value.(type) {
		case *ydbworkload.DesiredPool:
			pools = append(pools, object.Ref.Name.Source)
		case *ydbworkload.DesiredClassifier:
			classifiers = append(classifiers, ydbworkload.Classifier{Name: object.Ref.Name.Source, Spec: value.Spec})
		}
	}
	return ydbworkload.CheckRouting(pools, classifiers)
}

type workloadDeclarationBatch struct {
	changes      []schemaext.ChangeRecord
	indexes      []int
	defaultIndex *int
}

func workloadDeclarationChanges(objects []schemaext.Object) (workloadDeclarationBatch, error) {
	var batch workloadDeclarationBatch
	for index, object := range objects {
		var change schemaext.ChangeValue
		switch value := object.Value.(type) {
		case *ydbworkload.DesiredPool:
			if object.Ref == ydbworkload.PoolRef(ydbworkload.DefaultPool) {
				if batch.defaultIndex != nil {
					return workloadDeclarationBatch{}, fmt.Errorf("%w: duplicate default pool declaration", schemaext.ErrInvalidValue)
				}
				batch.defaultIndex = new(index)
				continue
			}
			change = &ydbdiff.ResourcePool{After: value}
		case *ydbworkload.DesiredClassifier:
			change = &ydbdiff.ResourcePoolClassifier{After: value}
		default:
			return workloadDeclarationBatch{}, fmt.Errorf("%w: expected a desired workload object, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		batch.changes = append(batch.changes, schemaext.ChangeRecord{Subject: object.Ref, Value: change})
		batch.indexes = append(batch.indexes, index)
	}
	return batch, nil
}

func declareDefaultPool(request featureplan.DeclarationRequest, result featureplan.DeclarationResult, index int) (featureplan.DeclarationResult, error) {
	object := request.Objects[index]
	value, err := ydbworkload.PoolCodecs()[0].Clone(object.Value)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	payload := &ydbast.DefaultPoolSettings{Spec: value.(*ydbworkload.DesiredPool).Spec}
	problem := schemavalidation.Diagnostic{Kind: string(ydbworkload.PoolKind), Object: object.Ref.String()}
	switch {
	case !request.Capabilities.Has(capability.ResourcePools):
		problem.Code, problem.Feature, problem.Message = schemavalidation.UnsupportedFeature, string(capability.ResourcePools), "this target does not support resource pools; "+ydbworkload.FlagHint
	default:
		if err := payload.Validate(); err != nil {
			problem.Code, problem.Message = schemavalidation.InvalidSchema, err.Error()
		}
	}
	if problem.Message != "" {
		return featureplan.DeclarationResult{Complete: true,
			Diagnostics: []featureplan.DeclarationDiagnostic{{Object: new(index), Problem: problem}}}, nil
	}
	result.Declarations[index] = featureplan.DeclarationPlan{Subject: object.Ref, Strategy: "set declared default-pool limits while preserving unspecified settings"}
	if ydbworkload.PoolsEqual(payload.Spec, ydbworkload.PoolSpec{}) {
		return result, nil
	}
	id := plangraph.StepID{Owner: "ptah.run/ydb", Name: "default-pool/settings"}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: id.Owner,
		Steps: []plangraph.Step[featureplan.Operation]{{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: payload},
			Effects: []plangraph.Effect{{Subject: object.Ref, Action: plangraph.Alter}}, Transaction: plangraph.TransactionForbidden, Impact: payload.Effect(),
		}}, Dependencies: workloadPrincipalDependencies(id, request.CommonSteps)}
	for _, other := range result.Contributions {
		for _, step := range other.Steps {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: id, After: step.ID})
		}
	}
	result.Contributions = append(result.Contributions, contribution)
	result.Declarations[index].Steps = []plangraph.StepID{id}
	return result, nil
}
