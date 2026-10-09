package ydbplan

import (
	"context"
	"fmt"
	"slices"

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
	"ptah.run/dialect/ydb/ydbscheme"
)

// CoordinationService lowers standalone node changes into the host's graph.
// A coordination node has no table owner. Its service calls cannot participate
// in a SQL transaction, and every change keeps that execution constraint.
type CoordinationService struct{}

type coordinationOperation struct {
	input int
	value *ydbast.CoordinationNode
}

// PlanFeatures returns complete operands, footprints, transaction requirements,
// and safety metadata. A refused node discards all operations in this batch.
func (CoordinationService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != "ydb" {
		return featureplan.Result{}, fmt.Errorf("%w: YDB planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !request.Identifiers.Equal(identifier.ForDialect("ydb")) || len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: invalid standalone coordination planning scope", schemaext.ErrInvalidValue)
	}
	operations, diagnostics, err := coordinationOperations(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if len(request.Changes) > 0 && !request.Capabilities.Has(capability.CoordinationNodes) {
		diagnostics = append(diagnostics, featureplan.Diagnostic{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: string(ydbdiff.CoordinationNodeKind),
			Feature: string(capability.CoordinationNodes), Message: "this target does not support coordination nodes",
		}})
	}
	if len(diagnostics) > 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	slices.SortFunc(operations, func(a, b coordinationOperation) int {
		return schemaext.CompareRefs(a.value.Subject(), b.value.Subject())
	})
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for index, operation := range operations {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		action := coordinationAction(operation.value)
		id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("coordination/%06d/%s", index, action)}
		slot := ydbscheme.Path(operation.value.Schema, operation.value.Name)
		edges, err := schemePathDependencies("coordination", id, slot, action, request.CommonSteps)
		if err != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Change: new(operation.input), Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.InvalidSchema, Kind: string(ydbdiff.CoordinationNodeKind), Object: operation.value.Subject().String(), Message: err.Error(),
			}}}}, nil
		}
		contribution.Dependencies = append(contribution.Dependencies, edges...)
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: operation.value},
			Effects:     []plangraph.Effect{{Subject: operation.value.Subject(), Action: action}, {Subject: slot, Action: action}},
			Transaction: plangraph.TransactionForbidden, Impact: operation.value.Effect(),
		})
		result.Changes[operation.input] = featureplan.ChangePlan{Subject: operation.value.Subject(), Kind: ydbdiff.CoordinationNodeKind,
			Strategy: "apply the captured configuration through the coordination service", Steps: []plangraph.StepID{id}}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

// The slot is shared with tables and other scheme objects, while the model
// identity stays specific to coordination nodes. Only replacing a dropped
// occupant is valid; an ALTER cannot turn another object kind into a node.
func schemePathDependencies(family string, id plangraph.StepID, slot objectidentity.ID, action plangraph.Action, common []featureplan.CommonStep) ([]plangraph.Dependency, error) {
	type use struct {
		id     plangraph.StepID
		action plangraph.Action
	}
	var uses []use
	var replacement plangraph.StepID
	wanted := plangraph.Create
	if action == plangraph.Create {
		wanted = plangraph.Drop
	}
	for _, step := range common {
		for _, effect := range step.Effects {
			if effect.Subject.Key() != slot.Key() {
				continue
			}
			uses = append(uses, use{step.ID, effect.Action})
			switch effect.Action {
			case plangraph.Read, plangraph.Alter:
			default:
				if effect.Action != wanted || replacement != (plangraph.StepID{}) || action == plangraph.Alter {
					return nil, fmt.Errorf("%s %s conflicts with %s at scheme path %s", family, action, effect.Action, slot)
				}
				replacement = step.ID
			}
		}
	}
	if len(uses) == 0 {
		return nil, nil
	}
	if replacement == (plangraph.StepID{}) || action == plangraph.Alter {
		return nil, fmt.Errorf("%s %s conflicts with an existing occupant at scheme path %s", family, action, slot)
	}
	// Changes to the other occupant must belong to its side of the handoff.
	// If the common graph requires the opposite order, scheduling finds a cycle.
	var edges []plangraph.Dependency
	if action == plangraph.Create {
		edges = append(edges, plangraph.Dependency{Before: replacement, After: id})
	} else {
		edges = append(edges, plangraph.Dependency{Before: id, After: replacement})
	}
	for _, use := range uses {
		if use.id == replacement {
			continue
		}
		if action == plangraph.Create {
			edges = append(edges, plangraph.Dependency{Before: use.id, After: replacement})
		} else {
			edges = append(edges, plangraph.Dependency{Before: replacement, After: use.id})
		}
	}
	return edges, nil
}

func coordinationOperations(ctx context.Context, request featureplan.Request) ([]coordinationOperation, []featureplan.Diagnostic, error) {
	var operations []coordinationOperation
	var diagnostics []featureplan.Diagnostic
	seen := make(map[objectidentity.Key]bool)
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		cloned, err := record.Clone()
		if err != nil {
			return nil, nil, err
		}
		change, ok := cloned.Value.(*ydbdiff.CoordinationNode)
		if !ok || seen[cloned.Subject.Key()] {
			return nil, nil, fmt.Errorf("%w: unexpected or duplicate coordination change", schemaext.ErrInvalidValue)
		}
		seen[cloned.Subject.Key()] = true
		value := &ydbast.CoordinationNode{Schema: record.Subject.Schema.Source, Name: record.Subject.Name.Source, Change: *change}
		if value.Subject() != record.Subject {
			return nil, nil, fmt.Errorf("%w: coordination change has an invalid standalone identity", schemaext.ErrInvalidValue)
		}
		if err := value.Validate(); err != nil {
			diagnostics = append(diagnostics, featureplan.Diagnostic{Change: new(index), Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.InvalidSchema, Kind: string(ydbdiff.CoordinationNodeKind), Object: record.Subject.String(), Message: err.Error(),
			}})
			continue
		}
		operations = append(operations, coordinationOperation{input: index, value: value})
	}
	return operations, diagnostics, nil
}

func coordinationAction(value *ydbast.CoordinationNode) plangraph.Action {
	if value.Change.Before == nil {
		return plangraph.Create
	}
	if value.Change.After == nil {
		return plangraph.Drop
	}
	return plangraph.Alter
}
