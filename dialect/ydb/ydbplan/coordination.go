package ydbplan

import (
	"context"
	"fmt"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
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

// coordinationPlanner plans coordination node statements on the shared
// skeleton. A node statement is not early: its dependencies and its name
// place it.
func coordinationPlanner() standalonePlanner[*ydbast.CoordinationNode] {
	return standalonePlanner[*ydbast.CoordinationNode]{family: "coordination", scope: "standalone coordination",
		kind: ydbdiff.CoordinationNodeKind, operations: coordinationOperations, refusals: coordinationRefusals,
		action: coordinationAction, strategy: func(*ydbast.CoordinationNode) string {
			return "apply the captured configuration through the coordination service"
		},
		dependencies: func(id plangraph.StepID, _, slot objectidentity.ID, action plangraph.Action, common commonSteps) ([]plangraph.Dependency, error) {
			return schemePathDependencies("coordination", id, slot, action, common.steps)
		}}
}

// PlanFeatures returns complete operands, footprints, transaction requirements,
// and safety metadata. A refused node discards all operations in this batch.
func (CoordinationService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return coordinationPlanner().plan(ctx, request)
}

// schemePathDependencies orders a standalone object's statement against the
// common statements at its scheme path and above it. The slot is shared with
// tables and other scheme objects, while the model identity stays specific to
// the owner's family. Only replacing a dropped occupant is valid; an ALTER
// cannot turn another object kind into this one. Every directory above the
// slot must be a directory, so a creation follows a common drop at any of
// them, and a drop precedes a common creation there.
func schemePathDependencies(family string, id plangraph.StepID, slot objectidentity.ID, action plangraph.Action, common []featureplan.CommonStep) ([]plangraph.Dependency, error) {
	edges, err := slotDependencies(family, id, slot, action, common)
	if err != nil {
		return nil, err
	}
	return append(edges, directoryDependencies(id, slot, action, common)...), nil
}

// directoryDependencies orders a creation after every common drop at a
// directory above slot, and a drop before every common creation there.
func directoryDependencies(id plangraph.StepID, slot objectidentity.ID, action plangraph.Action, common []featureplan.CommonStep) []plangraph.Dependency {
	if action == plangraph.Alter {
		return nil
	}
	above := make(map[objectidentity.Key]bool)
	for _, directory := range ydbscheme.DirectoriesAbove(slot) {
		above[directory.Key()] = true
	}
	var edges []plangraph.Dependency
	for _, step := range common {
		for _, effect := range step.Effects {
			if !above[effect.Subject.Key()] {
				continue
			}
			switch {
			case action == plangraph.Create && effect.Action == plangraph.Drop:
				edges = append(edges, plangraph.Dependency{Before: step.ID, After: id})
			case action == plangraph.Drop && effect.Action == plangraph.Create:
				edges = append(edges, plangraph.Dependency{Before: id, After: step.ID})
			}
		}
	}
	return edges
}

// slotDependencies orders the handoff of the slot itself.
func slotDependencies(family string, id plangraph.StepID, slot objectidentity.ID, action plangraph.Action, common []featureplan.CommonStep) ([]plangraph.Dependency, error) {
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

func coordinationOperations(ctx context.Context, request featureplan.Request) ([]standaloneChange[*ydbast.CoordinationNode], error) {
	operations := make([]standaloneChange[*ydbast.CoordinationNode], 0, len(request.Changes))
	seen := make(map[objectidentity.Key]bool)
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := record.Clone()
		if err != nil {
			return nil, err
		}
		change, ok := cloned.Value.(*ydbdiff.CoordinationNode)
		if !ok || seen[cloned.Subject.Key()] {
			return nil, fmt.Errorf("%w: unexpected or duplicate coordination change", schemaext.ErrInvalidValue)
		}
		seen[cloned.Subject.Key()] = true
		value := &ydbast.CoordinationNode{Schema: record.Subject.Schema.Source, Name: record.Subject.Name.Source, Change: *change}
		if value.Subject() != record.Subject {
			return nil, fmt.Errorf("%w: coordination change has an invalid standalone identity", schemaext.ErrInvalidValue)
		}
		operations = append(operations, standaloneChange[*ydbast.CoordinationNode]{input: index, ref: record.Subject, operation: value})
	}
	return operations, nil
}

// coordinationRefusals refuses, before any operation is returned, a node
// whose operands are invalid, and every change on a target without
// coordination nodes.
func coordinationRefusals(request featureplan.Request, operations []standaloneChange[*ydbast.CoordinationNode]) []featureplan.Diagnostic {
	var diagnostics []featureplan.Diagnostic
	for _, operation := range operations {
		if err := operation.operation.Validate(); err != nil {
			diagnostics = append(diagnostics, standaloneDiagnostic(ydbdiff.CoordinationNodeKind, operation.input, operation.ref, err))
		}
	}
	if len(request.Changes) > 0 && !request.Capabilities.Has(capability.CoordinationNodes) {
		diagnostics = append(diagnostics, featureplan.Diagnostic{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: string(ydbdiff.CoordinationNodeKind),
			Feature: string(capability.CoordinationNodes), Message: "this target does not support coordination nodes",
		}})
	}
	return diagnostics
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
