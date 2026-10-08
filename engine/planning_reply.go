package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

func (r *Runtime) validatePlanningReply(ctx context.Context, service int, request featureplan.Request, reply featureplan.Result) (featureplan.Result, error) {
	if len(reply.Changes) != len(request.Changes) {
		return featureplan.Result{}, fmt.Errorf("%w: planning changed the result count", schemaext.ErrInvalidValue)
	}
	owner := r.planningServices[service]
	result := featureplan.Result{Contributions: make([]plangraph.Contribution[featureplan.Operation], len(reply.Contributions)), Changes: slices.Clone(reply.Changes)}
	steps := make(map[plangraph.StepID]bool)
	for i, contribution := range reply.Contributions {
		if contribution.Owner != owner.owner {
			return featureplan.Result{}, fmt.Errorf("%w: planning changed its contribution owner", schemaext.ErrInvalidValue)
		}
		cloned := contribution
		cloned.Steps = slices.Clone(contribution.Steps)
		cloned.Dependencies = slices.Clone(contribution.Dependencies)
		for j, step := range cloned.Steps {
			if step.ID.Owner != owner.owner || !reversalText(step.ID.Name) || steps[step.ID] {
				return featureplan.Result{}, fmt.Errorf("%w: duplicate or invalid planning step", schemaext.ErrInvalidValue)
			}
			steps[step.ID] = true
			operation, err := r.snapshotPlannedOperation(ctx, owner, request.Tables, step.Payload)
			if err != nil {
				return featureplan.Result{}, err
			}
			step.Payload = operation
			step.Effects = slices.Clone(step.Effects)
			cloned.Steps[j] = step
		}
		result.Contributions[i] = cloned
	}
	if err := validatePlannedChanges(request.Changes, result.Changes, steps); err != nil {
		return featureplan.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func validatePlannedChanges(inputs []schemaext.ChangeRecord, changes []featureplan.ChangePlan, steps map[plangraph.StepID]bool) error {
	covered := make(map[plangraph.StepID]bool)
	for i, change := range changes {
		if change.Subject != inputs[i].Subject || change.Kind != inputs[i].Value.Kind() || !reversalText(change.Strategy) {
			return fmt.Errorf("%w: planning changed a subject/kind or omitted its strategy", schemaext.ErrInvalidValue)
		}
		seen := make(map[plangraph.StepID]bool)
		for _, step := range change.Steps {
			if !steps[step] || seen[step] {
				return fmt.Errorf("%w: change names a missing or duplicate planning step", schemaext.ErrInvalidValue)
			}
			seen[step], covered[step] = true, true
		}
		changes[i].Steps = slices.Clone(change.Steps)
	}
	if len(covered) != len(steps) {
		return fmt.Errorf("%w: planning emitted an unaccounted step", schemaext.ErrInvalidValue)
	}
	return nil
}

func (r *Runtime) snapshotPlannedOperation(ctx context.Context, owner ownedPlanning, tables []featureplan.Table, operation featureplan.Operation) (featureplan.Operation, error) {
	for _, note := range operation.Notes {
		if !reversalText(note) {
			return featureplan.Operation{}, fmt.Errorf("%w: planning operation has an invalid note", schemaext.ErrInvalidValue)
		}
	}
	if err := schemaext.ValidatePayload(operation.Payload); err != nil {
		return featureplan.Operation{}, err
	}
	if !slices.Contains(owner.OperationKinds, operation.Payload.Kind()) {
		return featureplan.Operation{}, fmt.Errorf("%w: unregistered planning operation %q", schemaext.ErrInvalidValue, operation.Payload.Kind())
	}
	switch operation.Role {
	case ast.StatementExtension:
		if operation.Parent != (objectidentity.ID{}) {
			return featureplan.Operation{}, fmt.Errorf("%w: standalone operation carries an ALTER parent", schemaext.ErrInvalidValue)
		}
	case ast.AlterExtension:
		if operation.Parent.Kind != objectidentity.KindTable || !slices.ContainsFunc(tables, func(table featureplan.Table) bool { return table.Subject == operation.Parent }) {
			return featureplan.Operation{}, fmt.Errorf("%w: operation has no captured parent", schemaext.ErrInvalidValue)
		}
	default:
		return featureplan.Operation{}, fmt.Errorf("%w: unknown planning operation role", schemaext.ErrInvalidValue)
	}
	payloads, err := r.codecs.SnapshotPayloads(ctx, schemaext.Operation, []schemaext.Payload{operation.Payload})
	if err != nil {
		return featureplan.Operation{}, err
	}
	payload, ok := payloads[0].(ast.ExtensionPayload)
	if !ok {
		return featureplan.Operation{}, fmt.Errorf("%w: operation codec returned a non-operation", schemaext.ErrInvalidValue)
	}
	operation.Payload = payload
	operation.Notes = slices.Clone(operation.Notes)
	return operation, nil
}
