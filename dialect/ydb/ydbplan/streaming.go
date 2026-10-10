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
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
)

// StreamingService plans complete query changes around the captured common
// operations. Running queries stop before dependency changes and restart after
// them. Body changes require reset permission before any operation is returned.
type StreamingService struct{}

type streamingChange struct {
	input int
	ref   objectidentity.ID
	steps []*ydbast.StreamingQuery
	// before is the body of the query the change replaces or drops, empty
	// for a creation.
	before string
}

// PlanFeatures contributes scheme operations with explicit checkpoint effects
// and nontransactional execution. Invalid batches return no successful prefix.
func (StreamingService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
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
		return featureplan.Result{}, fmt.Errorf("%w: invalid standalone streaming planning scope", schemaext.ErrInvalidValue)
	}
	changes, diagnostics, err := streamingOperations(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if len(request.Changes) > 0 && !request.Capabilities.Has(capability.StreamingQueries) {
		diagnostics = append(diagnostics, featureplan.Diagnostic{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: string(ydbdiff.StreamingQueryKind),
			Feature: string(capability.StreamingQueries), Message: "this target does not support streaming queries",
		}})
	}
	if len(diagnostics) > 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	slices.SortFunc(changes, func(a, b streamingChange) int { return schemaext.CompareRefs(a.ref, b.ref) })
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(changes))}
	for index, change := range changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		plan := featureplan.ChangePlan{Subject: change.ref, Kind: ydbdiff.StreamingQueryKind, Strategy: "apply streaming settings in place while preserving topic offsets"}
		for ordinal, operation := range change.steps {
			id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("streaming/%06d/%d", index, ordinal)}
			action := streamingAction(operation)
			slot := ydbscheme.Path(operation.Schema, operation.Name)
			edges, err := schemePathDependencies("streaming query", id, slot, action, request.CommonSteps)
			if err != nil {
				return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{streamingDiagnostic(change.input, change.ref, err)}}, nil
			}
			contribution.Dependencies = append(contribution.Dependencies, edges...)
			for _, common := range request.CommonSteps {
				edge := plangraph.Dependency{Before: common.ID, After: id}
				if operation.RunsBeforeChanges() {
					edge = plangraph.Dependency{Before: id, After: common.ID}
				}
				contribution.Dependencies = append(contribution.Dependencies, edge)
			}
			if len(plan.Steps) > 0 {
				contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: plan.Steps[len(plan.Steps)-1], After: id})
			}
			contribution.Steps = append(contribution.Steps, streamingStep(id, change, operation, slot, action))
			plan.Steps = append(plan.Steps, id)
		}
		result.Changes[change.input] = plan
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func streamingOperations(ctx context.Context, request featureplan.Request) ([]streamingChange, []featureplan.Diagnostic, error) {
	var changes []streamingChange
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
		change, ok := cloned.Value.(*ydbdiff.StreamingQuery)
		if !ok || seen[record.Subject.Key()] {
			return nil, nil, fmt.Errorf("%w: unexpected or duplicate streaming change", schemaext.ErrInvalidValue)
		}
		seen[record.Subject.Key()] = true
		if err := ydbstreaming.ValidateIdentity(record.Subject); err != nil {
			return nil, nil, err
		}
		steps, err := lowerStreaming(record.Subject, change)
		if err != nil {
			diagnostics = append(diagnostics, streamingDiagnostic(index, record.Subject, err))
			continue
		}
		var before string
		if change.Before != nil {
			before = change.Before.Spec.Text
		}
		changes = append(changes, streamingChange{input: index, ref: record.Subject, steps: steps, before: before})
	}
	return changes, diagnostics, nil
}

func lowerStreaming(ref objectidentity.ID, change *ydbdiff.StreamingQuery) ([]*ydbast.StreamingQuery, error) {
	if err := change.Validate(); err != nil {
		return nil, err
	}
	operation := &ydbast.StreamingQuery{Schema: ref.Schema.Source, Name: ref.Name.Source}
	switch {
	case change.Before == nil:
		operation.Operation, operation.Spec = ydbast.StreamingCreate, change.After.Spec.Clone()
		operation.Creation.OrReplace = change.After.AllowStateReset
	case change.After == nil:
		operation.Operation = ydbast.StreamingDrop
	default:
		if ydbstreaming.Equal(change.Before.Spec, change.After.Spec) {
			return nil, nil
		}
		operation.Operation, operation.Spec, operation.Previous = ydbast.StreamingAlter, change.After.Spec.Clone(), change.Before.Spec.Clone()
		operation.AllowStateReset = change.After.AllowStateReset
	}
	if err := operation.Validate(); err != nil {
		return nil, err
	}
	if operation.Operation != ydbast.StreamingAlter || !ydbstreaming.Running(operation.Previous) {
		return []*ydbast.StreamingQuery{operation}, nil
	}
	stopped := operation.Previous.Clone()
	stopped.Run = new(false)
	stop := &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Schema: operation.Schema, Name: operation.Name, Spec: stopped, Previous: operation.Previous.Clone()}
	if ydbstreaming.Equal(stopped, operation.Spec) {
		return []*ydbast.StreamingQuery{stop}, nil
	}
	operation.Previous = stopped.Clone()
	return []*ydbast.StreamingQuery{stop, operation}, nil
}

// streamingStep is one statement of the change and its effects. A statement
// that runs before the plan's other changes is early, so it runs before a
// topic its query reads changes, and it reads the topics of the body it stops
// or drops; any other statement reads the topics of the body it leaves.
func streamingStep(id plangraph.StepID, change streamingChange, operation *ydbast.StreamingQuery, slot objectidentity.ID,
	action plangraph.Action,
) plangraph.Step[featureplan.Operation] {
	step := plangraph.Step[featureplan.Operation]{ID: id,
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: operation},
		Effects:     []plangraph.Effect{{Subject: change.ref, Action: action}, {Subject: slot, Action: action}},
		Transaction: plangraph.TransactionForbidden, Impact: operation.Effect(),
	}
	body := operation.Spec.Text
	if operation.RunsBeforeChanges() {
		step.Placement, body = plangraph.PlacementEarly, change.before
	}
	step.Effects = append(step.Effects, streamingTopicReads(body)...)
	return step
}

// streamingTopicReads is a read of each topic a query body names by a path
// relative to the database root. A statement that leaves the query running
// reads the topics of its new body, so a topic the plan creates or changes
// comes first; one that stops or drops the query is early and reads the
// topics of the body it stops, so it runs before a topic it reads changes or
// goes. A name that is not a topic's path reads nothing the plan changes.
func streamingTopicReads(body string) []plangraph.Effect {
	var effects []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, path := range ydbstreaming.NamedPaths(body) {
		ref, err := ydbtopic.ParsePath(path)
		if err != nil || seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		effects = append(effects, plangraph.Effect{Subject: ref, Action: plangraph.Read})
	}
	return effects
}

func streamingAction(operation *ydbast.StreamingQuery) plangraph.Action {
	switch operation.Operation {
	case ydbast.StreamingCreate:
		return plangraph.Create
	case ydbast.StreamingDrop:
		return plangraph.Drop
	default:
		return plangraph.Alter
	}
}

func streamingDiagnostic(index int, ref objectidentity.ID, err error) featureplan.Diagnostic {
	return featureplan.Diagnostic{Change: new(index), Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.InvalidSchema, Kind: string(ydbdiff.StreamingQueryKind), Object: ref.String(), Message: err.Error(),
	}}
}
