package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbscheme"
)

// standalonePayload is the statement a standalone owner plans on its object.
type standalonePayload interface {
	ast.ExtensionPayload
	Effect() schemaext.Effect
}

// standaloneChange is one statement a standalone owner plans: the index of
// the change it answers, the object's identity, and the statement.
type standaloneChange[P standalonePayload] struct {
	input     int
	ref       objectidentity.ID
	operation P
}

// standalonePlanner holds what differs between the owners of standalone
// objects, a secret, a topic or a coordination node. [standalonePlanner.plan]
// holds what they share: the scope check, a refusal that discards the whole
// batch, path order, and one step per change, outside a transaction, with
// its effects on the object and on its scheme path.
type standalonePlanner[P standalonePayload] struct {
	// family names the steps, as in topic/000000/create.
	family string
	// scope names the owner in a refusal of the request's scope.
	scope string
	// kind is the change kind each receipt and each diagnostic reports.
	kind schemaext.Kind
	// placement is where each step prefers to run.
	placement plangraph.Placement
	// operations lowers the request's changes in input order. An error is a
	// request no owner could answer.
	operations func(context.Context, featureplan.Request) ([]standaloneChange[P], error)
	// refusals are the changes the target cannot take. Any refusal
	// discards the batch.
	refusals func(featureplan.Request, []standaloneChange[P]) []featureplan.Diagnostic
	action   func(P) plangraph.Action
	// dependencies orders one step against the common statements; an error
	// refuses the change.
	dependencies func(id plangraph.StepID, ref, slot objectidentity.ID, action plangraph.Action, common commonSteps) ([]plangraph.Dependency, error)
	strategy     func(P) string
}

// plan returns one operation per change, or only diagnostics when the batch
// is refused. A canceled context returns no result.
func (p standalonePlanner[P]) plan(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
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
		return featureplan.Result{}, fmt.Errorf("%w: invalid %s planning scope", schemaext.ErrInvalidValue, p.scope)
	}
	changes, err := p.operations(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if diagnostics := p.refusals(request, changes); len(diagnostics) > 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	slices.SortFunc(changes, func(a, b standaloneChange[P]) int { return schemaext.CompareRefs(a.ref, b.ref) })
	common := indexCommonSteps(request.CommonSteps)
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for index, change := range changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		action := p.action(change.operation)
		id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("%s/%06d/%s", p.family, index, action)}
		slot := ydbscheme.Path(change.ref.Schema.Source, change.ref.Name.Source)
		edges, err := p.dependencies(id, change.ref, slot, action, common)
		if err != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{standaloneDiagnostic(p.kind, change.input, change.ref, err)}}, nil
		}
		contribution.Dependencies = append(contribution.Dependencies, edges...)
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: change.operation},
			Effects:     []plangraph.Effect{{Subject: change.ref, Action: action}, {Subject: slot, Action: action}},
			Transaction: plangraph.TransactionForbidden, Impact: change.operation.Effect(), Placement: p.placement,
		})
		result.Changes[change.input] = featureplan.ChangePlan{Subject: change.ref, Kind: p.kind,
			Strategy: p.strategy(change.operation), Steps: []plangraph.StepID{id}}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

// standaloneDiagnostic refuses the change at input, of kind, on the object
// ref, for err.
func standaloneDiagnostic(kind schemaext.Kind, input int, ref objectidentity.ID, err error) featureplan.Diagnostic {
	return featureplan.Diagnostic{Change: new(input), Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.InvalidSchema, Kind: string(kind), Object: ref.String(), Message: err.Error(),
	}}
}
