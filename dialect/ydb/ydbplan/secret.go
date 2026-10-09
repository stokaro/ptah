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
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretService plans statements on YDB secrets. A secret depends on nothing
// but its path, so each statement runs before the common statements, except
// the ones that free its path: a secret created where the plan drops a table
// follows that drop. Anything that reads a secret by path -- an external data
// source, an async replication or a transfer -- follows its creation or
// rotation. YDB records no dependency of those objects on a secret (DROP
// SECRET succeeds while a data source names it, measured on 26.2.1.14), so a
// drop that a statement of the same plan still reads is refused.
type SecretService struct{}

type secretChange struct {
	input     int
	ref       objectidentity.ID
	operation *ydbast.Secret
}

// PlanFeatures returns one operation per created, rotated or dropped secret,
// and an explicit no-op for a change that keeps one. A refused change returns
// no operation from the batch.
func (SecretService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
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
		return featureplan.Result{}, fmt.Errorf("%w: invalid secret planning scope", schemaext.ErrInvalidValue)
	}
	changes, err := secretOperations(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if diagnostics := secretRefusals(request, changes); len(diagnostics) > 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	slices.SortFunc(changes, func(a, b secretChange) int { return schemaext.CompareRefs(a.ref, b.ref) })
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for index, change := range changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		plan := featureplan.ChangePlan{Subject: change.ref, Kind: ydbdiff.SecretKind, Strategy: "keep the secret and the value it holds"}
		if change.operation == nil {
			result.Changes[change.input] = plan
			continue
		}
		action := secretAction(change.operation)
		id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("secret/%06d/%s", index, action)}
		edges, err := secretDependencies(id, change, action, request.CommonSteps)
		if err != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{secretDiagnostic(change.input, change.ref, err)}}, nil
		}
		contribution.Dependencies = append(contribution.Dependencies, edges...)
		slot := ydbscheme.Path(change.ref.Schema.Source, change.ref.Name.Source)
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: change.operation},
			Effects:     []plangraph.Effect{{Subject: change.ref, Action: action}, {Subject: slot, Action: action}},
			Transaction: plangraph.TransactionForbidden, Impact: change.operation.Effect(),
		})
		plan.Strategy, plan.Steps = secretStrategy(change.operation), []plangraph.StepID{id}
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

// secretDependencies orders one operation against the common statements:
// after the drop that frees its path, before the next common statement, and
// before every statement that reads the secret. A drop that a statement still
// reads is an error, since the reader would name a secret that is gone.
func secretDependencies(id plangraph.StepID, change secretChange, action plangraph.Action, common []featureplan.CommonStep) ([]plangraph.Dependency, error) {
	slot := ydbscheme.Path(change.ref.Schema.Source, change.ref.Name.Source)
	edges, err := schemePathDependencies("secret", id, slot, action, common)
	if err != nil {
		return nil, err
	}
	positions := make(map[plangraph.StepID]int, len(common))
	for position, step := range common {
		positions[step.ID] = position
	}
	next := 0
	for _, edge := range edges {
		if position, found := positions[edge.Before]; found && edge.After == id {
			next = max(next, position+1)
		}
	}
	if next < len(common) {
		edges = append(edges, plangraph.Dependency{Before: id, After: common[next].ID})
	}
	for _, step := range common {
		if !slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool {
			return effect.Action == plangraph.Read && effect.Subject.Key() == change.ref.Key()
		}) {
			continue
		}
		if action == plangraph.Drop {
			return nil, fmt.Errorf("secret %s is dropped while a statement of this plan reads it by its path",
				ydbsecret.Display(change.ref.Schema.Source, change.ref.Name.Source))
		}
		edges = append(edges, plangraph.Dependency{Before: id, After: step.ID})
	}
	return edges, nil
}

func secretOperations(ctx context.Context, request featureplan.Request) ([]secretChange, error) {
	changes := make([]secretChange, 0, len(request.Changes))
	seen := make(map[objectidentity.Key]bool)
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := record.Clone()
		if err != nil {
			return nil, err
		}
		change, ok := cloned.Value.(*ydbdiff.Secret)
		if !ok || seen[record.Subject.Key()] {
			return nil, fmt.Errorf("%w: unexpected or duplicate secret change", schemaext.ErrInvalidValue)
		}
		seen[record.Subject.Key()] = true
		if err := ydbsecret.ValidateIdentity(record.Subject); err != nil {
			return nil, err
		}
		if err := change.Validate(); err != nil {
			return nil, err
		}
		changes = append(changes, secretChange{input: index, ref: record.Subject, operation: lowerSecret(record.Subject, change)})
	}
	return changes, nil
}

// lowerSecret returns the statement a change needs, or nil when it keeps the
// secret as it is.
func lowerSecret(ref objectidentity.ID, change *ydbdiff.Secret) *ydbast.Secret {
	operation := &ydbast.Secret{Schema: ref.Schema.Source, Name: ref.Name.Source}
	switch {
	case change.Before == nil:
		operation.Operation, operation.ValueEnv = ydbast.SecretCreate, change.After.Variable(ref)
	case change.After == nil:
		operation.Operation = ydbast.SecretDrop
	case change.Rotates():
		operation.Operation, operation.ValueEnv = ydbast.SecretRotate, change.After.Variable(ref)
	default:
		return nil
	}
	return operation
}

// secretRefusals refuses, before any operation is returned, a statement the
// target cannot take: every one on a target without the secrets capability,
// and a creation or rotation whose variable no value may come from.
func secretRefusals(request featureplan.Request, changes []secretChange) []featureplan.Diagnostic {
	var diagnostics []featureplan.Diagnostic
	for _, change := range changes {
		if change.operation == nil {
			continue
		}
		subject := "secret " + change.operation.Path()
		if refusal := ydbsecret.Refuse(request.Target, request.Capabilities, subject); refusal != nil {
			return []featureplan.Diagnostic{{Change: new(change.input), Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.UnsupportedFeature, Kind: string(ydbdiff.SecretKind), Object: change.ref.String(),
				Feature: string(capability.Secrets), Message: refusal.Error(),
			}}}
		}
		if err := change.operation.Validate(); err != nil {
			diagnostics = append(diagnostics, secretDiagnostic(change.input, change.ref, err))
		}
	}
	return diagnostics
}

func secretAction(operation *ydbast.Secret) plangraph.Action {
	switch operation.Operation {
	case ydbast.SecretCreate:
		return plangraph.Create
	case ydbast.SecretDrop:
		return plangraph.Drop
	default:
		return plangraph.Alter
	}
}

func secretStrategy(operation *ydbast.Secret) string {
	switch operation.Operation {
	case ydbast.SecretCreate:
		return "create the secret with the value its variable holds when the plan runs"
	case ydbast.SecretDrop:
		return "drop the secret and the value it holds"
	default:
		return "give the secret the value its variable holds when the plan runs"
	}
}

func secretDiagnostic(index int, ref objectidentity.ID, err error) featureplan.Diagnostic {
	return featureplan.Diagnostic{Change: new(index), Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.InvalidSchema, Kind: string(ydbdiff.SecretKind), Object: ref.String(), Message: err.Error(),
	}}
}
