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
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretService plans statements on YDB secrets. A secret depends on nothing
// but its path, so each statement is early and runs before the common
// statements, except when it has to follow one that frees its path: a secret
// created where the plan drops a table, or beneath a path where it drops one,
// follows that drop, since every directory above a secret must be free of any
// other object. A dropped secret likewise goes before a common statement that
// creates something at its path or at a directory above it. Anything that
// reads a secret by path -- an external data source, an async replication or a
// transfer -- follows its creation or rotation. YDB records no dependency of
// those objects on a secret (DROP SECRET succeeds while a data source names
// it, measured on 26.2.1.14), so a drop that a statement of the same plan
// still reads is refused.
type SecretService struct{}

// secretChange is one statement on a secret.
type secretChange = standaloneChange[*ydbast.Secret]

// secretPlanner plans secret statements on the shared skeleton. A secret
// statement is early.
func secretPlanner() standalonePlanner[*ydbast.Secret] {
	return standalonePlanner[*ydbast.Secret]{family: "secret", scope: "secret", kind: ydbdiff.SecretKind,
		placement: plangraph.PlacementEarly, operations: secretOperations, refusals: secretRefusals, action: secretAction,
		dependencies: secretDependencies, strategy: secretStrategy}
}

// PlanFeatures returns one operation per created, rotated or dropped secret.
// A refused change returns no operation from the batch.
func (SecretService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return secretPlanner().plan(ctx, request)
}

// commonSteps indexes the host's statements once per batch: each statement
// that touches a subject, in the host's order.
type commonSteps struct {
	steps []featureplan.CommonStep
	uses  map[objectidentity.Key][]commonUse
}

type commonUse struct {
	position int
	action   plangraph.Action
}

func indexCommonSteps(steps []featureplan.CommonStep) commonSteps {
	index := commonSteps{steps: steps, uses: make(map[objectidentity.Key][]commonUse)}
	for position, step := range steps {
		for _, effect := range step.Effects {
			key := effect.Subject.Key()
			index.uses[key] = append(index.uses[key], commonUse{position: position, action: effect.Action})
		}
	}
	return index
}

// secretDependencies orders one operation on the secret ref, at the scheme
// path slot, against the common statements: the handoffs at its path and the
// directories above it (see [schemePathDependencies]), and before every
// statement that reads the secret. A drop that a statement still reads is an
// error, since the reader would name a secret that is gone. The operation is
// early, so it runs as soon as these allow, ahead of the common statements;
// a dependency on one of them would be one another owner's handoff could
// have to break.
func secretDependencies(id plangraph.StepID, ref, slot objectidentity.ID, action plangraph.Action, common commonSteps) ([]plangraph.Dependency, error) {
	edges, err := schemePathDependencies("secret", id, slot, action, common.steps)
	if err != nil {
		return nil, err
	}
	for _, use := range common.uses[ref.Key()] {
		if use.action != plangraph.Read {
			continue
		}
		if action == plangraph.Drop {
			return nil, fmt.Errorf("secret %s is dropped while a statement of this plan reads it by its path",
				ydbsecret.Display(ref.Schema.Source, ref.Name.Source))
		}
		edges = append(edges, plangraph.Dependency{Before: id, After: common.steps[use.position].ID})
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

// lowerSecret returns the statement a change needs.
func lowerSecret(ref objectidentity.ID, change *ydbdiff.Secret) *ydbast.Secret {
	operation := &ydbast.Secret{Schema: ref.Schema.Source, Name: ref.Name.Source}
	switch {
	case change.Before == nil:
		operation.Operation, operation.ValueEnv = ydbast.SecretCreate, change.After.Variable(ref)
	case change.After == nil:
		operation.Operation = ydbast.SecretDrop
	default:
		operation.Operation, operation.ValueEnv = ydbast.SecretRotate, change.After.Variable(ref)
	}
	return operation
}

// secretRefusals refuses, before any operation is returned, a statement the
// target cannot take: every one on a target without the secrets capability,
// and a creation or rotation whose variable no value may come from.
func secretRefusals(request featureplan.Request, changes []secretChange) []featureplan.Diagnostic {
	var diagnostics []featureplan.Diagnostic
	for _, change := range changes {
		subject := "secret " + change.operation.Path()
		if refusal := ydbsecret.Refuse(request.Target, request.Capabilities, subject); refusal != nil {
			return []featureplan.Diagnostic{{Change: new(change.input), Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.UnsupportedFeature, Kind: string(ydbdiff.SecretKind), Object: change.ref.String(),
				Feature: string(capability.Secrets), Message: refusal.Error(),
			}}}
		}
		if err := change.operation.Validate(); err != nil {
			diagnostics = append(diagnostics, standaloneDiagnostic(ydbdiff.SecretKind, change.input, change.ref, err))
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
