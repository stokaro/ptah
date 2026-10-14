// Package privilegeplan plans default ACL adjustments as typed operations in
// the host's plan graph. It neither renders SQL nor reads a database.
package privilegeplan

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// Service plans captured changes and declarations. Its zero value is ready for
// concurrent use. Migrations adjust default ACLs after common creations and
// before removals; dependency edges retain the required roles and schema.
type Service struct{}

// PlanFeatures returns one receipt per change, preserving operation order.
// An invalid input or canceled batch returns no usable prefix.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: default privileges belong to no table", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgdefaultprivilege.Owner}
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		change, ok := record.Value.(*pgdefaultprivilege.Change)
		if !ok {
			return featureplan.Result{}, fmt.Errorf("%w: unexpected default privilege change %T", schemaext.ErrInvalidValue, record.Value)
		}
		if err := change.Validate(); err != nil {
			return featureplan.Result{}, err
		}
		receipt, err := plan(fmt.Sprintf("change/%06d", index), record.Subject, change, request.Identifiers, featureplan.PhaseDependent, request.CommonSteps, &contribution)
		if err != nil {
			return featureplan.Result{}, err
		}
		result.Changes = append(result.Changes, receipt)
	}
	if len(contribution.Steps) != 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

func validateRequest(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: default privilege planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !platform.IsPostgresFamily(target) {
		return fmt.Errorf("%w: default privilege planning on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

func plan(name string, subject objectidentity.ID, change *pgdefaultprivilege.Change, semantics identifier.Semantics, phase featureplan.Phase,
	common []featureplan.CommonStep, contribution *plangraph.Contribution[featureplan.Operation]) (featureplan.ChangePlan, error) {
	identity, err := pgdefaultprivilege.IdentityOf(subject)
	if err != nil {
		return featureplan.ChangePlan{}, err
	}
	ref, err := pgdefaultprivilege.Ref(identity, semantics)
	if err != nil {
		return featureplan.ChangePlan{}, err
	}
	if ref != subject {
		return featureplan.ChangePlan{}, fmt.Errorf("%w: default privilege identity differs from target semantics", schemaext.ErrInvalidValue)
	}
	receipt := featureplan.ChangePlan{Subject: subject, Kind: pgdefaultprivilege.ChangeKind, Strategy: "adjust default privileges after their roles and schema exist"}
	for index, operation := range operations(identity, change) {
		if err := operation.Validate(); err != nil {
			return featureplan.ChangePlan{}, err
		}
		step := plangraph.Step[featureplan.Operation]{ID: plangraph.StepID{Owner: pgdefaultprivilege.Owner, Name: fmt.Sprintf("%s/%06d", name, index)},
			Payload: featureplan.Operation{Role: ast.StatementExtension, Phase: phase, Payload: operation},
			Effects: []plangraph.Effect{{Subject: subject, Action: plangraph.Alter}}, Transaction: plangraph.TransactionAllowed, Impact: operation.Effect()}
		step.Effects = append(step.Effects, reads(identity, semantics)...)
		if index != 0 {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: receipt.Steps[index-1], After: step.ID})
		}
		addDependencies(step, common, contribution)
		receipt.Steps = append(receipt.Steps, step.ID)
		contribution.Steps = append(contribution.Steps, step)
	}
	return receipt, nil
}
