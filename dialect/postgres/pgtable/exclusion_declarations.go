package pgtable

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

const exclusionCreationStrategy = "add each named exclusion constraint after its table exists"

// PlanDeclarations creates every named exclusion as its own ALTER TABLE step.
// PostgreSQL folds equivalent inline CREATE TABLE exclusions, but a desired
// schema declares separate identities that must all exist. Common operations
// run first so functions used by expressions and predicates are available.
// Missing or duplicate parents and invalid objects refuse the complete batch.
// The service performs no I/O and never mutates its inputs.
func (ExclusionService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := exclusionContext(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	builder := objectidentity.NewBuilder(request.Identifiers)
	for i, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		parent := ExclusionTable(object.Ref)
		matches := 0
		for _, table := range request.Tables {
			if table.HasTable() && builder.TableParts(table.Table.Schema, table.Table.Name).Key() == parent.Key() {
				matches++
			}
		}
		var step plangraph.Step[featureplan.Operation]
		var err error
		if matches != 1 {
			err = exclusionModelError(schemaext.Desired, "EXCLUDE declaration requires one declared parent table")
		} else {
			step, err = createdExclusionStep(request.Target, request.Capabilities, parent, object, fmt.Sprintf("exclusion-declared/%06d", i), featureplan.PhaseDefault)
		}
		if err != nil {
			return exclusionDeclarationRefusal(err, i), ctx.Err()
		}
		contribution.Steps = append(contribution.Steps, step)
		for _, common := range request.CommonSteps {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: common.ID, After: step.ID})
		}
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref, Strategy: exclusionCreationStrategy, Steps: []plangraph.StepID{step.ID}})
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

func planExclusionParent(request featureplan.Request, table featureplan.Table, index int) (featureplan.ParentPlan, plangraph.Contribution[featureplan.Operation], error) {
	receipt := featureplan.ParentPlan{Subject: table.Subject, Kind: ExclusionKind, Action: table.Action}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	strategy, err := exclusionParent(request, table)
	if err != nil {
		return receipt, contribution, err
	}
	receipt.Strategy = strategy
	if table.Action != featureplan.CreateTable {
		return receipt, contribution, nil
	}
	parent := objectidentity.NewBuilder(request.Identifiers).TableParts(table.Desired.Table.Schema, table.Desired.Table.Name)
	if parent.Key() != table.Subject.Key() || table.Current.HasTable() {
		return receipt, contribution, exclusionModelError(schemaext.Desired, "EXCLUDE creation requires the declared new parent table")
	}
	objects, err := table.Desired.OwnedObjects.All()
	if err != nil {
		return receipt, contribution, err
	}
	for i, object := range objects {
		if object.Ref.Kind != objectidentity.Kind(ExclusionKind) {
			continue
		}
		step, err := createdExclusionStep(request.Target, request.Capabilities, parent, object, fmt.Sprintf("exclusion-created/%06d/%06d", index, i), featureplan.PhaseDependent)
		if err != nil {
			return receipt, contribution, err
		}
		contribution.Steps = append(contribution.Steps, step)
		receipt.Steps = append(receipt.Steps, step.ID)
	}
	return receipt, contribution, nil
}

func createdExclusionStep(target string, caps capability.Capabilities, parent objectidentity.ID, object schemaext.Object, name string, phase featureplan.Phase) (plangraph.Step[featureplan.Operation], error) {
	if platform.NormalizeDialect(target) != platform.Postgres {
		return plangraph.Step[featureplan.Operation]{}, fmt.Errorf("%w: EXCLUDE constraints require PostgreSQL", ptaherr.ErrUnsupportedFeature)
	}
	if err := ValidateExclusionRef(object.Ref); err != nil {
		return plangraph.Step[featureplan.Operation]{}, err
	}
	if ExclusionTable(object.Ref).Key() != parent.Key() {
		return plangraph.Step[featureplan.Operation]{}, exclusionModelError(schemaext.Desired, "EXCLUDE declaration names another parent table")
	}
	desired, ok := object.Value.(*DesiredExclusion)
	if !ok {
		return plangraph.Step[featureplan.Operation]{}, exclusionModelError(schemaext.Desired, "EXCLUDE creation requires a declaration")
	}
	if err := ValidateDesiredExclusion(desired); err != nil {
		return plangraph.Step[featureplan.Operation]{}, err
	}
	if desired.Deferrable && !caps.Has(capability.DeferrableKeys) {
		return plangraph.Step[featureplan.Operation]{}, fmt.Errorf("%w: EXCLUDE constraint %s declares unsupported DEFERRABLE", ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	change := ExclusionChange{After: copyDesiredExclusion(desired)}
	return plangraph.Step[featureplan.Operation]{
		ID:      plangraph.StepID{Owner: Owner, Name: name},
		Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: parent, Payload: &AlterExclusion{Name: object.Ref.Name.Source, Change: change}, Phase: phase},
		Effects: exclusionEffects(object.Ref, parent, plangraph.Create, &change), Transaction: plangraph.TransactionAllowed, Impact: change.Effect(),
	}, nil
}

func exclusionDeclarationRefusal(err error, index int) featureplan.DeclarationResult {
	refusal := exclusionRefusal(err, nil, nil).Diagnostics[0].Problem
	refusal.Kind = string(ExclusionKind)
	return featureplan.DeclarationResult{Complete: true, Diagnostics: []featureplan.DeclarationDiagnostic{{Object: new(index), Problem: refusal}}}
}
