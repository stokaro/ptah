package pgtable

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// PlanFeatures plans exclusion changes against captured table state. Replacing
// a definition requires a transaction around its drop and creation. Comments
// retain the constraint. Named declarations are added after the new table;
// a rebuild is refused because this service supplies no reconstruction plan.
// Any refusal discards every contribution in the batch.
func (ExclusionService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := exclusionContext(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{ExclusionKind}) {
		return featureplan.Result{}, exclusionModelError(schemaext.Change, "EXCLUDE planner received other parent kinds")
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, err := planExclusion(request, record, i)
		if err != nil {
			return completedExclusionPlan(ctx, exclusionRefusal(err, new(i), nil))
		}
		steps := make([]plangraph.StepID, 0, len(contribution.Steps))
		for _, step := range contribution.Steps {
			steps = append(steps, step.ID)
		}
		result.Contributions = append(result.Contributions, contribution)
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: record.Subject, Kind: ExclusionChangeKind, Strategy: exclusionStrategy(record.Value.(*ExclusionChange)), Steps: steps})
	}
	if len(request.ParentKinds) > 0 {
		for i, table := range request.Tables {
			if err := ctx.Err(); err != nil {
				return featureplan.Result{}, err
			}
			if table.Action == "" {
				continue
			}
			receipt, contribution, err := planExclusionParent(request, table, i)
			if err != nil {
				return completedExclusionPlan(ctx, exclusionRefusal(err, nil, new(i)))
			}
			result.Parents = append(result.Parents, receipt)
			if len(contribution.Steps) > 0 {
				result.Contributions = append(result.Contributions, contribution)
			}
		}
	}
	return completedExclusionPlan(ctx, result)
}

func planExclusion(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], error) {
	var result plangraph.Contribution[featureplan.Operation]
	if !SupportsExclusion(request.Target) {
		return result, fmt.Errorf("%w: EXCLUDE constraints cannot be altered on %s", ptaherr.ErrUnsupportedFeature, request.Target)
	}
	change, ok := record.Value.(*ExclusionChange)
	if !ok {
		return result, exclusionModelError(schemaext.Change, "EXCLUDE planner requires an exclusion change")
	}
	if err := ValidateExclusionChange(change); err != nil {
		return result, err
	}
	if err := ValidateExclusionRef(record.Subject); err != nil {
		return result, err
	}
	parent := ExclusionTable(record.Subject)
	matches := 0
	var table featureplan.Table
	for _, candidate := range request.Tables {
		if candidate.Subject.Key() == parent.Key() {
			table = candidate
			matches++
		}
	}
	if matches != 1 || !table.Desired.HasTable() || !table.Current.HasTable() || table.Action != "" && table.Action != featureplan.AlterTable {
		return result, exclusionModelError(schemaext.Change, "EXCLUDE change requires one captured surviving table")
	}
	if err := matchesExclusionCapture(record.Subject, change, table); err != nil {
		return result, err
	}
	if change.After != nil && change.After.Deferrable && !request.Capabilities.Has(capability.DeferrableKeys) {
		return result, fmt.Errorf("%w: EXCLUDE constraint %s declares unsupported DEFERRABLE", ptaherr.ErrUnsupportedFeature, record.Subject)
	}
	result.Owner = Owner
	step := func(suffix string, half *ExclusionChange, action plangraph.Action, transaction plangraph.Transaction, phase featureplan.Phase) plangraph.Step[featureplan.Operation] {
		operation := &AlterExclusion{Name: record.Subject.Name.Source, Change: *half.Copy()}
		return plangraph.Step[featureplan.Operation]{
			ID:      plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("exclusion/%06d%s", index, suffix)},
			Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: parent, Payload: operation, Phase: phase},
			Effects: exclusionEffects(record.Subject, parent, action, half), Transaction: transaction, Impact: half.Effect(),
		}
	}
	switch {
	case change.Before == nil:
		result.Steps = append(result.Steps, step("", change, plangraph.Create, plangraph.TransactionAllowed, featureplan.PhaseDefault))
	case change.After == nil:
		// The constraint leaves ahead of the host's column changes. The server
		// rebuilds it under ALTER COLUMN TYPE, and answers a type its index
		// method cannot take with `data type text has no default operator class
		// for access method "gist"`. It also drops the constraint with the last
		// column it covers, so a drop that follows that column answers that the
		// constraint does not exist.
		result.Steps = append(result.Steps, step("", change, plangraph.Drop, plangraph.TransactionAllowed, featureplan.PhaseLifecycle))
	case change.CommentOnly:
		result.Steps = append(result.Steps, step("", change, plangraph.Alter, plangraph.TransactionAllowed, featureplan.PhaseDefault))
	default:
		// The old definition leaves for the same reason, and the new one may read
		// a column the host's column changes add, so they run on either side of
		// those changes.
		drop := step("", &ExclusionChange{Before: change.Before}, plangraph.Drop, plangraph.TransactionRequired, featureplan.PhaseLifecycle)
		create := step("/create", &ExclusionChange{After: change.After}, plangraph.Create, plangraph.TransactionRequired, featureplan.PhaseDefault)
		result.Steps = append(result.Steps, drop, create)
		result.Dependencies = append(result.Dependencies, plangraph.Dependency{Before: drop.ID, After: create.ID})
	}
	return result, nil
}

func matchesExclusionCapture(ref objectidentity.ID, change *ExclusionChange, table featureplan.Table) error {
	for _, side := range []struct {
		objects  schemaext.Objects
		value    schemaext.Value
		coverage schemaext.Coverage
	}{
		{table.Desired.OwnedObjects, exclusionDesiredValue(change.After), table.Desired.FeatureCoverage},
		{table.Current.OwnedObjects, exclusionObservedValue(change.Before), table.Current.FeatureCoverage},
	} {
		captured, found, err := side.objects.Get(ref)
		if err != nil {
			return err
		}
		knowledge := side.coverage.Lookup(ExclusionKind, ref)
		if !found && knowledge.State != schemaext.Complete && knowledge.State != schemaext.Absent || found && limitedExclusion(side.coverage, ref) {
			return exclusionModelError(schemaext.Change, "EXCLUDE capture does not establish the constraint or its absence")
		}

		if found != (side.value != nil) || found && !captured.Value.Equal(side.value) {
			return exclusionModelError(schemaext.Change, "EXCLUDE operands disagree with captured table state")
		}
	}
	return nil
}

func exclusionDesiredValue(value *DesiredExclusion) schemaext.Value {
	if value == nil {
		return nil
	}
	return value
}

func exclusionObservedValue(value *ObservedExclusion) schemaext.Value {
	if value == nil {
		return nil
	}
	return value
}

func exclusionEffects(subject, parent objectidentity.ID, action plangraph.Action, change *ExclusionChange) []plangraph.Effect {
	result := []plangraph.Effect{{Subject: parent, Action: plangraph.Read}, {Subject: subject, Action: action}}
	seen := map[objectidentity.Key]bool{subject.Key(): true, parent.Key(): true}
	var dependencies []objectidentity.ID
	if change.Before != nil {
		dependencies = append(dependencies, change.Before.Dependencies...)
	}
	if change.After != nil {
		dependencies = append(dependencies, change.After.Dependencies...)
	}
	for _, ref := range sortedExclusionDependencies(dependencies) {
		if !seen[ref.Key()] {
			result = append(result, plangraph.Effect{Subject: ref, Action: plangraph.Read})
			seen[ref.Key()] = true
		}
	}
	return result
}

func exclusionStrategy(change *ExclusionChange) string {
	switch {
	case change.CommentOnly:
		return "set the exclusion constraint comment"
	case change.Before == nil:
		return "add the exclusion constraint to its table"
	case change.After == nil:
		return "drop the exclusion constraint from its table"
	default:
		return "drop and recreate the exclusion constraint in one transaction"
	}
}

func exclusionParent(request featureplan.Request, table featureplan.Table) (string, error) {
	if table.Subject.Kind != objectidentity.KindTable || table.Subject.Name.Empty() {
		return "", exclusionModelError(schemaext.Change, "EXCLUDE parent must name a table")
	}
	switch table.Action {
	case featureplan.CreateTable:
		if !table.Desired.HasTable() {
			return "", exclusionModelError(schemaext.Change, "EXCLUDE creation requires a table declaration")
		}
		return exclusionCreationStrategy, nil
	case featureplan.DropTable:
		if !table.Current.HasTable() {
			return "", exclusionModelError(schemaext.Change, "EXCLUDE removal requires a table observation")
		}
		return "remove exclusion constraints with their table", nil
	case featureplan.AlterTable:
		if !table.Desired.HasTable() || !table.Current.HasTable() {
			return "", exclusionModelError(schemaext.Change, "EXCLUDE preservation requires both table captures")
		}
		return "retain exclusion constraints except for their planned changes", nil
	default:
		return "", fmt.Errorf("%w: EXCLUDE has no reconstruction plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}

func exclusionRefusal(err error, change, parent *int) featureplan.Result {
	kind := ExclusionKind
	if change != nil {
		kind = ExclusionChangeKind
	}
	code := schemavalidation.UnsupportedFeature
	if errors.Is(err, schemaext.ErrInvalidValue) {
		code = schemavalidation.InvalidSchema
	}
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: schemavalidation.Diagnostic{Code: code, Kind: string(kind), Feature: "PostgreSQL EXCLUDE constraint", Message: err.Error()},
		Change: change, Parent: parent}}}
}

func completedExclusionPlan(ctx context.Context, result featureplan.Result) (featureplan.Result, error) {
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}
