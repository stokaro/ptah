package pgtable

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
)

// PlanFeatures replaces a surviving foreign key when its delete selection
// changes. Common constraint replacements already carry the declared selection
// and need no second operation. Each replacement requires a transaction so
// dropping the key, restoring it and restoring its comment are atomic. Missing
// or conflicting captured state refuses the complete batch; no server is read.
func (DeleteColumnsService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := deleteColumnsContext(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{DeleteColumnsKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unexpected ON DELETE parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, plan, err := planDeleteColumns(request, record, i)
		if err != nil {
			return completedPersistence(ctx, deleteColumnsRefusal(err, new(i), nil))
		}
		if len(contribution.Steps) > 0 {
			result.Contributions = append(result.Contributions, contribution)
		}
		result.Changes = append(result.Changes, plan)
	}
	for i, table := range request.Tables {
		if len(request.ParentKinds) == 0 || table.Action == "" {
			continue
		}
		strategy, err := deleteColumnsParent(table.Action)
		if err != nil {
			return completedPersistence(ctx, deleteColumnsRefusal(err, nil, new(i)))
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: DeleteColumnsKind, Action: table.Action, Strategy: strategy})
	}
	return completedPersistence(ctx, result)
}
func deleteColumnsParent(action featureplan.ParentAction) (string, error) {
	switch action {
	case featureplan.CreateTable, featureplan.RebuildTable:
		return "the common foreign-key definitions carry their declared ON DELETE column selections", nil
	case featureplan.DropTable:
		return "remove ON DELETE column selections with their foreign keys", nil
	case featureplan.AlterTable:
		return "retain selections unless the common constraint replacement or a selection change writes their declarations", nil
	default:
		return "", fmt.Errorf("%w: unsupported parent action %q for ON DELETE column selections", schemaext.ErrInvalidValue, action)
	}
}
func planDeleteColumns(request featureplan.Request, record schemaext.ChangeRecord, position int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var contribution plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: DeleteColumnsChangeKind}
	change, ok := record.Value.(*DeleteColumnsChange)
	if !ok || !HoldsDeleteColumns(record.Subject) {
		return contribution, plan, fmt.Errorf("%w: ON DELETE planning requires a constraint selection change", schemaext.ErrInvalidValue)
	}
	if err := ValidateDeleteColumnsChange(change); err != nil {
		return contribution, plan, err
	}
	table, definition, err := captureDeleteColumns(request, record.Subject, change)
	if err != nil {
		return contribution, plan, err
	}
	if err := ValidateDeleteSelection(request.Target, request.Capabilities, definition.Columns, definition.OnDelete, definition.Facets); err != nil {
		return contribution, plan, err
	}
	replaced, err := commonReplacesDeleteKey(request, record.Subject)
	if err != nil {
		return contribution, plan, err
	}
	if replaced {
		plan.Strategy = "the common plan replaces the foreign key with its declared ON DELETE column selection"
		return contribution, plan, nil
	}
	operation, err := deleteColumnsOperation(definition)
	if err != nil {
		return contribution, plan, err
	}
	if !request.Capabilities.Has(capability.ForeignKeyDeleteColumnList) {
		return contribution, plan, fmt.Errorf("%w: ON DELETE column-list replacement requires server support", schemaext.ErrInvalidValue)
	}
	id := plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("foreign-key-delete-columns/%06d", position)}
	contribution.Owner = Owner
	contribution.Steps = []plangraph.Step[featureplan.Operation]{{ID: id,
		Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: operation},
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: record.Subject, Action: plangraph.Alter}},
		Transaction: plangraph.TransactionRequired, Impact: operation.Effect()}}
	plan.Steps = []plangraph.StepID{id}
	plan.Strategy = "replace the foreign key and restore its comment in one transaction"
	return contribution, plan, nil
}
func captureDeleteColumns(request featureplan.Request, subject objectidentity.ID, change *DeleteColumnsChange) (featureplan.Table, schemamodel.Constraint, error) {
	for _, table := range request.Tables {
		if subject.Catalog.Normalized != table.Subject.Catalog.Normalized || subject.Schema.Normalized != table.Subject.Schema.Normalized || subject.Parent.Normalized != table.Subject.Name.Normalized {
			continue
		}
		if !deleteColumnsTableSurvives(table) {
			break
		}
		desired := slices.IndexFunc(table.Desired.Constraints, func(value schemamodel.Constraint) bool {
			ref, ok := schemacapture.ConstraintSubject(table.Subject, value.Type, value.Name, request.Identifiers)
			return ok && ref.Key() == subject.Key()
		})
		current := -1
		for i, value := range table.Current.Constraints {
			ref, ok := schemacapture.ConstraintSubject(table.Subject, value.Type, value.Name, request.Identifiers)
			if ok && ref.Key() == subject.Key() {
				current = i
				break
			}
		}
		if desired < 0 || current < 0 {
			break
		}
		declared, observed := table.Desired.Constraints[desired], table.Current.Constraints[current]
		if declared.Type != "FOREIGN KEY" || observed.Type != "FOREIGN KEY" {
			break
		}
		after, _, err := schemaext.FacetAs[*DesiredDeleteColumns](declared.Facets, DeleteColumnsKind)
		if err != nil {
			return featureplan.Table{}, schemamodel.Constraint{}, err
		}
		before, _, err := schemaext.FacetAs[*ObservedDeleteColumns](observed.Facets, DeleteColumnsKind)
		if err != nil {
			return featureplan.Table{}, schemamodel.Constraint{}, err
		}
		if !after.Equal(change.After) || !before.Equal(change.Before) {
			return featureplan.Table{}, schemamodel.Constraint{}, fmt.Errorf("%w: ON DELETE change disagrees with captured constraint selection", schemaext.ErrInvalidValue)
		}
		return table, declared, nil
	}
	return featureplan.Table{}, schemamodel.Constraint{}, fmt.Errorf("%w: ON DELETE change requires both captured definitions of a surviving foreign key", schemaext.ErrInvalidValue)
}
func deleteColumnsOperation(definition schemamodel.Constraint) (*ReplaceDeleteColumns, error) {
	columns, _, err := DeleteColumnsOf(definition.Facets)
	if err != nil {
		return nil, err
	}
	foreign := slices.Clone(definition.ForeignColumns)
	if len(foreign) == 0 && definition.ForeignColumn != "" {
		foreign = []string{definition.ForeignColumn}
	}
	operation := &ReplaceDeleteColumns{Name: definition.Name, Columns: slices.Clone(definition.Columns), ForeignTable: definition.ForeignTable, ForeignColumns: foreign,
		OnDelete: definition.OnDelete, OnUpdate: definition.OnUpdate, DeleteColumns: columns, Match: definition.Match, Deferrable: definition.Deferrable,
		Initially: definition.Initially, NotEnforced: definition.NotEnforced, NotValid: definition.NotValid, Comment: definition.Comment}
	if err := ValidateReplaceDeleteColumns(operation); err != nil {
		return nil, err
	}
	return operation, nil
}

// A comment or validation step alters the constraint without replacing its
// definition. Only a matched drop and creation carry the selection already.
func commonReplacesDeleteKey(request featureplan.Request, subject objectidentity.ID) (bool, error) {
	var dropped, created bool
	for _, step := range request.CommonSteps {
		for _, effect := range step.Effects {
			if effect.Subject.Key() != subject.Key() {
				continue
			}
			dropped = dropped || effect.Action == plangraph.Drop
			created = created || effect.Action == plangraph.Create
		}
	}
	if dropped != created {
		return false, fmt.Errorf("%w: common plan creates or removes a foreign key whose selection changes", schemaext.ErrInvalidValue)
	}
	return dropped, nil
}
func deleteColumnsRefusal(err error, change, parent *int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.UnsupportedFeature, Kind: string(DeleteColumnsKind), Feature: "PostgreSQL foreign-key ON DELETE column list", Message: err.Error()}, Change: change, Parent: parent}}}
}

func deleteColumnsTableSurvives(table featureplan.Table) bool {
	return table.Desired.HasTable() && table.Current.HasTable() && (table.Action == "" || table.Action == featureplan.AlterTable)
}
