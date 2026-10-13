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
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// PlanFeatures plans persistence changes from captured parent state. A change
// retains the table and permits transactional execution. Parent creation and
// removal carry the facet with the table. Rebuilds are refused because this
// service supplies no table reconstruction. Invalid batches or cancellation
// return no executable prefix; inputs remain unchanged.
func (PersistenceService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{PersistenceKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported table persistence parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, err := planPersistence(request, record, i)
		if err != nil {
			return completedPersistence(ctx, persistenceRefusal(err, new(i), nil))
		}
		result.Contributions = append(result.Contributions, contribution)
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: record.Subject, Kind: PersistenceChangeKind,
			Strategy: "change table persistence in place", Steps: []plangraph.StepID{contribution.Steps[0].ID}})
	}
	if len(request.ParentKinds) == 0 {
		return completedPersistence(ctx, result)
	}
	for i, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		if table.Action == "" {
			continue
		}
		strategy, err := persistenceParent(request.Target, table)
		if err != nil {
			return completedPersistence(ctx, persistenceRefusal(err, nil, new(i)))
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: PersistenceKind, Action: table.Action, Strategy: strategy})
	}
	return completedPersistence(ctx, result)
}

func planPersistence(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], error) {
	var result plangraph.Contribution[featureplan.Operation]
	if !SupportsPersistence(request.Target) {
		return result, fmt.Errorf("%w: ALTER TABLE ... SET UNLOGGED cannot be rendered on %s: this target creates no unlogged table", ptaherr.ErrUnsupportedFeature, request.Target)
	}
	change, ok := record.Value.(*PersistenceChange)
	if !ok || !HoldsPersistence(record.Subject) {
		return result, fmt.Errorf("%w: persistence change requires a table subject and matching operands", schemaext.ErrInvalidValue)
	}
	if err := ValidatePersistenceChange(change); err != nil {
		return result, err
	}
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if position < 0 {
		return result, fmt.Errorf("%w: table persistence change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[position]
	if !table.Desired.HasTable() || !table.Current.HasTable() || table.Action != "" && table.Action != featureplan.AlterTable {
		return result, fmt.Errorf("%w: table persistence requires captured declarations and observations of a surviving table", schemaext.ErrInvalidValue)
	}
	desired, _, err := schemaext.FacetAs[*DesiredPersistence](table.Desired.Table.Facets, PersistenceKind)
	if err != nil {
		return result, err
	}
	current, _, err := schemaext.FacetAs[*ObservedPersistence](table.Current.Table.Facets, PersistenceKind)
	if err != nil {
		return result, err
	}
	if persistenceDesiredUnlogged(change.After) != persistenceDesiredUnlogged(desired) || persistenceObservedUnlogged(change.Before) != persistenceObservedUnlogged(current) {
		return result, fmt.Errorf("%w: persistence operands disagree with captured table state", schemaext.ErrInvalidValue)
	}
	if table.Current.Table.Partitioned && persistenceDesiredUnlogged(change.After) {
		return result, fmt.Errorf("%w: PostgreSQL partitioned table %s cannot be UNLOGGED", ptaherr.ErrUnsupportedFeature, table.Subject)
	}
	operation := &AlterPersistence{Change: *change.Copy()}
	persistence := table.Subject
	persistence.Kind = objectidentity.Kind(PersistenceKind)
	result.Owner = Owner
	result.Steps = []plangraph.Step[featureplan.Operation]{{
		ID:          plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("table-persistence/%06d", index)},
		Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: operation},
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: persistence, Action: plangraph.Alter}},
		Transaction: plangraph.TransactionAllowed, Impact: operation.Effect(),
	}}
	return result, nil
}

func persistenceParent(target string, table featureplan.Table) (string, error) {
	if !HoldsPersistence(table.Subject) {
		return "", fmt.Errorf("%w: persistence parent must name a table", schemaext.ErrInvalidValue)
	}
	desired, _, err := PersistenceOf(table.Desired.Table.Facets)
	if err != nil {
		return "", err
	}
	if platform.NormalizeDialect(target) == platform.YDB && desired.Unlogged {
		return "", fmt.Errorf("%w: UNLOGGED is PostgreSQL's; YDB has no unlogged table", ptaherr.ErrUnsupportedFeature)
	}
	if !SupportsPersistence(target) {
		return "the target has no table persistence setting; CREATE TABLE reports omitted declarations", nil
	}
	switch table.Action {
	case featureplan.CreateTable:
		if !table.Desired.HasTable() {
			return "", fmt.Errorf("%w: persistence creation requires a declared table", schemaext.ErrInvalidValue)
		}
		return "CREATE TABLE carries declared persistence", nil
	case featureplan.DropTable:
		if !table.Current.HasTable() {
			return "", fmt.Errorf("%w: persistence removal requires an observed table", schemaext.ErrInvalidValue)
		}
		return "remove persistence with the table", nil
	case featureplan.AlterTable:
		if !table.Current.HasTable() || !table.Desired.HasTable() {
			return "", fmt.Errorf("%w: persistence alteration requires both table captures", schemaext.ErrInvalidValue)
		}
		return "retain table persistence unless a planned persistence change replaces it", nil
	default:
		return "", fmt.Errorf("%w: PostgreSQL table persistence has no reconstruction plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}

func persistenceRefusal(err error, change, parent *int) featureplan.Result {
	kind := PersistenceKind
	if change != nil {
		kind = PersistenceChangeKind
	}
	code := schemavalidation.UnsupportedFeature
	if errors.Is(err, schemaext.ErrInvalidValue) {
		code = schemavalidation.InvalidSchema
	}
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: code, Kind: string(kind), Feature: "PostgreSQL table persistence", Message: err.Error()},
		Change:  change, Parent: parent,
	}}}
}
