package chplan

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
)

// RefreshService plans changes to the refresh schedules of materialized views.
// A schedule that changes to another is set in place with
// chast.ModifyRefresh, which keeps the view's rows. A view gaining or losing a
// schedule, or APPEND, is replaced by the common plan, which recreates it with
// its declared schedule; the change reports so through
// schemaext.OwnerReplacement, and this service accounts for it through that
// replacement and contributes no step. Its zero value supports concurrent use
// without database access.
type RefreshService struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. A change MODIFY REFRESH cannot make while the common plan keeps the
// view is refused: the server refuses MODIFY REFRESH on a plain view, no ALTER
// removes a schedule, and none adds or removes APPEND. Errors describe invalid requests or cancellation.
func (RefreshService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: refresh planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.ClickHouse {
		return featureplan.Result{}, fmt.Errorf("%w: ClickHouse refresh planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 {
		return featureplan.Result{}, fmt.Errorf("%w: ClickHouse refresh planning takes no parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, plan, err := planRefreshChange(request, record, i)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				return featureplan.Result{}, canceled
			}
			refused := refusal(chdiff.RefreshKind, err, new(i), nil)
			refused.Diagnostics[0].Problem.Feature = "ClickHouse refresh planning"
			return refused, nil
		}
		if len(contribution.Steps) > 0 {
			result.Contributions = append(result.Contributions, contribution)
		}
		result.Changes = append(result.Changes, plan)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func planRefreshChange(request featureplan.Request, record schemaext.ChangeRecord, position int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var contribution plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: chdiff.RefreshKind}
	change, ok := record.Value.(*chdiff.Refresh)
	if !ok || record.Subject.Kind != objectidentity.KindMatView || record.Subject.Name.Empty() {
		return contribution, plan, fmt.Errorf("%w: expected a ClickHouse change on a materialized view", schemaext.ErrInvalidValue)
	}
	if err := chdiff.ValidateRefresh(change); err != nil {
		return contribution, plan, err
	}
	replaced, err := commonReplacement(request, record.Subject)
	if err != nil {
		return contribution, plan, err
	}
	if replaced {
		plan.Strategy = "the materialized view replacement recreates the view with its declared refresh schedule; the rows it held are discarded"
		return contribution, plan, nil
	}
	if change.ReplacesOwner() {
		return contribution, plan, fmt.Errorf("%w: materialized view %s gains or loses a refresh schedule or its APPEND, which only replacing the view does, and the plan keeps the view",
			ptaherr.ErrUnsupportedFeature, record.Subject)
	}
	contribution.Owner = "ptah.run/clickhouse"
	operation := &chast.ModifyRefresh{Schedule: change.After.Schedule}
	id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("refresh/%d", position)}
	contribution.Steps = []plangraph.Step[featureplan.Operation]{{
		ID: id, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: record.Subject, Payload: operation},
		Effects:     []plangraph.Effect{{Subject: record.Subject, Action: plangraph.Alter}},
		Transaction: plangraph.TransactionForbidden, Impact: operation.Effect(),
	}}
	plan.Strategy = "change the refresh schedule in place; the view keeps its rows"
	plan.Steps = []plangraph.StepID{id}
	return contribution, plan, nil
}
