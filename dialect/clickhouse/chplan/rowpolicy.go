package chplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

// RowPolicyService plans changes to ClickHouse row policies whose table the
// plan keeps. Every change is one statement: CREATE ROW POLICY, DROP ROW
// POLICY, or ALTER ROW POLICY, which sets the filter, the composition and the
// users of an existing policy in place, so no change is a drop followed by a
// create and a policy is never absent between two statements. Its zero value
// supports concurrent use without database access.
//
// A policy names its table, so each step reads the table; the step carries
// the change's access assessment for safety reports. Statements run outside a
// transaction, because ClickHouse DDL is not transactional. As the parent
// planner of the row policy model it accounts for every table the plan
// alters, rebuilds or drops; see planRowPolicyParent.
type RowPolicyService struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation.
func (RowPolicyService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: row policy planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.ClickHouse {
		return featureplan.Result{}, fmt.Errorf("%w: ClickHouse row policy planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{chschema.RowPolicyKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: ClickHouse row policy planning assesses only the row policy model", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/clickhouse"}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		step, plan, err := planRowPolicyChange(record, fmt.Sprintf("row-policy/%06d", i))
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				return featureplan.Result{}, canceled
			}
			return rowPolicyRefusal(err, new(i), nil), nil
		}
		contribution.Steps = append(contribution.Steps, step)
		result.Changes = append(result.Changes, plan)
	}
	if len(request.ParentKinds) > 0 {
		for i, table := range request.Tables {
			if table.Action == "" {
				continue
			}
			parent, steps, err := planRowPolicyParent(i, table)
			if err != nil {
				if canceled := ctx.Err(); canceled != nil {
					return featureplan.Result{}, canceled
				}
				return rowPolicyRefusal(err, nil, new(i)), nil
			}
			contribution.Steps = append(contribution.Steps, steps...)
			result.Parents = append(result.Parents, parent)
		}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func rowPolicyRefusal(err error, change, parent *int) featureplan.Result {
	refused := refusal(chdiff.RowPolicyKind, err, change, parent)
	refused.Diagnostics[0].Problem.Feature = "ClickHouse row policy planning"
	if parent != nil {
		refused.Diagnostics[0].Problem.Kind = string(chschema.RowPolicyKind)
	}
	return refused
}

// planRowPolicyParent accounts for a table's row policies through an
// operation on the table.
//
// ClickHouse keeps a row policy when its table is dropped, measured on 24.10
// and 26.9: the policy stays in system.row_policies and applies again to a
// table created under the name later. So a dropped table's captured policies
// are dropped by statements of their own, and a table that is rebuilt or
// altered keeps them without any.
func planRowPolicyParent(index int, table featureplan.Table) (featureplan.ParentPlan, []plangraph.Step[featureplan.Operation], error) {
	parent := featureplan.ParentPlan{Subject: table.Subject, Kind: chschema.RowPolicyKind, Action: table.Action}
	switch table.Action {
	case featureplan.AlterTable:
		parent.Strategy = "keep the table's row policies; ClickHouse resolves a policy's table by name"
		return parent, nil, nil
	case featureplan.RebuildTable:
		parent.Strategy = "keep the table's row policies; ClickHouse keeps a policy while its table is dropped and created again"
		return parent, nil, nil
	case featureplan.DropTable:
	default:
		return parent, nil, fmt.Errorf("%w: ClickHouse row policies have no plan through parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
	objects, err := table.Current.OwnedObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(chschema.RowPolicyKind)
	}).All()
	if err != nil {
		return parent, nil, err
	}
	if len(objects) == 0 {
		parent.Strategy = "no row policy of the table is captured; ClickHouse keeps any it holds when the table is dropped"
		return parent, nil, nil
	}
	steps := make([]plangraph.Step[featureplan.Operation], 0, len(objects))
	for i, object := range objects {
		observed, ok := object.Value.(*chschema.ObservedRowPolicy)
		if !ok {
			return parent, nil, fmt.Errorf("%w: a dropped table's captured row policy is %T, not an observation", schemaext.ErrInvalidValue, object.Value)
		}
		record := schemaext.ChangeRecord{Subject: object.Ref, Value: chdiff.NewRowPolicy(observed, nil)}
		step, _, err := planRowPolicyChange(record, fmt.Sprintf("row-policy/parent/%06d/%06d", index, i))
		if err != nil {
			return parent, nil, err
		}
		steps = append(steps, step)
		parent.Steps = append(parent.Steps, step.ID)
	}
	parent.Strategy = "drop the table's row policies, which ClickHouse keeps when the table is dropped"
	return parent, steps, nil
}

func planRowPolicyChange(record schemaext.ChangeRecord, name string) (plangraph.Step[featureplan.Operation], featureplan.ChangePlan, error) {
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: chdiff.RowPolicyKind}
	cloned, err := record.Clone()
	if err != nil {
		return plangraph.Step[featureplan.Operation]{}, plan, err
	}
	change, ok := cloned.Value.(*chdiff.RowPolicy)
	if !ok {
		return plangraph.Step[featureplan.Operation]{}, plan, fmt.Errorf("%w: expected a ClickHouse row policy change, got %T", schemaext.ErrInvalidValue, cloned.Value)
	}
	if err := chschema.ValidateRowPolicyRef(record.Subject); err != nil {
		return plangraph.Step[featureplan.Operation]{}, plan, err
	}
	operation := &chast.RowPolicy{Database: chschema.RowPolicyDatabase(record.Subject), Table: record.Subject.Parent.Source,
		Name: record.Subject.Name.Source, Change: *change}
	if err := operation.Validate(); err != nil {
		return plangraph.Step[featureplan.Operation]{}, plan, err
	}
	action, strategy := plangraph.Alter, "change the row policy in place with ALTER ROW POLICY; it is never absent between statements"
	switch {
	case change.Before == nil:
		action, strategy = plangraph.Create, "create the row policy with CREATE ROW POLICY"
	case change.After == nil:
		action, strategy = plangraph.Drop, "drop the row policy with DROP ROW POLICY; its table keeps its rows"
	}
	id := plangraph.StepID{Owner: "ptah.run/clickhouse", Name: name + "/" + string(action)}
	step := plangraph.Step[featureplan.Operation]{
		ID:      id,
		Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: operation},
		Effects: []plangraph.Effect{
			{Subject: chschema.RowPolicyTable(record.Subject), Action: plangraph.Read},
			{Subject: record.Subject, Action: action},
		},
		Transaction: plangraph.TransactionForbidden, Impact: operation.Effect(),
	}
	plan.Strategy, plan.Steps = strategy, []plangraph.StepID{id}
	return step, plan, nil
}
