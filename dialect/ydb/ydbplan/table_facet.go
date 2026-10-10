package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// tableFacetPlanning is what the planner of one YDB table facet supplies to
// the frame they share: its name, as an error names it, its facet and change
// kinds, how it plans one change, and how it accounts for the facet through a
// table operation.
type tableFacetPlanning struct {
	name          string
	facet, change schemaext.Kind
	plan          func(featureplan.Request, schemaext.ChangeRecord, int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error)
	assess        func(capability.Capabilities, featureplan.Table) (string, error)
}

// planFeatures returns complete receipts or a completed refusal with no
// usable prefix. Errors describe invalid requests or cancellation.
func (p tableFacetPlanning) planFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.YDB {
		return featureplan.Result{}, fmt.Errorf("%w: %s planning on %q", ptaherr.ErrUnsupportedDialect, p.name, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{p.facet}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported %s parent kinds", schemaext.ErrInvalidValue, p.name)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, plan, err := p.plan(request, record, i)
		if err != nil {
			return p.refusal(p.change, err, new(i), nil), nil
		}
		if len(contribution.Steps) > 0 {
			result.Contributions = append(result.Contributions, contribution)
		}
		result.Changes = append(result.Changes, plan)
	}
	if len(request.ParentKinds) == 0 {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		return result, nil
	}
	for i, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		strategy, err := p.assess(request.Capabilities, table)
		if err != nil {
			return p.refusal(p.facet, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: p.facet, Action: table.Action, Strategy: strategy})
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func (p tableFacetPlanning) refusal(kind schemaext.Kind, err error, change, parent *int) featureplan.Result {
	refusal := ttlRefusal(kind, err, change, parent)
	refusal.Diagnostics[0].Problem.Feature = p.name + " planning"
	return refusal
}

// surviving finds the table a change of a table facet names among the
// request's tables, and refuses a change of a table the plan does not keep in
// place: one it drops, creates or rebuilds carries the facet with the table.
func surviving(request featureplan.Request, record schemaext.ChangeRecord, what string) (featureplan.Table, error) {
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if position < 0 {
		return featureplan.Table{}, fmt.Errorf("%w: a %s change requires captured parent state", schemaext.ErrInvalidValue, what)
	}
	table := request.Tables[position]
	if table.Action != "" && table.Action != featureplan.AlterTable {
		return featureplan.Table{}, fmt.Errorf("%w: a %s change requires a table that survives the plan in place", schemaext.ErrInvalidValue, what)
	}
	return table, nil
}
