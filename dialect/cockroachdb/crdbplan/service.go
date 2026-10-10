// Package crdbplan plans CockroachDB row-level TTL changes from captured
// operands and contributes owned operations to the host's plan.
package crdbplan

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
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/internal/ttlsql"
)

// Service plans row-level TTL changes in place and accounts for the policy
// through table removal. Its zero value supports concurrent use without
// database access. A rebuilt table has no plan that preserves its policy and
// is refused.
type Service struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host's plan before any operation is rendered or executed.
//
// The host places the operations after the columns a TTL expression may refer
// to exist and before any column is dropped: the expression can name a column
// added in the same plan, and the column the current expression names cannot be
// dropped while the policy still refers to it.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.CockroachDB {
		return featureplan.Result{}, fmt.Errorf("%w: CockroachDB planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{crdbschema.RowTTLKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported CockroachDB parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, err := planChange(request, record, i)
		if err != nil {
			return refusal(crdbdiff.RowTTLKind, err, new(i), nil), nil
		}
		result.Contributions = append(result.Contributions, contribution)
		result.Changes = append(result.Changes, featureplan.ChangePlan{
			Subject: record.Subject, Kind: crdbdiff.RowTTLKind,
			Strategy: "set and reset row-level TTL storage parameters in place after column additions and before column removals",
			Steps:    []plangraph.StepID{contribution.Steps[0].ID},
		})
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
		strategy, err := assessParent(table)
		if err != nil {
			return refusal(crdbschema.RowTTLKind, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: crdbschema.RowTTLKind, Action: table.Action, Strategy: strategy})
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func refusal(kind schemaext.Kind, err error, change, parent *int) featureplan.Result {
	code := schemavalidation.UnsupportedFeature
	if errors.Is(err, schemaext.ErrInvalidValue) {
		code = schemavalidation.InvalidSchema
	}
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: code, Kind: string(kind), Feature: "CockroachDB row-level TTL planning", Message: err.Error()},
		Change:  change, Parent: parent,
	}}}
}

func planChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], error) {
	var result plangraph.Contribution[featureplan.Operation]
	change, ok := record.Value.(*crdbdiff.RowTTL)
	if !ok {
		return result, fmt.Errorf("%w: expected a CockroachDB row-level TTL change", schemaext.ErrInvalidValue)
	}
	if err := ttlsql.ValidateChange(change); err != nil {
		return result, err
	}
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if position < 0 {
		return result, fmt.Errorf("%w: a row-level TTL change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[position]
	if table.Action != "" && table.Action != featureplan.AlterTable {
		return result, fmt.Errorf("%w: a row-level TTL change requires a table that survives the plan in place", schemaext.ErrInvalidValue)
	}
	result.Owner = crdbschema.Owner
	policy := table.Subject
	policy.Kind = objectidentity.Kind(crdbschema.RowTTLKind)
	payload := &crdbast.AlterRowTTL{Change: *change.CloneChange().(*crdbdiff.RowTTL)}
	result.Steps = []plangraph.Step[featureplan.Operation]{{
		// Zero-padded, because a scheduler orders independent steps by name and
		// the statements should keep the order of the changes.
		ID: plangraph.StepID{Owner: result.Owner, Name: fmt.Sprintf("row-ttl/%06d", index)},
		Payload: featureplan.Operation{
			Role: ast.AlterExtension, Parent: table.Subject, Payload: payload,
			Notes: []string{"Row-level TTL on table: " + displayName(table.Subject)},
		},
		Effects: []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: policy, Action: plangraph.Alter}},
		Impact:  payload.Effect(),
	}}
	return result, nil
}

// assessParent accounts for the policy through a table operation. Dropping a
// table removes its policy with it. A table that survives keeps its policy
// unless a change in the same plan replaces it. A rebuild has no plan here.
func assessParent(table featureplan.Table) (string, error) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove the row-level TTL with the table", nil
	case featureplan.AlterTable:
		return "retain the row-level TTL unless a planned change in this plan replaces it", nil
	default:
		return "", fmt.Errorf("%w: CockroachDB row-level TTL has no plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}

func displayName(subject objectidentity.ID) string {
	if subject.Schema.Empty() || subject.Schema.Defaulted {
		return subject.Name.Source
	}
	return subject.Schema.Source + "." + subject.Name.Source
}
