// Package chplan plans ClickHouse-owned storage changes from captured table
// state and contributes operations to the host's complete dependency graph.
package chplan

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
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

// Service plans TTL changes and checks storage dependencies of common column
// operations. Its zero value supports concurrent use without database access.
// Other storage changes require an explicit supported plan and are refused.
type Service struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host graph before any operation is rendered or executed.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.ClickHouse {
		return featureplan.Result{}, fmt.Errorf("%w: ClickHouse planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{chschema.TableKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported ClickHouse parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, err := planChange(request, record, i)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				return featureplan.Result{}, canceled
			}
			return refusal(chdiff.TableKind, err, new(i), nil), nil
		}
		result.Contributions = append(result.Contributions, contribution)
		result.Changes = append(result.Changes, featureplan.ChangePlan{
			Subject: record.Subject, Kind: chdiff.TableKind, Strategy: "change TTL rules in place after required column additions and before dependent column removals",
			Steps: []plangraph.StepID{contribution.Steps[0].ID},
		})
	}
	for i, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		strategy, err := assessParent(request, table)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				return featureplan.Result{}, canceled
			}
			return refusal(chschema.TableKind, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: chschema.TableKind, Action: table.Action, Strategy: strategy})
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
		Problem: schemavalidation.Diagnostic{Code: code, Kind: string(kind), Feature: "ClickHouse storage planning", Message: err.Error()},
		Change:  change, Parent: parent,
	}}}
}

func planChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], error) {
	var result plangraph.Contribution[featureplan.Operation]
	change, ok := record.Value.(*chdiff.Table)
	if !ok {
		return result, fmt.Errorf("%w: expected ClickHouse table change", schemaext.ErrInvalidValue)
	}
	if err := chsql.ValidateTTLChange(change); err != nil {
		return result, err
	}
	indexOfTable := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if indexOfTable < 0 {
		return result, fmt.Errorf("%w: table change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[indexOfTable]
	if table.Action != "" && table.Action != featureplan.AlterTable {
		return result, fmt.Errorf("%w: a TTL change requires a surviving table outside a rebuild", schemaext.ErrInvalidValue)
	}
	before, after, err := capturedStorage(table)
	if err != nil {
		return result, err
	}
	projected, err := change.After.Observed()
	if err != nil {
		return result, err
	}
	if !chsql.SameTable(before, change.Before) || !chsql.SameTable(after, projected) {
		return result, fmt.Errorf("%w: storage change disagrees with captured parent operands", schemaext.ErrInvalidValue)
	}
	result.Owner = "ptah.run/clickhouse"
	id := plangraph.StepID{Owner: result.Owner, Name: fmt.Sprintf("table-ttl/%d", index)}
	payload := &chast.AlterTTL{Change: *change.CloneChange().(*chdiff.Table)}
	subject := table.Subject
	subject.Kind = objectidentity.Kind(chschema.TableKind)
	step := plangraph.Step[featureplan.Operation]{
		ID: id, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: payload},
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: subject, Action: plangraph.Alter}},
		Transaction: plangraph.TransactionForbidden, Impact: payload.Effect(),
	}
	dependencies, effects, err := ttlDependencies(request, table, before, after, id)
	if err != nil {
		return plangraph.Contribution[featureplan.Operation]{}, err
	}
	step.Effects = append(step.Effects, effects...)
	result.Steps, result.Dependencies = []plangraph.Step[featureplan.Operation]{step}, dependencies
	return result, nil
}

func capturedStorage(table featureplan.Table) (before, after *chschema.ObservedTable, err error) {
	coverage := table.Current.FeatureCoverage
	if !table.Current.HasTable() || coverage.Representation() != schemaext.Observed || coverage.Lookup(chschema.TableKind, table.Subject).State != schemaext.Complete {
		return nil, nil, fmt.Errorf("%w: ClickHouse storage requires complete captured observation coverage", schemaext.ErrInvalidValue)
	}
	before, found, err := schemaext.FacetAs[*chschema.ObservedTable](table.Current.Table.Facets, chschema.TableKind)
	if err != nil || !found {
		return nil, nil, fmt.Errorf("%w: captured ClickHouse storage observation is missing or invalid", schemaext.ErrInvalidValue)
	}
	if err := chschema.ValidateObserved(before); err != nil {
		return nil, nil, err
	}
	if table.Action == featureplan.DropTable {
		return before, nil, nil
	}
	if knowledge, found := table.Desired.FeatureCoverage.SubjectKnowledge(chschema.TableKind, table.Subject); found &&
		(knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable) {
		return nil, nil, fmt.Errorf("%w: captured ClickHouse declaration has a storage knowledge limit: %s", schemaext.ErrInvalidValue, knowledge.Reason)
	}
	desired, found, err := schemaext.FacetAs[*chschema.DesiredTable](table.Desired.Table.Facets, chschema.TableKind)
	if err != nil || !found || !table.Desired.HasTable() {
		return nil, nil, fmt.Errorf("%w: captured ClickHouse storage declaration is missing or invalid", schemaext.ErrInvalidValue)
	}
	after, err = desired.Observed()
	return before, after, err
}
