// Package chplan plans ClickHouse-owned storage changes from captured table
// state and contributes operations to the host's complete dependency graph.
package chplan

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

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

// Service plans changes to the sorting key, sampling key, TTL rules and
// settings of a surviving MergeTree table in place, and checks the storage
// dependencies of common column operations. A change to the engine, PARTITION
// BY or PRIMARY KEY is refused by name: ClickHouse has no ALTER TABLE for them,
// and creating the table again and copying its rows is not planned. When the
// sorting key gains an expression, the operation takes over the table's column
// additions, because ClickHouse adds to the key only columns the same ALTER
// TABLE adds. Its zero value supports concurrent use without database access.
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
		planned, err := planChange(request, record, i)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				return featureplan.Result{}, canceled
			}
			return refusal(chdiff.TableKind, err, new(i), nil), nil
		}
		result.Contributions = append(result.Contributions, planned.contribution)
		result.Rewrites = append(result.Rewrites, planned.rewrites...)
		result.Changes = append(result.Changes, featureplan.ChangePlan{
			Subject: record.Subject, Kind: chdiff.TableKind, Strategy: planned.strategy,
			Steps: []plangraph.StepID{planned.contribution.Steps[0].ID},
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

// plannedChange is one table's storage operation, the common column additions
// it takes over, and the strategy its receipt states.
type plannedChange struct {
	contribution plangraph.Contribution[featureplan.Operation]
	rewrites     []plangraph.Rewrite
	strategy     string
}

func planChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plannedChange, error) {
	change, ok := record.Value.(*chdiff.Table)
	if !ok {
		return plannedChange{}, fmt.Errorf("%w: expected ClickHouse table change", schemaext.ErrInvalidValue)
	}
	if err := chsql.ValidateStorageChange(change); err != nil {
		return plannedChange{}, err
	}
	indexOfTable := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if indexOfTable < 0 {
		return plannedChange{}, fmt.Errorf("%w: table change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[indexOfTable]
	if table.Action != "" && table.Action != featureplan.AlterTable {
		return plannedChange{}, fmt.Errorf("%w: a storage change requires a surviving table outside a rebuild", schemaext.ErrInvalidValue)
	}
	before, after, err := capturedStorage(table)
	if err != nil {
		return plannedChange{}, err
	}
	projected, err := change.After.Observed()
	if err != nil {
		return plannedChange{}, err
	}
	if !chsql.SameTable(before, change.Before) || !chsql.SameTable(after, projected) {
		return plannedChange{}, fmt.Errorf("%w: storage change disagrees with captured parent operands", schemaext.ErrInvalidValue)
	}
	owner := "ptah.run/clickhouse"
	id := plangraph.StepID{Owner: owner, Name: fmt.Sprintf("table-storage/%d", index)}
	additions, err := keyColumnAdditions(request, table, before, after)
	if err != nil {
		return plannedChange{}, err
	}
	payload := &chast.AlterStorage{Change: *change.CloneChange().(*chdiff.Table), Columns: additions.columns}
	if err := payload.Validate(); err != nil {
		return plannedChange{}, err
	}
	subject := table.Subject
	subject.Kind = objectidentity.Kind(chschema.TableKind)
	step := plangraph.Step[featureplan.Operation]{
		ID:          id,
		Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: payload, Notes: additions.notes},
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: subject, Action: plangraph.Alter}},
		Transaction: plangraph.TransactionForbidden, Impact: payload.Effect(),
	}
	step.Effects = withEffects(step.Effects, additions.effects...)
	dependencies, effects, err := storageDependencies(request, table, before, after, id, additions.steps)
	if err != nil {
		return plannedChange{}, err
	}
	step.Effects = withEffects(step.Effects, effects...)
	planned := plannedChange{
		contribution: plangraph.Contribution[featureplan.Operation]{Owner: owner, Steps: []plangraph.Step[featureplan.Operation]{step}, Dependencies: dependencies},
		strategy:     strategy(before, after, additions.columns),
	}
	if len(additions.steps) > 0 {
		planned.rewrites = []plangraph.Rewrite{{Sources: additions.steps, Replacement: id}}
	}
	return planned, nil
}

func strategy(before, after *chschema.ObservedTable, columns []chast.Column) string {
	clauses := chsql.Changed(before, after)
	names := make([]string, len(clauses))
	for i, clause := range clauses {
		names[i] = string(clause)
	}
	result := "change " + strings.Join(names, ", ") + " in place after the column additions the new clauses need and before removals of columns only the old ones use"
	if len(columns) > 0 {
		result += "; the table's new columns are added in the ALTER TABLE that changes the sorting key"
	}
	return result
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
