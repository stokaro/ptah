// Package tsplan plans TimescaleDB changes and authored continuous aggregates
// into operations for the host's dependency graph. It refuses what TimescaleDB
// has no statement for, orders aggregates after the hypertables and relations
// they read and before the relations their removal frees, and performs no I/O.
package tsplan

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/timescaledb/tsast"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
)

// Owner is the contribution owner of every TimescaleDB step.
const Owner = tsschema.Owner

// Service plans hypertable and continuous-aggregate changes. Its zero value is
// ready for concurrent use.
//
// A table that becomes a hypertable gets the `create_hypertable` call after the
// common steps that create or change it. A hypertable that stops being one, or
// one whose partitioning changes, is refused: measured on 2.29.2,
// `drop_hypertable` answers `function drop_hypertable(unknown) does not exist`,
// and changing a dimension is not a statement either. Planning nothing would be
// worse than refusing -- the table stays partitioned, the description says it
// is not, and the next diff reports the same change forever.
//
// A continuous aggregate's body is not parsed for the relations it reads. A
// creation depends on every hypertable this batch partitions; the host places
// it after the relations it creates or changes, and a removal before the
// tables it drops. An aggregate whose name the host also creates is refused: a
// continuous aggregate holds its name as a relation.
type Service struct{}

type planned struct {
	input int
	step  plangraph.Step[featureplan.Operation]
	plan  featureplan.ChangePlan
}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. A successful reply must join the host graph before any operation is
// rendered or executed.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{tsschema.HypertableKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: TimescaleDB planning assesses only the hypertable model", schemaext.ErrInvalidValue)
	}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	var steps []planned
	var diagnostics []featureplan.Diagnostic
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		step, diagnostic, err := planChange(request, index, record)
		if err != nil {
			return featureplan.Result{}, err
		}
		if diagnostic != nil {
			diagnostics = append(diagnostics, *diagnostic)
			continue
		}
		steps = append(steps, step)
	}
	if len(diagnostics) > 0 {
		return featureplan.Result{Complete: true, Diagnostics: diagnostics}, nil
	}
	hypertables := hypertableSteps(steps)
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for _, step := range steps {
		contribution.Steps = append(contribution.Steps, step.step)
		result.Changes[step.input] = step.plan
		edges, diagnostic := dependencies(request.Identifiers, request.CommonSteps, step, hypertables)
		if diagnostic != nil {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{*diagnostic}}, nil
		}
		contribution.Dependencies = append(contribution.Dependencies, edges...)
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	if len(request.ParentKinds) != 0 {
		for i, table := range request.Tables {
			if table.Action == "" {
				continue
			}
			strategy, refused := assessParent(i, table)
			if refused != nil {
				return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{*refused}}, nil
			}
			result.Parents = append(result.Parents, featureplan.ParentPlan{
				Subject: table.Subject, Kind: tsschema.HypertableKind, Action: table.Action, Strategy: strategy,
			})
		}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

// assessParent accounts for a hypertable through an operation on its table.
// DROP TABLE removes a hypertable and its chunks with the table, so a removal
// needs no statement of its own. A table that survives keeps its hypertable
// unless a change in the same plan says otherwise, which is refused there. A
// created table is partitioned by the call that follows its CREATE TABLE. A
// rebuild would recreate the table as an ordinary one, and has no plan here.
func assessParent(index int, table featureplan.Table) (string, *featureplan.Diagnostic) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove any hypertable and its chunks with the table", nil
	case featureplan.AlterTable:
		return "keep any hypertable unless a planned change in this plan replaces it", nil
	case featureplan.CreateTable:
		return "partition the table with the create_hypertable call that follows its CREATE TABLE", nil
	default:
		return "", &featureplan.Diagnostic{Parent: new(index), Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: string(tsschema.HypertableKind), Object: tsschema.QualifiedName(table.Subject),
			Feature: "TimescaleDB", Message: fmt.Sprintf("TimescaleDB has no plan for a hypertable through parent action %q", table.Action),
		}}
	}
}

// PlanDeclarations derives CREATE operations from authored aggregates. Each
// creation follows the common steps before the first view the render creates,
// and precedes that view: an aggregate reads hypertables, which reach the
// script with their tables, and a view may read an aggregate.
func (Service) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		declared, ok := object.Value.(*tsschema.DesiredContinuousAggregate)
		if !ok || declared == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired continuous aggregate", schemaext.ErrInvalidValue)
		}
		record := schemaext.ChangeRecord{Subject: object.Ref, Value: &tsdiff.ContinuousAggregate{After: declared}}
		step, diagnostic, err := planChange(featureplan.Request{Target: request.Target, Identifiers: request.Identifiers}, index, record)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		if diagnostic == nil {
			var edges []plangraph.Dependency
			edges, diagnostic = dependencies(request.Identifiers, request.CommonSteps, step, nil)
			contribution.Dependencies = append(contribution.Dependencies, declarationOrder(request.CommonSteps, step.step.ID)...)
			contribution.Dependencies = append(contribution.Dependencies, edges...)
		}
		if diagnostic != nil {
			diagnostic.Problem.Kind = string(tsschema.ContinuousAggregateKind)
			result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Problem: diagnostic.Problem, Object: new(index)})
			continue
		}
		contribution.Steps = append(contribution.Steps, step.step)
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref,
			Strategy: "create the continuous aggregate WITH NO DATA after the relations it may read", Steps: []plangraph.StepID{step.step.ID}})
	}
	if len(result.Diagnostics) > 0 {
		return featureplan.DeclarationResult{Complete: true, Diagnostics: result.Diagnostics}, nil
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

func validateRequest(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !platform.IsPostgresFamily(target) {
		return fmt.Errorf("%w: TimescaleDB planning on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

func planChange(request featureplan.Request, index int, record schemaext.ChangeRecord) (planned, *featureplan.Diagnostic, error) {
	cloned, err := record.Clone()
	if err != nil {
		return planned{}, nil, err
	}
	switch change := cloned.Value.(type) {
	case *tsdiff.Hypertable:
		return planHypertable(request, index, cloned.Subject, change)
	case *tsdiff.ContinuousAggregate:
		return planAggregate(index, cloned.Subject, change)
	default:
		return planned{}, nil, fmt.Errorf("%w: unexpected TimescaleDB change %T", schemaext.ErrInvalidValue, cloned.Value)
	}
}

func planHypertable(request featureplan.Request, index int, subject objectidentity.ID, change *tsdiff.Hypertable) (planned, *featureplan.Diagnostic, error) {
	if err := change.Validate(); err != nil {
		return planned{}, nil, err
	}
	if subject.Kind != objectidentity.KindTable {
		return planned{}, nil, fmt.Errorf("%w: a hypertable change names its table", schemaext.ErrInvalidValue)
	}
	table := tableName(request, subject)
	switch {
	case change.After == nil:
		return planned{}, refusal(index, tsdiff.HypertableKind, table,
			"%s is a hypertable and the desired schema does not declare one; TimescaleDB has no statement that turns a "+
				"hypertable back into an ordinary table, so this needs an explicit migration that drops and recreates it",
			table,
		), nil
	case change.Before != nil && !strings.EqualFold(strings.TrimSpace(change.Before.Column), strings.TrimSpace(change.After.Column)):
		return planned{}, refusal(index, tsdiff.HypertableKind, table,
			"hypertable %s is partitioned on %q and the desired schema declares %q; TimescaleDB has no statement that "+
				"repartitions an existing hypertable, so this needs an explicit migration",
			table, change.Before.Column, change.After.Column,
		), nil
	case change.Before != nil:
		return planned{}, refusal(index, tsdiff.HypertableKind, table,
			"hypertable %s has chunk interval %q and the desired schema declares %q; Ptah does not plan a change to an "+
				"existing hypertable's partitioning, so this needs an explicit migration",
			table, change.Before.ChunkInterval, change.After.ChunkInterval,
		), nil
	}
	payload := &tsast.CreateHypertable{Table: table, Hypertable: *change.After}
	id := plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("hypertable/%06d", index)}
	step := plangraph.Step[featureplan.Operation]{ID: id,
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: payload},
		Effects:     []plangraph.Effect{{Subject: subject, Action: plangraph.Read}, {Subject: tsschema.HypertableSubject(subject), Action: plangraph.Create}},
		Transaction: plangraph.TransactionAllowed, Impact: payload.Effect(),
	}
	return planned{input: index, step: step, plan: featureplan.ChangePlan{Subject: subject, Kind: tsdiff.HypertableKind,
		Strategy: "partition the existing table with create_hypertable after the steps that create or change it", Steps: []plangraph.StepID{id}}}, nil, nil
}

func planAggregate(index int, subject objectidentity.ID, change *tsdiff.ContinuousAggregate) (planned, *featureplan.Diagnostic, error) {
	if err := change.Validate(); err != nil {
		return planned{}, nil, err
	}
	if err := tsschema.ValidateContinuousAggregateRef(subject); err != nil {
		return planned{}, nil, err
	}
	payload := &tsast.ContinuousAggregate{Schema: tsschema.AuthoredSchema(subject), Name: subject.Name.Source, Change: *change}
	action, strategy := plangraph.Alter, "replace the continuous aggregate: drop it, then create it WITH NO DATA"
	switch {
	case change.Before == nil:
		action, strategy = plangraph.Create, "create the continuous aggregate WITH NO DATA after the relations it may read"
	case change.After == nil:
		action, strategy = plangraph.Drop, "drop the continuous aggregate with DROP MATERIALIZED VIEW before the relations it may read"
	}
	id := plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("continuous-aggregate/%06d/%s", index, action)}
	step := plangraph.Step[featureplan.Operation]{ID: id,
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: payload},
		Effects:     []plangraph.Effect{{Subject: subject, Action: action}},
		Transaction: plangraph.TransactionAllowed, Impact: payload.Effect(),
	}
	return planned{input: index, step: step, plan: featureplan.ChangePlan{Subject: subject, Kind: tsdiff.ContinuousAggregateKind,
		Strategy: strategy, Steps: []plangraph.StepID{id}}}, nil, nil
}

func hypertableSteps(steps []planned) []plangraph.StepID {
	var result []plangraph.StepID
	for _, step := range steps {
		if _, ok := step.step.Payload.Payload.(*tsast.CreateHypertable); ok {
			result = append(result, step.step.ID)
		}
	}
	return result
}

// declarationOrder places a declared aggregate in a whole-schema render: after
// every common step before the first that creates a view or materialized view,
// and before that one. An aggregate reads hypertables and other aggregates,
// which the render creates with their tables, and a view may read an
// aggregate. A render that creates no view puts the aggregate last.
func declarationOrder(common []featureplan.CommonStep, id plangraph.StepID) []plangraph.Dependency {
	edges := make([]plangraph.Dependency, 0, len(common))
	for _, other := range common {
		if createsView(other) {
			return append(edges, plangraph.Dependency{Before: id, After: other.ID})
		}
		edges = append(edges, plangraph.Dependency{Before: other.ID, After: id})
	}
	return edges
}

// createsView reports a common step that creates a view or a materialized view.
func createsView(step featureplan.CommonStep) bool {
	return slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool {
		return effect.Action == plangraph.Create && (effect.Subject.Kind == objectidentity.KindView || effect.Subject.Kind == objectidentity.KindMatView)
	})
}

// dependencies orders one step against the host's common steps. It names only
// what it can identify exactly -- the creation of the table a hypertable
// partitions and of its columns -- and leaves the rest of each family where the
// host places feature operations, so a step here never contradicts the host's
// own order. A relation the host creates under an aggregate's name is refused.
func dependencies(semantics identifier.Semantics, common []featureplan.CommonStep, step planned, hypertables []plangraph.StepID) ([]plangraph.Dependency, *featureplan.Diagnostic) {
	id := step.step.ID
	var edges []plangraph.Dependency
	switch payload := step.step.Payload.Payload.(type) {
	case *tsast.CreateHypertable:
		table := step.plan.Subject
		for _, other := range common {
			if creates(other, table) {
				edges = append(edges, plangraph.Dependency{Before: other.ID, After: id})
			}
		}
	case *tsast.ContinuousAggregate:
		subject := tsschema.ContinuousAggregateRefWith(semantics, tsschema.AuthoredSchema(step.plan.Subject), step.plan.Subject.Name.Source)
		for _, other := range common {
			if occupies(other, subject) {
				return nil, refusal(step.input, tsdiff.ContinuousAggregateKind, step.plan.Subject.String(),
					"a declared relation takes the name %s, which a TimescaleDB continuous aggregate holds as a relation, "+
						"so the plan would create a relation the name already belongs to; declare it as a continuous aggregate "+
						"instead, rename the relation, or drop the aggregate with DROP MATERIALIZED VIEW in a migration of its own",
					tsschema.QualifiedName(step.plan.Subject),
				)
			}
		}
		if payload.Change.After != nil {
			for _, hypertable := range hypertables {
				edges = append(edges, plangraph.Dependency{Before: hypertable, After: id})
			}
		}
	}
	return edges, nil
}

// relationKinds are the common families that share PostgreSQL's relation
// namespace with a continuous aggregate.
var relationKinds = []objectidentity.Kind{objectidentity.KindTable, objectidentity.KindView, objectidentity.KindMatView}

// creates reports a common step that creates the table or one of its columns.
func creates(step featureplan.CommonStep, table objectidentity.ID) bool {
	return slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool {
		if effect.Action != plangraph.Create {
			return false
		}
		if effect.Subject.Key() == table.Key() {
			return true
		}
		return effect.Subject.Kind == objectidentity.KindColumn && effect.Subject.Catalog.Normalized == table.Catalog.Normalized &&
			effect.Subject.Schema.Normalized == table.Schema.Normalized && effect.Subject.Parent.Normalized == table.Name.Normalized
	})
}

// occupies reports a common creation of a relation under the aggregate's
// schema and name, compared by normalized components.
func occupies(step featureplan.CommonStep, aggregate objectidentity.ID) bool {
	return slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool {
		return slices.Contains(relationKinds, effect.Subject.Kind) && effect.Action == plangraph.Create &&
			effect.Subject.Catalog.Normalized == aggregate.Catalog.Normalized &&
			effect.Subject.Schema.Normalized == aggregate.Schema.Normalized && effect.Subject.Name.Normalized == aggregate.Name.Normalized
	})
}

// tableName is the table as the declaration spells it: schema-qualified when
// the declaration wrote the schema. A captured declaration is preferred over
// the identity, because it keeps the author's spelling.
func tableName(request featureplan.Request, subject objectidentity.ID) string {
	for _, table := range request.Tables {
		if table.Subject.Key() == subject.Key() && table.Desired.HasTable() {
			if table.Desired.Table.Schema == "" {
				return table.Desired.Table.Name
			}
			return table.Desired.Table.Schema + "." + table.Desired.Table.Name
		}
	}
	return tsschema.QualifiedName(subject)
}

func refusal(index int, kind schemaext.Kind, object, format string, args ...any) *featureplan.Diagnostic {
	return &featureplan.Diagnostic{Change: new(index), Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.UnsupportedFeature, Kind: string(kind), Object: object, Feature: "TimescaleDB", Message: fmt.Sprintf(format, args...),
	}}
}
