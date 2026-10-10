package synonym

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

func checkTarget(ctx context.Context, target, stage string) error {
	if ctx == nil {
		return fmt.Errorf("%w: %s requires a context", schemaext.ErrInvalidValue, stage)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !supported(target) {
		return fmt.Errorf("%w: synonym %s on %q", ptaherr.ErrUnsupportedDialect, stage, target)
	}
	return nil
}

// CompareService compares declared and read synonyms without database access.
// Its zero value is ready for concurrent use.
//
// Synonyms are matched by alias under the target's identifier rules, the
// connection's default schema included. A synonym both sides hold whose target
// differs is retargeted, which is one change rather than a drop and an add,
// so a plan and a reader both see that the two statements belong together.
// Targets are compared without their quoting and without case, see
// [SameTarget]. A synonym the database holds and a source that cannot declare
// one leaves out is adopted into the declaration rather than dropped.
type CompareService struct{}

// CompareObjects returns a complete comparison. Invalid input and
// cancellation return a zero result.
func (CompareService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if err := checkTarget(ctx, request.Target, "comparison"); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{Kind}) || len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: synonym comparison takes only its own kind and no change request", schemaext.ErrInvalidValue)
	}
	desired, err := collect[*DesiredSynonym](request.Identifiers, request.Desired.Objects)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	current, err := collect[*ObservedSynonym](request.Identifiers, request.Current.Objects)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}
	keys := make([]objectidentity.Key, 0, len(desired)+len(current))
	for key := range desired {
		keys = append(keys, key)
	}
	for key := range current {
		if _, found := desired[key]; !found {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, func(a, b objectidentity.Key) int {
		return cmp.Compare(label(desired, current, a), label(desired, current, b))
	})
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		wanted, held := desired[key], current[key]
		declared, observed := wanted.value, held.value
		switch {
		case observed == nil:
			result.Changes = append(result.Changes, record(wanted.ref, nil, declared))
		case declared == nil && !removable(request.Desired.Coverage, held.ref):
			var err error
			if result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: held.ref, Value: observed.Desired()}); err != nil {
				return schemaext.ObjectComparisonResult{}, err
			}
		case declared == nil:
			result.Changes = append(result.Changes, record(held.ref, observed, nil))
		case !SameTarget(declared.Target, observed.Target):
			result.Changes = append(result.Changes, record(wanted.ref, observed, declared))
		}
	}
	return result, ctx.Err()
}

func label(desired map[objectidentity.Key]entry[*DesiredSynonym], current map[objectidentity.Key]entry[*ObservedSynonym], key objectidentity.Key) string {
	if synonym := desired[key].value; synonym != nil {
		return fold(synonym.QualifiedName())
	}
	return fold(current[key].value.QualifiedName())
}

func record(subject objectidentity.ID, before *ObservedSynonym, after *DesiredSynonym) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: subject, Value: (&Change{Before: before, After: after}).Copy()}
}

// removable reports whether the desired source knows the synonym's absence:
// it can declare synonyms, so leaving one out asks for its drop.
func removable(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	switch coverage.Lookup(Kind, ref).State {
	case schemaext.Complete, schemaext.Absent:
		return true
	default:
		return false
	}
}

// entry is one object of a side: its identity and its value.
type entry[V any] struct {
	ref   objectidentity.ID
	value V
}

// collect keys each synonym of a side by its alias under the target's
// identifier rules, so an alias in the connection's default schema written
// with the schema and one written without it are one synonym, as they are on
// the server. Each entry keeps the object's own identity, which a change names.
func collect[V interface {
	schemaext.Value
	comparable
}](semantics identifier.Semantics, objects schemaext.Objects) (map[objectidentity.Key]entry[V], error) {
	builder := objectidentity.NewBuilder(semantics)
	all, err := objects.All()
	if err != nil {
		return nil, err
	}
	values := make(map[objectidentity.Key]entry[V], len(all))
	for _, object := range all {
		if err := ValidateRef(object.Ref); err != nil {
			return nil, err
		}
		value, ok := object.Value.(V)
		var zero V
		if !ok || value == zero {
			return nil, fmt.Errorf("%w: unexpected synonym value %T", schemaext.ErrInvalidValue, object.Value)
		}
		key := builder.TablePartsVerbatim(object.Ref.Schema.Source, object.Ref.Name.Source)
		key.Kind = object.Ref.Kind
		values[key.Key()] = entry[V]{ref: object.Ref, value: value}
	}
	return values, nil
}

// PlanService plans synonym changes and the synonyms a whole-schema render
// declares. Its zero value is ready for concurrent use.
//
// Every statement joins the host's dependent window: after the tables, views
// and routines a target may name are created, and before any of them is
// dropped. Neither engine requires the target to exist, so a statement reads
// nothing else and its owner orders it by the synonym alone.
type PlanService struct{}

// PlanFeatures returns complete receipts. Errors describe invalid requests or
// cancellation.
func (PlanService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := checkTarget(ctx, request.Target, "planning"); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: a synonym has no table parent to assess", schemaext.ErrInvalidValue)
	}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	result := featureplan.Result{Complete: true}
	for index, change := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		value, ok := change.Value.(*Change)
		if !ok {
			return featureplan.Result{}, fmt.Errorf("%w: expected a synonym change, got %T", schemaext.ErrInvalidValue, change.Value)
		}
		if err := ValidateChange(value); err != nil {
			return featureplan.Result{}, err
		}
		step := planStep(index, value, featureplan.PhaseDependent)
		contribution.Steps = append(contribution.Steps, step)
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: ChangeKind,
			Strategy: "create, drop or retarget the synonym once the objects it may name exist, and before any of them is dropped",
			Steps:    []plangraph.StepID{step.ID}})
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// PlanDeclarations creates each declared synonym for a whole-schema render,
// after every common step: a target may be any object the render creates.
func (PlanService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := checkTarget(ctx, request.Target, "planning"); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		declared, ok := object.Value.(*DesiredSynonym)
		if !ok || declared == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired synonym, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		if err := ValidateDesired(declared); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		step := planStep(index, &Change{After: declared}, featureplan.PhaseDefault)
		for _, common := range request.CommonSteps {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: common.ID, After: step.ID})
		}
		contribution.Steps = append(contribution.Steps, step)
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref,
			Strategy: "create the synonym after every object the render creates", Steps: []plangraph.StepID{step.ID}})
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// planStep is the one step a valid change takes.
func planStep(index int, change *Change, phase featureplan.Phase) plangraph.Step[featureplan.Operation] {
	operation := &Operation{Action: Retarget}
	action := plangraph.Alter
	switch {
	case change.Before == nil:
		operation.Action, action = Create, plangraph.Create
		operation.Synonym = *change.After
	case change.After == nil:
		operation.Action, action = Drop, plangraph.Drop
		operation.Synonym = DesiredSynonym{Synonym: change.Before.Synonym}
	default:
		operation.Synonym = *change.After
	}
	return plangraph.Step[featureplan.Operation]{
		// Zero-padded, because a scheduler orders independent steps by name
		// and the statements should keep the order of the changes.
		ID:          plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("synonym/%06d/%s", index, operation.Action)},
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: operation, Phase: phase},
		Effects:     []plangraph.Effect{{Subject: operation.Synonym.Ref(), Action: action}},
		Transaction: plangraph.TransactionAllowed,
		Impact:      operation.Effect(),
	}
}

// ReverseService reconstructs the prior synonym from complete operands. Its
// zero value is ready for concurrent use and never reads a database.
type ReverseService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. An added synonym is dropped, a dropped one created again
// for the target it had, and a retargeted one pointed back at that target.
func (ReverseService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := checkTarget(ctx, request.Target, "reversal"); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, change := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, ok := change.Value.(*Change)
		if !ok {
			return nil, fmt.Errorf("%w: reversal requires a synonym change", schemaext.ErrInvalidValue)
		}
		if err := ValidateChange(value); err != nil {
			return nil, err
		}
		reversed := &Change{}
		var forward []schemaext.ProjectedValue
		if value.After != nil {
			after, err := value.After.Observed()
			if err != nil {
				return nil, err
			}
			reversed.Before = after
			forward = []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: Kind, Value: after.Clone()}}
		}
		if value.Before != nil {
			reversed.After = value.Before.Desired()
		}
		result = append(result, schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: change.Subject, Value: reversed},
			ForwardState: forward, Strategy: "restore the synonym as the read found it"})
	}
	return result, ctx.Err()
}

// ConvertService converts synonyms between their representations.
type ConvertService struct{}

// ConvertFeatures converts an ordered batch without mutating inputs.
func (ConvertService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if err := checkTarget(ctx, request.Target, "conversion"); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		var converted schemaext.Value
		var err error
		switch {
		case request.From == schemaext.Desired && request.To == schemaext.Observed:
			declared, ok := value.(*DesiredSynonym)
			if !ok {
				return nil, fmt.Errorf("%w: expected a desired synonym, got %T", schemaext.ErrInvalidValue, value)
			}
			converted, err = declared.Observed()
		case request.From == schemaext.Observed && request.To == schemaext.Desired:
			observed, ok := value.(*ObservedSynonym)
			if !ok {
				return nil, fmt.Errorf("%w: expected an observed synonym, got %T", schemaext.ErrInvalidValue, value)
			}
			if err = ValidateObserved(observed); err == nil {
				converted = observed.Desired()
			}
		default:
			return nil, fmt.Errorf("%w: invalid synonym conversion direction", schemaext.ErrInvalidValue)
		}
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

// ReportService reports captured synonyms without inspecting a server. Its
// zero value is ready for concurrent use.
type ReportService struct{}

// Definitions returns independent omission labels and count metadata.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: Kind, DisplayName: "synonyms", Metrics: []schemaext.MetricDefinition{
		{Name: "synonyms", Help: "Synonyms"},
	}}}
}

// ReportValues returns one report per value. Errors and cancellation expose
// no partial report.
func (ReportService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var err error
		switch value := value.(type) {
		case *DesiredSynonym:
			err = ValidateDesired(value)
		case *ObservedSynonym:
			err = ValidateObserved(value)
		default:
			err = fmt.Errorf("%w: unexpected synonym value %T", schemaext.ErrInvalidValue, value)
		}
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: Kind, Counts: []schemaext.MetricCount{{Name: "synonyms", Value: 1}}})
	}
	return reports, ctx.Err()
}
