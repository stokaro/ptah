package mssqlproperty

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
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
	if platform.NormalizeDialect(target) != platform.SQLServer {
		return fmt.Errorf("%w: SQL Server extended property %s on %q", ptaherr.ErrUnsupportedDialect, stage, target)
	}
	return nil
}

// CompareService compares declared and read extended properties without
// database access. Its zero value is ready for concurrent use.
//
// A property both sides hold whose value differs changes in place, since SQL
// Server updates a value with sp_updateextendedproperty and dropping and
// adding it would take it away for the length of the script. A property the
// database holds under a value type Ptah cannot write back is left exactly as
// it is, in both directions: the read records it in coverage rather than as a
// value, see [UnrepresentableValue]. A property the database holds and a source that
// cannot declare one leaves out is adopted into the declaration rather than
// dropped.
type CompareService struct{}

// CompareObjects returns a complete comparison. Invalid input and
// cancellation return a zero result.
func (CompareService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if err := checkTarget(ctx, request.Target, "comparison"); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{Kind}) || len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: extended property comparison takes only its own kind and no change request", schemaext.ErrInvalidValue)
	}
	desired, err := collect[*DesiredProperty](request.Desired.Objects)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	current, err := collect[*ObservedProperty](request.Current.Objects)
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
		case observed == nil && unreadable(request.Current.Coverage, wanted.ref):
			// Nobody read the value, so "differs" is not a fact this comparison
			// has, and an add would fail on the property the database holds.
		case observed == nil:
			result.Changes = append(result.Changes, record(wanted.ref, nil, declared))
		case declared == nil && !removable(request.Desired.Coverage, held.ref):
			var err error
			if result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: held.ref, Value: observed.Desired()}); err != nil {
				return schemaext.ObjectComparisonResult{}, err
			}
		case declared == nil:
			result.Changes = append(result.Changes, record(held.ref, observed, nil))
		case declared.Value != observed.Value:
			result.Changes = append(result.Changes, record(wanted.ref, observed, declared))
		}
	}
	return result, ctx.Err()
}

func label(desired map[objectidentity.Key]entry[*DesiredProperty], current map[objectidentity.Key]entry[*ObservedProperty], key objectidentity.Key) string {
	if property := desired[key].value; property != nil {
		return fold(property.Label())
	}
	return fold(current[key].value.Label())
}

func record(subject objectidentity.ID, before *ObservedProperty, after *DesiredProperty) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: subject, Value: (&Change{Before: before, After: after}).Copy()}
}

// unreadable reports whether the read found the property and could not
// describe its value.
func unreadable(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(Kind, ref)
	return found && knowledge.State == schemaext.Unrepresentable
}

// removable reports whether the desired source knows the property's absence:
// it can declare extended properties, so leaving one out asks for its drop.
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

func collect[V interface {
	schemaext.Value
	comparable
}](objects schemaext.Objects) (map[objectidentity.Key]entry[V], error) {
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
			return nil, fmt.Errorf("%w: unexpected extended property value %T", schemaext.ErrInvalidValue, object.Value)
		}
		values[object.Ref.Key()] = entry[V]{ref: object.Ref, value: value}
	}
	return values, nil
}

// PlanService plans extended property changes and the properties a
// whole-schema render declares. Its zero value is ready for concurrent use.
//
// A property is metadata nothing common reads, on an object of any family,
// so each statement joins the host's dependent window: after the tables and
// columns it names exist, and before any of them is dropped, which takes its
// properties with it. Each statement reads the table or the column the
// property is on, which orders it within that window. sp_addextendedproperty
// resolves its owner when it runs (`Cannot find the object ...`), and
// sp_dropextendedproperty after the owner is gone answers `Property cannot be
// dropped`.
type PlanService struct{}

// PlanFeatures returns complete receipts. Errors describe invalid requests or
// cancellation.
func (PlanService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := checkTarget(ctx, request.Target, "planning"); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: an extended property has no table parent to assess", schemaext.ErrInvalidValue)
	}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	result := featureplan.Result{Complete: true}
	for index, change := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		value, ok := change.Value.(*Change)
		if !ok {
			return featureplan.Result{}, fmt.Errorf("%w: expected an extended property change, got %T", schemaext.ErrInvalidValue, change.Value)
		}
		if err := ValidateChange(value); err != nil {
			return featureplan.Result{}, err
		}
		step := planStep(request.Identifiers, index, value, featureplan.PhaseDependent)
		contribution.Steps = append(contribution.Steps, step)
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: ChangeKind,
			Strategy: "run the extended property procedure once the object it is on exists, and before that object is dropped",
			Steps:    []plangraph.StepID{step.ID}})
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// PlanDeclarations adds each declared property for a whole-schema render,
// after every common step: a property names a table or a column, and nothing
// the render creates reads a property.
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
		declared, ok := object.Value.(*DesiredProperty)
		if !ok || declared == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired extended property, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		if err := ValidateDesired(declared); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		step := planStep(request.Identifiers, index, &Change{After: declared}, featureplan.PhaseDefault)
		for _, common := range request.CommonSteps {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: common.ID, After: step.ID})
		}
		contribution.Steps = append(contribution.Steps, step)
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref,
			Strategy: "add the extended property after the object it is on", Steps: []plangraph.StepID{step.ID}})
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}

// planStep is the one statement a valid change takes.
func planStep(semantics identifier.Semantics, index int, change *Change, phase featureplan.Phase) plangraph.Step[featureplan.Operation] {
	operation := &Operation{Action: Update}
	action := plangraph.Alter
	switch {
	case change.Before == nil:
		operation.Action, action = Add, plangraph.Create
		operation.Property = *change.After
	case change.After == nil:
		operation.Action, action = Drop, plangraph.Drop
		operation.Property = DesiredProperty{Property: change.Before.Property}
	default:
		operation.Property = *change.After
	}
	property := operation.Property.Property
	effects := []plangraph.Effect{{Subject: property.Ref(), Action: action}}
	if owner, found := ownerRef(semantics, property); found {
		effects = append(effects, plangraph.Effect{Subject: owner, Action: plangraph.Read})
	}
	return plangraph.Step[featureplan.Operation]{
		// Zero-padded, because a scheduler orders independent steps by name
		// and the statements should keep the order of the changes.
		ID:          plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("extended-property/%06d/%s", index, operation.Action)},
		Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: operation, Phase: phase},
		Effects:     effects,
		Transaction: plangraph.TransactionAllowed,
		Impact:      operation.Effect(),
	}
}

// ownerRef is the table or the column a property is on, as the host's common
// statements name it. A schema or database property names neither.
func ownerRef(semantics identifier.Semantics, property Property) (objectidentity.ID, bool) {
	builder := objectidentity.NewBuilder(semantics)
	switch {
	case property.Column != "":
		return builder.ColumnParts(property.Schema, property.Table, property.Column), true
	case property.Table != "":
		return builder.TableParts(property.Schema, property.Table), true
	default:
		return objectidentity.ID{}, false
	}
}

// ReverseService reconstructs the prior property from complete operands. Its
// zero value is ready for concurrent use and never reads a database.
type ReverseService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. An added property is dropped, a dropped one added back
// with the value it held, and a changed one set back to that value.
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
			return nil, fmt.Errorf("%w: reversal requires an extended property change", schemaext.ErrInvalidValue)
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
			reversed.After = &DesiredProperty{Property: value.Before.Property}
		}
		result = append(result, schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: change.Subject, Value: reversed},
			ForwardState: forward, Strategy: "restore the extended property as the read found it"})
	}
	return result, ctx.Err()
}

// ConvertService converts extended properties between their representations.
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
			declared, ok := value.(*DesiredProperty)
			if !ok {
				return nil, fmt.Errorf("%w: expected a desired extended property, got %T", schemaext.ErrInvalidValue, value)
			}
			converted, err = declared.Observed()
		case request.From == schemaext.Observed && request.To == schemaext.Desired:
			observed, ok := value.(*ObservedProperty)
			if !ok {
				return nil, fmt.Errorf("%w: expected an observed extended property, got %T", schemaext.ErrInvalidValue, value)
			}
			if err = ValidateObserved(observed); err == nil {
				converted = observed.Desired()
			}
		default:
			return nil, fmt.Errorf("%w: invalid extended property conversion direction", schemaext.ErrInvalidValue)
		}
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

// ReportService reports captured extended properties without inspecting a
// server. Its zero value is ready for concurrent use.
type ReportService struct{}

// Definitions returns independent omission labels and count metadata.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: Kind, DisplayName: "extended properties", Metrics: []schemaext.MetricDefinition{
		{Name: "extended_properties", Help: "SQL Server extended properties"},
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
		case *DesiredProperty:
			err = ValidateDesired(value)
		case *ObservedProperty:
			err = ValidateObserved(value)
		default:
			err = fmt.Errorf("%w: unexpected extended property value %T", schemaext.ErrInvalidValue, value)
		}
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: Kind, Counts: []schemaext.MetricCount{{Name: "extended_properties", Value: 1}}})
	}
	return reports, ctx.Err()
}
