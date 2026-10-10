package sqlitetable

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// VirtualCompareService compares what SQLite tables are: a virtual table
// with its module declaration, or an ordinary table. Its zero value is ready
// for concurrent use.
type VirtualCompareService struct{}

// CompareFacets reports a change for each table on both sides that is a
// virtual table declared differently, or a virtual table on one side and an
// ordinary table on the other. An ordinary desired table counts only where
// the desired coverage describes virtual tables for it: a source with no
// syntax for one does not ask for an ordinary table by leaving it out. A table
// on one side only is created or dropped with its declaration and reports no
// change. Each value is validated. Invalid input and cancellation return a
// zero result.
func (VirtualCompareService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := checkTarget(ctx, request.Target, "virtual table comparison"); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{VirtualKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported SQLite virtual table comparison kinds", schemaext.ErrInvalidValue)
	}
	desired := make(map[objectidentity.Key]*DesiredVirtual, len(request.Desired.Records))
	for _, record := range request.Desired.Records {
		value, found, err := schemaext.FacetAs[*DesiredVirtual](record.Values, VirtualKind)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if found {
			if err := ValidateDesiredVirtual(value); err != nil {
				return schemaext.FacetComparisonResult{}, err
			}
			desired[record.Subject.Key()] = value
		}
	}
	current := make(map[objectidentity.Key]*ObservedVirtual, len(request.Current.Records))
	for _, record := range request.Current.Records {
		value, found, err := schemaext.FacetAs[*ObservedVirtual](record.Values, VirtualKind)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if found {
			if err := ValidateObservedVirtual(value); err != nil {
				return schemaext.FacetComparisonResult{}, err
			}
			current[record.Subject.Key()] = value
		}
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindTable {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: a SQLite virtual table declaration attaches to a table, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		if !owner.Desired || !owner.Current || !request.Includes(VirtualKind, owner.Subject) {
			continue
		}
		declared, held := desired[owner.Subject.Key()], current[owner.Subject.Key()]
		if !virtualTablesDiffer(declared, held, request, owner.Subject) {
			continue
		}
		change := &VirtualChange{After: declared, Before: held}
		result.Changes = append(result.Changes, schemaext.FacetChange{Kind: VirtualKind,
			Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change.Copy()}})
	}
	return result, ctx.Err()
}

// virtualTablesDiffer reports whether a table both sides hold is not the same
// kind of table, or not the same virtual table. A missing declaration says the
// table is ordinary only where that side's coverage describes virtual tables:
// a source with no syntax for one, or a read that did not look, says nothing.
func virtualTablesDiffer(declared *DesiredVirtual, held *ObservedVirtual, request schemaext.FacetComparisonRequest, table objectidentity.ID) bool {
	switch {
	case declared != nil && held != nil:
		return !declared.SameDeclaration(held.Virtual)
	case declared != nil:
		return describesVirtualTables(request.Current.Coverage, table)
	case held != nil:
		return describesVirtualTables(request.Desired.Coverage, table)
	default:
		return false
	}
}

func describesVirtualTables(coverage schemaext.Coverage, table objectidentity.ID) bool {
	state := coverage.Lookup(VirtualKind, table).State
	return state == schemaext.Complete || state == schemaext.Absent
}

// VirtualPlanService accounts for virtual tables through the common table
// operations and refuses every change. CREATE VIRTUAL TABLE writes the
// declaration when a table is created and DROP TABLE removes a virtual table
// with everything its module stores; SQLite has no ALTER VIRTUAL TABLE and no
// statement that turns one kind of table into the other, and Ptah does not
// rebuild a virtual table. Its zero value is ready for concurrent use.
type VirtualPlanService struct{}

// PlanFeatures returns a receipt for every table with a parent action, or a
// completed refusal of a change or of a rebuild. Errors describe invalid
// requests or cancellation.
func (VirtualPlanService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := checkTarget(ctx, request.Target, "virtual table planning"); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{VirtualKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported SQLite virtual table parent kinds", schemaext.ErrInvalidValue)
	}
	if len(request.Changes) > 0 {
		// Every change is refused, so the first one answers for the batch.
		record := request.Changes[0]
		change, ok := record.Value.(*VirtualChange)
		if !ok {
			return featureplan.Result{}, fmt.Errorf("%w: unexpected SQLite virtual table change %T", schemaext.ErrInvalidValue, record.Value)
		}
		if err := ValidateVirtualChange(change); err != nil {
			return featureplan.Result{}, err
		}
		return virtualRefusal(virtualChangeRefusal(record.Subject, change), new(0), nil), nil
	}
	result := featureplan.Result{Complete: true}
	for i, table := range request.Tables {
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		strategy, err := virtualParentStrategy(table.Action)
		if err != nil {
			return virtualRefusal(err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: VirtualKind, Action: table.Action, Strategy: strategy})
	}
	return result, ctx.Err()
}

func virtualChangeRefusal(table objectidentity.ID, change *VirtualChange) error {
	if change.Before != nil && change.After != nil {
		return fmt.Errorf("virtual table %s is declared with another module or other arguments; SQLite has no ALTER VIRTUAL TABLE, "+
			"and dropping and recreating it destroys everything its module stores", table)
	}
	return fmt.Errorf("table %s is a virtual table on one side and an ordinary table on the other; "+
		"SQLite has no statement that turns one kind of table into the other", table)
}

func virtualParentStrategy(action featureplan.ParentAction) (string, error) {
	switch action {
	case featureplan.CreateTable:
		return "CREATE VIRTUAL TABLE writes the module declaration in place of a column list", nil
	case featureplan.AlterTable:
		return "the module declaration stays as it is held; SQLite has no ALTER VIRTUAL TABLE", nil
	case featureplan.DropTable:
		return "DROP TABLE removes the virtual table and everything its module stores", nil
	case featureplan.RebuildTable:
		return "", fmt.Errorf("SQLite virtual tables are not rebuilt: their rows belong to the module, not to a table")
	default:
		return "", fmt.Errorf("SQLite virtual tables have no plan for parent action %q", action)
	}
}

func virtualRefusal(err error, change, parent *int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(VirtualKind),
			Feature: "SQLite virtual table planning", Message: err.Error()},
		Change: change, Parent: parent,
	}}}
}

// VirtualConvertService converts virtual table declarations between their
// representations. Its zero value is ready for concurrent use.
type VirtualConvertService struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It
// accepts only the sqlite target and opposite desired and observed
// representations. Any error, including cancellation, returns no partial
// result.
func (VirtualConvertService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if err := checkTarget(ctx, request.Target, "virtual table conversion"); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertVirtual(value, request.From, request.To)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

func convertVirtual(value schemaext.Value, from, to schemaext.Representation) (schemaext.Value, error) {
	switch {
	case from == schemaext.Desired && to == schemaext.Observed:
		declared, ok := value.(*DesiredVirtual)
		if !ok {
			return nil, fmt.Errorf("%w: expected a desired SQLite virtual table, got %T", schemaext.ErrInvalidValue, value)
		}
		return declared.Observed()
	case from == schemaext.Observed && to == schemaext.Desired:
		observed, ok := value.(*ObservedVirtual)
		if !ok {
			return nil, fmt.Errorf("%w: expected an observed SQLite virtual table, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := ValidateObservedVirtual(observed); err != nil {
			return nil, err
		}
		return observed.Desired(), nil
	default:
		return nil, fmt.Errorf("%w: invalid SQLite virtual table conversion direction", schemaext.ErrInvalidValue)
	}
}

// VirtualReportService reports captured virtual tables. Its zero value is
// ready for concurrent use.
type VirtualReportService struct{}

// VirtualReportDefinitions returns independent omission labels and count
// metadata.
func VirtualReportDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: VirtualKind, DisplayName: "SQLite virtual table declarations", Metrics: []schemaext.MetricDefinition{
		{Name: "sqlite_virtual_tables", Help: "SQLite virtual tables"},
	}}}
}

// ReportValues returns one report per value. Errors and cancellation expose
// no partial report.
func (VirtualReportService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
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
		case *DesiredVirtual:
			err = ValidateDesiredVirtual(value)
		case *ObservedVirtual:
			err = ValidateObservedVirtual(value)
		default:
			err = fmt.Errorf("%w: unexpected SQLite virtual table value %T", schemaext.ErrInvalidValue, value)
		}
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: VirtualKind, Counts: []schemaext.MetricCount{{Name: "sqlite_virtual_tables", Value: 1}}})
	}
	return reports, ctx.Err()
}
