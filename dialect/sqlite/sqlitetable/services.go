package sqlitetable

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

func checkTarget(ctx context.Context, target, stage string) error {
	if ctx == nil {
		return fmt.Errorf("%w: SQLite table options %s requires a context", schemaext.ErrInvalidValue, stage)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if platform.NormalizeDialect(target) != platform.SQLite {
		return fmt.Errorf("%w: SQLite table options %s on %q", ptaherr.ErrUnsupportedDialect, stage, target)
	}
	return nil
}

// PropertyService decodes and encodes table options as the string-valued
// platform properties `strict` and `without_rowid` of the sqlite target, each
// `true` or `false`. It performs no inspection. Its zero value is ready for
// concurrent use.
type PropertyService struct{}

// tableProperties is the one vocabulary that registration, encoding and
// decoding share, so a newly supported key cannot escape ownership checks.
func tableProperties(options *Options) map[string]*bool {
	return map[string]*bool{"strict": &options.Strict, "without_rowid": &options.WithoutRowID}
}

// PropertyDefinitions returns independent property ownership declarations:
// the keys strict and without_rowid. Go annotations prefix the keys with
// platform.sqlite.; YAML places them in the target's platform group.
func PropertyDefinitions() []schemaext.PropertyDefinition {
	var options Options
	return []schemaext.PropertyDefinition{{Kind: TableKind, Keys: slices.Sorted(maps.Keys(tableProperties(&options)))}}
}

// DecodeProperties decodes an ordered batch into DesiredTable values. A value
// is `true` or `false`, and an empty value states nothing, as the option left
// out does. An unknown property and any other value wrap
// schemaext.ErrInvalidValue. Any failure or cancellation returns no partial
// batch.
func (PropertyService) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := checkTarget(ctx, request.Target, "decoding"); err != nil {
		return nil, err
	}
	if request.Format != schemaext.TablePlatformProperties {
		return nil, fmt.Errorf("%w: SQLite table options format %q", ptaherr.ErrUnsupportedFeature, request.Format)
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fragment.Kind != TableKind {
			return nil, fmt.Errorf("%w: SQLite table options kind %q", schemaext.ErrInvalidValue, fragment.Kind)
		}
		table := &DesiredTable{}
		properties := tableProperties(&table.Options)
		for _, key := range slices.Sorted(maps.Keys(fragment.Properties)) {
			target, known := properties[key]
			if !known {
				return nil, fmt.Errorf("%w: unknown SQLite table property %q", schemaext.ErrInvalidValue, key)
			}
			value := fragment.Properties[key]
			if value == "" {
				continue
			}
			on, err := strconv.ParseBool(value)
			if err != nil || (value != "true" && value != "false") {
				return nil, fmt.Errorf("%w: SQLite table property %s is %q, not true or false", schemaext.ErrInvalidValue, key, value)
			}
			*target = on
		}
		result = append(result, table)
	}
	return result, ctx.Err()
}

// EncodeProperties writes each option that is on as its property, `true`,
// and leaves an option that is off unwritten. Unknown model types wrap
// schemaext.ErrInvalidValue. Returned maps do not alias inputs. Any failure
// or cancellation returns no partial batch.
func (PropertyService) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := checkTarget(ctx, request.Target, "encoding"); err != nil {
		return nil, err
	}
	if request.Format != schemaext.TablePlatformProperties {
		return nil, fmt.Errorf("%w: SQLite table options format %q", ptaherr.ErrUnsupportedFeature, request.Format)
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		table, ok := value.(*DesiredTable)
		if !ok || table == nil {
			return nil, fmt.Errorf("%w: SQLite table options expected desired table options, got %T", schemaext.ErrInvalidValue, value)
		}
		fragment := schemaext.PropertyFragment{Kind: TableKind, Properties: make(map[string]string)}
		for key, option := range tableProperties(&table.Options) {
			if *option {
				fragment.Properties[key] = "true"
			}
		}
		result = append(result, fragment)
	}
	return result, ctx.Err()
}

// CompareService compares the options of SQLite tables. Its zero value is
// ready for concurrent use.
type CompareService struct{}

// CompareFacets returns a complete comparison with no change and the
// declarations as they were. SQLite has no statement that changes either
// option of a table that exists, so no plan changes them, as no plan ever
// has. Each value is validated. Invalid input and cancellation return a zero
// result.
func (CompareService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := checkTarget(ctx, request.Target, "comparison"); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{TableKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported SQLite table options comparison kinds", schemaext.ErrInvalidValue)
	}
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindTable {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: SQLite table options attach to a table, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
	}
	for _, record := range request.Desired.Records {
		if _, _, err := schemaext.FacetAs[*DesiredTable](record.Values, TableKind); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	for _, record := range request.Current.Records {
		if _, _, err := schemaext.FacetAs[*ObservedTable](record.Values, TableKind); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	return result, ctx.Err()
}

// PlanService accounts for table options through the common table
// operations. It plans no change of its own: CREATE TABLE writes the options,
// a table that survives keeps the ones it holds, a rebuild creates the new
// table with the declared ones, and DROP TABLE removes them with the table.
// Its zero value is ready for concurrent use.
type PlanService struct{}

// PlanFeatures returns a receipt for every table with a parent action, or a
// completed refusal of a change. Errors describe invalid requests or
// cancellation.
func (PlanService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := checkTarget(ctx, request.Target, "planning"); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{TableKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported SQLite table options parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	if len(request.Changes) > 0 {
		return refusal(fmt.Errorf("SQLite table options change only when their table is created"), new(0), nil), nil
	}
	for i, table := range request.Tables {
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		strategy, err := parentStrategy(table.Action)
		if err != nil {
			return refusal(err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: TableKind, Action: table.Action, Strategy: strategy})
	}
	return result, ctx.Err()
}

func parentStrategy(action featureplan.ParentAction) (string, error) {
	switch action {
	case featureplan.CreateTable:
		return "CREATE TABLE writes the declared SQLite table options", nil
	case featureplan.AlterTable:
		return "the SQLite table options stay as they are held; they change only when the table is created", nil
	case featureplan.DropTable:
		return "remove the SQLite table options with the table", nil
	case featureplan.RebuildTable:
		return "the rebuilt table is created with the declared SQLite table options", nil
	default:
		return "", fmt.Errorf("SQLite table options have no plan for parent action %q", action)
	}
}

func refusal(err error, change, parent *int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(TableKind),
			Feature: "SQLite table options planning", Message: err.Error()},
		Change: change, Parent: parent,
	}}}
}

// ConvertService converts table options between their representations. Its
// zero value is ready for concurrent use.
type ConvertService struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It
// accepts only the sqlite target and opposite desired and observed
// representations. Any error, including cancellation, returns no partial
// result.
func (ConvertService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if err := checkTarget(ctx, request.Target, "conversion"); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var converted schemaext.Value
		var err error
		switch {
		case request.From == schemaext.Desired && request.To == schemaext.Observed:
			declared, ok := value.(*DesiredTable)
			if !ok {
				return nil, fmt.Errorf("%w: expected desired SQLite table options, got %T", schemaext.ErrInvalidValue, value)
			}
			converted, err = declared.Observed()
		case request.From == schemaext.Observed && request.To == schemaext.Desired:
			observed, ok := value.(*ObservedTable)
			if !ok {
				return nil, fmt.Errorf("%w: expected observed SQLite table options, got %T", schemaext.ErrInvalidValue, value)
			}
			if err = ValidateObservedTable(observed); err == nil {
				converted = observed.Desired()
			}
		default:
			return nil, fmt.Errorf("%w: invalid SQLite table options conversion direction", schemaext.ErrInvalidValue)
		}
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

// ReportService reports captured table options. Its zero value is ready for
// concurrent use.
type ReportService struct{}

// ReportDefinitions returns independent omission labels and count metadata.
func ReportDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: TableKind, DisplayName: "SQLite table options", Metrics: []schemaext.MetricDefinition{
		{Name: "sqlite_strict_tables", Help: "SQLite tables created STRICT"},
		{Name: "sqlite_without_rowid_tables", Help: "SQLite tables created WITHOUT ROWID"},
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
		var options Options
		switch value := value.(type) {
		case *DesiredTable:
			if err := ValidateDesiredTable(value); err != nil {
				return nil, err
			}
			options = value.Options
		case *ObservedTable:
			if err := ValidateObservedTable(value); err != nil {
				return nil, err
			}
			options = value.Options
		default:
			return nil, fmt.Errorf("%w: unexpected SQLite table options value %T", schemaext.ErrInvalidValue, value)
		}
		strict, withoutRowID := 0, 0
		if options.Strict {
			strict = 1
		}
		if options.WithoutRowID {
			withoutRowID = 1
		}
		reports = append(reports, schemaext.ValueReport{Kind: TableKind, Counts: []schemaext.MetricCount{
			{Name: "sqlite_strict_tables", Value: strict},
			{Name: "sqlite_without_rowid_tables", Value: withoutRowID},
		}})
	}
	return reports, ctx.Err()
}

// TableOptions returns the options a table's facets declare, or nil for a
// table that declares none. It is what the SQLite renderer writes after the
// column list. A kind this package does not own is refused with
// ptaherr.ErrUnsupportedFeature.
func TableOptions(facets schemaext.Facets) (*DesiredTable, error) {
	for _, kind := range facets.Kinds() {
		if kind != TableKind && kind != VirtualKind {
			return nil, fmt.Errorf("%w: SQLite table facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, _, err := schemaext.FacetAs[*DesiredTable](facets, TableKind)
	if err != nil || value == nil {
		return nil, err
	}
	return value, ValidateDesiredTable(value)
}
