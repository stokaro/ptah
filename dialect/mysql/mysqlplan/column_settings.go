package mysqlplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/mysql/mysqlschema"
)

// ColumnService accounts for the column settings of
// [mysqlschema.ColumnSettingsKind] through the common table operations. It
// plans no operation of its own: a column definition writes the declared
// settings, a column that survives keeps the ones it holds unless the common
// plan rewrites it, and DROP TABLE removes them with the table. Its zero
// value is ready for concurrent use.
type ColumnService struct{}

// PlanFeatures returns a receipt for every table with a parent action, or a
// completed refusal of a change, which no comparison of these settings
// reports. Errors describe invalid requests or cancellation.
func (ColumnService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if !slices.Contains(mysqlschema.Targets(), request.Target) {
		return featureplan.Result{}, fmt.Errorf("%w: MySQL column settings planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{mysqlschema.ColumnSettingsKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported MySQL column settings parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	if len(request.Changes) > 0 {
		return columnRefusal(fmt.Errorf("MySQL column settings change only with their column's definition"), new(0), nil), nil
	}
	for i, table := range request.Tables {
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		strategy, err := columnStrategy(table.Action)
		if err != nil {
			return columnRefusal(err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: mysqlschema.ColumnSettingsKind, Action: table.Action, Strategy: strategy})
	}
	return result, ctx.Err()
}

func columnStrategy(action featureplan.ParentAction) (string, error) {
	switch action {
	case featureplan.CreateTable:
		return "each column definition writes its declared settings", nil
	case featureplan.AlterTable:
		return "a column keeps the settings it holds; a column the plan rewrites is written with its declared settings", nil
	case featureplan.DropTable:
		return "remove the settings with the table", nil
	case featureplan.RebuildTable:
		return "the rebuilt table's column definitions write the declared settings", nil
	default:
		return "", fmt.Errorf("MySQL column settings have no plan for parent action %q", action)
	}
}

func columnRefusal(err error, change, parent *int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(mysqlschema.ColumnSettingsKind),
			Feature: "MySQL column settings planning", Message: err.Error()},
		Change: change, Parent: parent,
	}}}
}
