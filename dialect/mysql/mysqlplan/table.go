package mysqlplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/mysql/mysqlschema"
)

// IndexService accounts for index options through the common table
// operations, as [TableService] does for table options: CREATE TABLE and ADD
// INDEX write them, an index that survives keeps them, and DROP TABLE and
// DROP INDEX remove them. Its zero value is ready for concurrent use.
type IndexService struct{}

// PlanFeatures returns a receipt for every table with a parent action, or a
// completed refusal, as [TableService.PlanFeatures] does.
func (IndexService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return optionsPlan(ctx, request, mysqlschema.IndexKind, "MySQL index options")
}

// TableService accounts for table options through the common table
// operations. It plans no change of its own: CREATE TABLE writes the
// options, a table that survives keeps the ones it holds, and DROP TABLE
// removes them with the table. Its zero value is ready for concurrent use.
type TableService struct{}

// PlanFeatures returns a receipt for every table with a parent action, or a
// completed refusal. Errors describe invalid requests or cancellation.
func (TableService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return optionsPlan(ctx, request, mysqlschema.TableKind, "MySQL table options")
}

// optionsPlan accounts for options of kind that a statement writes only when
// it creates their owner: it plans no change and gives every table with a
// parent action a receipt.
func optionsPlan(ctx context.Context, request featureplan.Request, kind schemaext.Kind, name string) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return featureplan.Result{}, fmt.Errorf("%w: %s planning on %q", ptaherr.ErrUnsupportedDialect, name, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{kind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported %s parent kinds", schemaext.ErrInvalidValue, name)
	}
	result := featureplan.Result{Complete: true}
	if len(request.Changes) > 0 {
		return refusal(kind, name, fmt.Errorf("%s change only when their owner is created", name), new(0), nil), nil
	}
	for i, table := range request.Tables {
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		strategy, err := tableStrategy(table.Action, name)
		if err != nil {
			return refusal(kind, name, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: kind, Action: table.Action, Strategy: strategy})
	}
	return result, ctx.Err()
}

func tableStrategy(action featureplan.ParentAction, name string) (string, error) {
	switch action {
	case featureplan.CreateTable:
		return "CREATE TABLE writes the declared " + name, nil
	case featureplan.AlterTable:
		return "the " + name + " stay as they are held; they change only when their owner is created", nil
	case featureplan.DropTable:
		return "remove the " + name + " with the table", nil
	case featureplan.RebuildTable:
		return "the rebuilt table is created with the declared " + name, nil
	default:
		return "", fmt.Errorf("%s have no plan for parent action %q", name, action)
	}
}

func refusal(kind schemaext.Kind, name string, err error, change, parent *int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(kind),
			Feature: name + " planning", Message: err.Error()},
		Change: change, Parent: parent,
	}}}
}
