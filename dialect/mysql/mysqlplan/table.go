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

// TableService accounts for table options through the common table
// operations. It plans no change of its own: CREATE TABLE writes the
// options, a table that survives keeps the ones it holds, and DROP TABLE
// removes them with the table. Its zero value is ready for concurrent use.
type TableService struct{}

// PlanFeatures returns a receipt for every table with a parent action, or a
// completed refusal. Errors describe invalid requests or cancellation.
func (TableService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return featureplan.Result{}, fmt.Errorf("%w: MySQL table options planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{mysqlschema.TableKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported MySQL table options parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	if len(request.Changes) > 0 {
		return refusal(fmt.Errorf("MySQL table options change only when the table is created"), new(0), nil), nil
	}
	for i, table := range request.Tables {
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		strategy, err := tableStrategy(table.Action)
		if err != nil {
			return refusal(err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: mysqlschema.TableKind, Action: table.Action, Strategy: strategy})
	}
	return result, ctx.Err()
}

func tableStrategy(action featureplan.ParentAction) (string, error) {
	switch action {
	case featureplan.CreateTable:
		return "CREATE TABLE writes the declared options", nil
	case featureplan.AlterTable:
		return "the table keeps the options it holds; they change only when the table is created", nil
	case featureplan.DropTable:
		return "remove the options with the table", nil
	case featureplan.RebuildTable:
		return "the rebuilt table is created with the declared options", nil
	default:
		return "", fmt.Errorf("MySQL table options have no plan for parent action %q", action)
	}
}

func refusal(err error, change, parent *int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(mysqlschema.TableKind),
			Feature: "MySQL table options planning", Message: err.Error()},
		Change: change, Parent: parent,
	}}}
}
