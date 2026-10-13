package pgtable

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// PlanFeatures accounts for partition keys on parent table actions. Creation
// and removal carry the key with the table; alteration retains it. A rebuild
// of a partitioned table is refused because it needs a partition-tree strategy.
// Errors and cancellation discard all parent receipts.
func (PartitionService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.Changes) > 0 || !slices.Equal(request.ParentKinds, []schemaext.Kind{PartitionKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: partition planning requires only parent actions", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		if table.Action == "" {
			continue
		}
		strategy, err := partitionParent(request.Target, table)
		if err != nil {
			return featureplan.Result{}, err
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: PartitionKind, Action: table.Action, Strategy: strategy})
	}
	return completedPersistence(ctx, result)
}

func partitionParent(target string, table featureplan.Table) (string, error) {
	if !HoldsPersistence(table.Subject) {
		return "", fmt.Errorf("%w: partition parent must name a table", schemaext.ErrInvalidValue)
	}
	desired, _, err := schemaext.FacetAs[*DesiredPartition](table.Desired.Table.Facets, PartitionKind)
	if err != nil {
		return "", err
	}
	current, _, err := schemaext.FacetAs[*ObservedPartition](table.Current.Table.Facets, PartitionKind)
	if err != nil {
		return "", err
	}
	if current != nil {
		if err := ValidateObservedPartition(current); err != nil {
			return "", err
		}
	}
	if desired != nil {
		if err := ValidateDesiredPartition(desired); err != nil {
			return "", err
		}
		if err := RefuseForeignPartition(target); err != nil {
			return "", err
		}
	}
	switch table.Action {
	case featureplan.CreateTable:
		if !table.Desired.HasTable() {
			return "", fmt.Errorf("%w: partition creation requires a declared table", schemaext.ErrInvalidValue)
		}
		return "CREATE TABLE carries or reports the declared partition key", nil
	case featureplan.DropTable:
		if !table.Current.HasTable() {
			return "", fmt.Errorf("%w: partition removal requires an observed table", schemaext.ErrInvalidValue)
		}
		return "remove partitioning with the table", nil
	case featureplan.AlterTable:
		if !table.Desired.HasTable() || !table.Current.HasTable() {
			return "", fmt.Errorf("%w: partition alteration requires both table captures", schemaext.ErrInvalidValue)
		}
		return "retain the table partition key", nil
	default:
		if desired != nil || current != nil || table.Current.Table.Partitioned {
			return "", fmt.Errorf("%w: PostgreSQL partitioning has no reconstruction plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
		}
		return "the table has no partition declaration to reconstruct", nil
	}
}
