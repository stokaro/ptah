package schemaprecondition

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/migration/schemadiff/difftypes"
)

// RefuseFeatureChanges is the boundary for a planner with no selected feature
// handlers. It refuses every extension change without importing its owner.
// A planner that supports feature changes must dispatch them through its runtime.
func RefuseFeatureChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	if len(diff.FeatureChanges) > 0 {
		return fmt.Errorf("%w: the %s planner has no feature handler for %s", ptaherr.ErrUnsupportedFeature, dialect, diff.FeatureChanges[0].Subject)
	}
	for _, table := range diff.TablesModified {
		if len(table.FeatureChanges) > 0 {
			return fmt.Errorf("%w: the %s planner has no feature handler for %s", ptaherr.ErrUnsupportedFeature, dialect, table.FeatureChanges[0].Subject)
		}
	}
	return RefuseMaterializedViewFeatureChanges(dialect, diff)
}

// RefuseMaterializedViewFeatureChanges is the same boundary for the settings
// attached to materialized views, for a planner that dispatches table and
// standalone feature changes but has no owner for a view's.
func RefuseMaterializedViewFeatureChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	for _, view := range diff.MaterializedViewsModified {
		if len(view.FeatureChanges) > 0 {
			return fmt.Errorf("%w: the %s planner has no feature handler for %s", ptaherr.ErrUnsupportedFeature, dialect, view.FeatureChanges[0].Subject)
		}
	}
	return nil
}
