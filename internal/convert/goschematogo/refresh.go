package goschematogo

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
)

// validateMaterializedViewFacets refuses a materialized view setting the
// matview annotation cannot carry. The ClickHouse refresh schedule is the one
// it can, as its `refresh` attribute.
func validateMaterializedViewFacets(views []schemamodel.MaterializedView) error {
	for _, view := range views {
		for _, kind := range view.Facets.DeclaredKinds() {
			if kind != chschema.RefreshKind {
				return fmt.Errorf("%w: materialized view %q carries setting %q, which a Go annotation cannot represent",
					ptaherr.ErrUnsupportedFeature, view.Name, kind)
			}
		}
		if _, err := refreshClause(view); err != nil {
			return fmt.Errorf("materialized view %q: %w", view.Name, err)
		}
	}
	return nil
}

// refreshClause is the view's refresh schedule as the `refresh` attribute
// spells it, or "" for a view that declares none.
func refreshClause(view schemamodel.MaterializedView) (string, error) {
	schedule, found, err := schemaext.FacetAs[*chschema.DesiredRefresh](view.Facets, chschema.RefreshKind)
	if err != nil || !found {
		return "", err
	}
	if err := chschema.ValidateDesiredRefresh(schedule); err != nil {
		return "", err
	}
	return schedule.Clause(), nil
}
