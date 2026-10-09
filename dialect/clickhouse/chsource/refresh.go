package chsource

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chrefresh"
)

// RefreshFacets reads the refresh clause a materialized view declares, such as
// `EVERY 1 HOUR OFFSET 5 MINUTE APPEND`, into the ClickHouse owner's setting,
// bound to the clickhouse target. The schedule is stored in the spelling the
// server keeps, so `EVERY 60 MINUTE` is `EVERY 1 HOUR`. An empty clause
// declares no schedule and returns no facets. A clause ClickHouse would refuse
// returns an error naming what is wrong and no facets.
func RefreshFacets(clause string) (schemaext.Facets, error) {
	declared := strings.TrimSpace(clause)
	if declared == "" {
		return schemaext.Facets{}, nil
	}
	parsed := chrefresh.ParseClause(declared)
	if parsed == nil {
		return schemaext.Facets{}, fmt.Errorf("%w: refresh %q is not a ClickHouse refresh clause; expected EVERY or AFTER "+
			"followed by an interval", schemaext.ErrInvalidValue, declared)
	}
	canonical, err := chrefresh.Canonical(parsed, "")
	if err != nil {
		return schemaext.Facets{}, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	value := &chschema.DesiredRefresh{Schedule: *canonical}
	if err := chschema.ValidateDesiredRefresh(value); err != nil {
		return schemaext.Facets{}, err
	}
	facets, err := schemaext.NewFacets(value)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(chschema.RefreshKind, platform.ClickHouse)
}

// RefreshCoverage is the knowledge a source records when it states refresh
// schedules: complete for every materialized view it declares, so a view with
// no schedule declares a plain view rather than leaving the server's schedule
// unmanaged.
func RefreshCoverage() (schemaext.Coverage, error) {
	return chschema.RefreshCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}
