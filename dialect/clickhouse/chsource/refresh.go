package chsource

import (
	"fmt"
	"strings"

	"ptah.run/core/annotation"
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
	parsed, err := chrefresh.ParseClause(declared)
	if err != nil {
		return schemaext.Facets{}, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
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

// MaterializedViewDirective is the frontend's directive the refresh schedule
// is an attribute of.
const MaterializedViewDirective = "ptah:schema:matview"

// Annotations is the owner's contribution to the Go annotation frontend: the
// refresh attribute of a materialized view, read by [RefreshFacets], and the
// claim [RefreshCoverage] makes, so a view declared without a schedule
// declares a plain view. A parse that does not select the owner refuses the
// attribute as one the directive does not declare.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner: chschema.Owner,
		Kinds: []schemaext.Kind{chschema.RefreshKind},
		Attributes: []annotation.DirectiveAttributes{{
			Directive: MaterializedViewDirective,
			Attributes: []annotation.Attribute{{Name: "refresh", Value: "string",
				Description: "ClickHouse refresh schedule, as ClickHouse spells it: " +
					"`every 1 hour`, `after 30 minute`, `every 1 day offset 2 hour`. " +
					"Omitted leaves the view maintained by inserts into its source."}},
			Decode: func(attributes map[string]string) (schemaext.Facets, error) {
				return RefreshFacets(attributes["refresh"])
			},
		}},
		Coverage: RefreshCoverage,
	}
}
