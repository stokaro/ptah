package goschematogo

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
)

// validateMaterializedViewFacets refuses a materialized view setting the
// matview annotation cannot carry. The ClickHouse refresh schedule is the one
// it can, as its `refresh` attribute. A schedule the source could not capture,
// such as a stored clause a reader could not read, is refused too: a Go
// source states every view's schedule, so the view would be written as a
// plain one, and applying it would remove the schedule (stokaro/ptah#4278).
func validateMaterializedViewFacets(views []schemamodel.MaterializedView, coverage schemaext.Coverage) error {
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
	for _, record := range coverage.SubjectRecords() {
		if record.Kind != chschema.RefreshKind || (record.Knowledge.State != schemaext.Unrepresentable && record.Knowledge.State != schemaext.Uninspected) {
			continue
		}
		return fmt.Errorf("%w: materialized view %s: its refresh schedule was not captured (%s), and a Go annotation states every schedule, "+
			"so the view would be written as a plain one", ptaherr.ErrUnsupportedFeature, qualifiedSubject(record.Subject), record.Knowledge.Reason)
	}
	return nil
}

// qualifiedSubject spells a subject as schema.name.
func qualifiedSubject(subject objectidentity.ID) string {
	if subject.Schema.Source == "" {
		return subject.Name.Source
	}
	return subject.Schema.Source + "." + subject.Name.Source
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
