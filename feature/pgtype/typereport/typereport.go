// Package typereport counts PostgreSQL domains, composite types and range
// types for inventory reports, for the owner of package pgtype. It reads
// captured values only.
package typereport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// The metric names, the ones the common schema statistics report the three
// kinds under.
const (
	domainsMetric    = "domains"
	compositesMetric = "composite_types"
	rangesMetric     = "range_types"
)

// metrics names each model's metric.
var metrics = map[schemaext.Kind]string{
	pgtype.DomainKind:    domainsMetric,
	pgtype.CompositeKind: compositesMetric,
	pgtype.RangeKind:     rangesMetric,
}

// Service counts user types. Its zero value is ready for concurrent use.
type Service struct{}

// Definitions returns independent report definitions of the three models.
// The display names are the family names an omission report prints.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{
		{Kind: pgtype.DomainKind, DisplayName: "domains", Metrics: []schemaext.MetricDefinition{{Name: domainsMetric, Help: "Domain types"}}},
		{Kind: pgtype.CompositeKind, DisplayName: "composite types", Metrics: []schemaext.MetricDefinition{
			{Name: compositesMetric, Help: "Composite types"},
		}},
		{Kind: pgtype.RangeKind, DisplayName: "range types", Metrics: []schemaext.MetricDefinition{{Name: rangesMetric, Help: "Range types"}}},
	}
}

// ReportValues counts each type once, under its model's metric, in the
// representation the request names. A value of another representation or
// model, or an invalid one, is refused, and cancellation returns no partial
// report.
func (Service) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validate(value, request.Representation); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: metrics[value.Kind()], Value: 1}}})
	}
	return reports, ctx.Err()
}

func validate(value schemaext.Value, representation schemaext.Representation) error {
	desired := representation == schemaext.Desired
	observed := representation == schemaext.Observed
	switch typed := value.(type) {
	case *pgtype.DesiredDomain:
		if desired {
			return pgtype.ValidateDesiredDomain(typed)
		}
	case *pgtype.DesiredComposite:
		if desired {
			return pgtype.ValidateDesiredComposite(typed)
		}
	case *pgtype.DesiredRange:
		if desired {
			return pgtype.ValidateDesiredRange(typed)
		}
	case *pgtype.ObservedDomain:
		if observed {
			return pgtype.ValidateObservedDomain(typed)
		}
	case *pgtype.ObservedComposite:
		if observed {
			return pgtype.ValidateObservedComposite(typed)
		}
	case *pgtype.ObservedRange:
		if observed {
			return pgtype.ValidateObservedRange(typed)
		}
	}
	return fmt.Errorf("%w: unexpected %s user type value %T", schemaext.ErrInvalidValue, representation, value)
}
