// Package publicationreport counts PostgreSQL publications for inventory reports,
// for the owner of package pgpublication. It reads captured values only.
package publicationreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
)

// publicationsMetric is the metric the count is reported under, the name the
// schema statistics report publications with.
const publicationsMetric = "publications"

// Service counts publications. Its zero value is ready for concurrent use.
type Service struct{}

// Definitions returns the report definitions of the publication model.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: pgpublication.PublicationKind, DisplayName: "publications", Metrics: []schemaext.MetricDefinition{
		{Name: publicationsMetric, Help: "Logical replication publications"},
	}}}
}

// ReportValues counts each publication once, in the representation the request
// names. A value of another representation or model is refused.
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
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: publicationsMetric, Value: 1}}})
	}
	return reports, ctx.Err()
}

func validate(value schemaext.Value, representation schemaext.Representation) error {
	switch typed := value.(type) {
	case *pgpublication.DesiredPublication:
		if representation == schemaext.Desired {
			return pgpublication.ValidateDesired(typed)
		}
	case *pgpublication.ObservedPublication:
		if representation == schemaext.Observed {
			return pgpublication.ValidateObserved(typed)
		}
	}
	return fmt.Errorf("%w: unexpected %s publication value %T", schemaext.ErrInvalidValue, representation, value)
}
