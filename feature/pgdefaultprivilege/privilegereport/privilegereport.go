// Package privilegereport counts PostgreSQL default privileges for inventory reports,
// for the owner of package pgdefaultprivilege. It reads captured values only.
package privilegereport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// privilegesMetric is the metric the count is reported under, the name the
// common schema statistics report default privileges with.
const privilegesMetric = "default_privileges"

// Service counts default privileges. Its zero value is ready for concurrent use.
type Service struct{}

// Definitions returns the report definitions of the default privilege model.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: pgdefaultprivilege.DefaultPrivilegeKind, DisplayName: "default privileges", Metrics: []schemaext.MetricDefinition{
		{Name: privilegesMetric, Help: "Default privileges"},
	}}}
}

// ReportValues counts each grantor, schema, class and grantee once, in the representation the request
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
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: privilegesMetric, Value: 1}}})
	}
	return reports, ctx.Err()
}

func validate(value schemaext.Value, representation schemaext.Representation) error {
	switch typed := value.(type) {
	case *pgdefaultprivilege.Desired:
		if representation == schemaext.Desired {
			return pgdefaultprivilege.ValidateDesired(typed)
		}
	case *pgdefaultprivilege.Observed:
		if representation == schemaext.Observed {
			return pgdefaultprivilege.ValidateObserved(typed)
		}
	}
	return fmt.Errorf("%w: unexpected %s default privilege value %T", schemaext.ErrInvalidValue, representation, value)
}
