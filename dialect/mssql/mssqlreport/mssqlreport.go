// Package mssqlreport counts captured SQL Server security policies for
// inventory and omission reports. It reads only the values it is handed.
package mssqlreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Service reports security policy values. Its zero value is ready for
// concurrent use.
type Service struct{}

// Definitions returns the report definition of the security policy model.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{
		{Kind: mssqlschema.SecurityPolicyKind, DisplayName: "security policies", Metrics: []schemaext.MetricDefinition{
			{Name: "security_policies", Help: "SQL Server security policies"},
			{Name: "security_predicates", Help: "Predicates SQL Server security policies bind to tables"},
		}},
	}
}

// ReportValues counts each policy once and its predicates, in input order. A
// value of the other representation, or an invalid one, is refused.
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
		predicates, err := count(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{
			{Name: "security_policies", Value: 1}, {Name: "security_predicates", Value: predicates},
		}})
	}
	return reports, ctx.Err()
}

func count(value schemaext.Value, representation schemaext.Representation) (int, error) {
	switch typed := value.(type) {
	case *mssqlschema.DesiredSecurityPolicy:
		if representation == schemaext.Desired {
			return len(typed.Predicates), mssqlschema.ValidateDesiredSecurityPolicy(typed)
		}
	case *mssqlschema.ObservedSecurityPolicy:
		if representation == schemaext.Observed {
			return len(typed.Predicates), mssqlschema.ValidateObservedSecurityPolicy(typed)
		}
	}
	return 0, fmt.Errorf("%w: security policy report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
