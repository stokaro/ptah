// Package policyreport counts captured PostgreSQL row-security values for
// inventory and omission reports, for the owner of package pgpolicy. It reads
// only the values it is handed and never a server.
package policyreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// The metric names. A table's switches count only where they are on, since a
// table without the facet has both off.
const (
	policiesMetric      = "row_security_policies"
	enabledTablesMetric = "row_security_enabled_tables"
	forcedTablesMetric  = "row_security_forced_tables"
)

// Service reports policy and table-state values. Its zero value is ready for
// concurrent use.
type Service struct{}

// Definitions returns independent report definitions of both models. The
// display names are the family names an omission report prints.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{
		{Kind: pgpolicy.PolicyKind, DisplayName: "row-level security policies", Metrics: []schemaext.MetricDefinition{
			{Name: policiesMetric, Help: "PostgreSQL row-level security policies"},
		}},
		{Kind: pgpolicy.TableStateKind, DisplayName: "row-level security switches", Metrics: []schemaext.MetricDefinition{
			{Name: enabledTablesMetric, Help: "Tables whose row-level security is enabled"},
			{Name: forcedTablesMetric, Help: "Tables whose row-level security applies to their owner"},
		}},
	}
}

// ReportValues returns one report per value, in input order: a policy counts
// once, and a table's switches count where they are on. A value of the other
// representation, or an invalid one, is refused, and cancellation returns no
// partial report.
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
		counts, err := count(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: counts})
	}
	return reports, ctx.Err()
}

func count(value schemaext.Value, representation schemaext.Representation) ([]schemaext.MetricCount, error) {
	switch typed := value.(type) {
	case *pgpolicy.DesiredPolicy:
		if representation == schemaext.Desired {
			return policyCount(), pgpolicy.ValidateDesiredPolicy(typed)
		}
	case *pgpolicy.ObservedPolicy:
		if representation == schemaext.Observed {
			return policyCount(), pgpolicy.ValidateObservedPolicy(typed)
		}
	case *pgpolicy.DesiredTableState:
		if representation == schemaext.Desired {
			return switchCounts(typed.Enabled, typed.Forced), pgpolicy.ValidateDesiredTableState(typed)
		}
	case *pgpolicy.ObservedTableState:
		if representation == schemaext.Observed {
			return switchCounts(typed.Enabled, typed.Forced), pgpolicy.ValidateObservedTableState(typed)
		}
	}
	return nil, fmt.Errorf("%w: row-security report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

func policyCount() []schemaext.MetricCount {
	return []schemaext.MetricCount{{Name: policiesMetric, Value: 1}}
}

func switchCounts(enabled, forced bool) []schemaext.MetricCount {
	return []schemaext.MetricCount{{Name: enabledTablesMetric, Value: switchCount[enabled]}, {Name: forcedTablesMetric, Value: switchCount[forced]}}
}

// switchCount is what one table adds to a switch's metric.
var switchCount = map[bool]int{false: 0, true: 1}
