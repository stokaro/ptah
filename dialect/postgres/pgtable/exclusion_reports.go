package pgtable

import (
	"context"

	"ptah.run/core/schemaext"
)

// ExclusionReports names exclusion constraints and their captured-value count.
// Counting values establishes no claim about inspection completeness.
func ExclusionReports() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ExclusionKind, DisplayName: "PostgreSQL exclusion constraints", Metrics: []schemaext.MetricDefinition{{Name: "postgres_exclusion_constraints", Help: "Captured PostgreSQL exclusion constraints"}}}}
}

// ReportValues validates and counts an ordered batch in either schema
// representation. Errors and cancellation return no partial report.
func (ExclusionService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, exclusionModelError(request.Representation, "EXCLUDE reporting requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Representation != schemaext.Desired && request.Representation != schemaext.Observed {
		return nil, exclusionModelError(request.Representation, "EXCLUDE reporting requires a schema representation")
	}
	result := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := convertExclusion(request.Representation, value); err != nil {
			return nil, err
		}
		result = append(result, schemaext.ValueReport{Kind: ExclusionKind, Counts: []schemaext.MetricCount{{Name: "postgres_exclusion_constraints", Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
