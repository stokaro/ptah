package pgtable

import (
	"context"

	"ptah.run/core/schemaext"
)

// PartitionReports names the feature and counts captured partitioned tables.
// Counts describe values, not the completeness of a schema inspection.
func PartitionReports() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: PartitionKind, DisplayName: "PostgreSQL table partition keys",
		Metrics: []schemaext.MetricDefinition{{Name: "postgres_partitioned_tables", Help: "Tables with a captured PostgreSQL partition key"}}}}
}

// ReportValues reports an ordered batch without inspecting a server. Invalid
// representations, values, nil contexts and cancellation return no partial batch.
func (PartitionService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportTableValues(ctx, request, PartitionKind, "partition", "postgres_partitioned_tables", func(representation schemaext.Representation, value schemaext.Value) (int, error) {
		_, err := convertPartition(representation, value)
		return 1, err
	})
}

// PartitionMemberReports names the feature and counts captured partitions. Counts
// describe values, not the completeness of a schema inspection.
func PartitionMemberReports() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: PartitionMemberKind, DisplayName: "PostgreSQL table partitions",
		Metrics: []schemaext.MetricDefinition{{Name: "postgres_table_partitions", Help: "Tables captured as a PostgreSQL partition of another table"}}}}
}

// ReportValues reports an ordered batch without inspecting a server. Invalid
// representations, values, nil contexts and cancellation return no partial batch.
func (PartitionMemberService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportTableValues(ctx, request, PartitionMemberKind, "partition member", "postgres_table_partitions", func(representation schemaext.Representation, value schemaext.Value) (int, error) {
		_, err := convertPartitionMember(representation, value)
		return 1, err
	})
}
