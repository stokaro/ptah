package mysqlreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// IndexBlockSizeMetric counts the indexes with a KEY_BLOCK_SIZE hint. A read
// observes every index, so the observed count is of hints held, not of
// observations.
const IndexBlockSizeMetric = "mysql_index_block_sizes"

// IndexBlockSizeService reports [mysqlschema.IndexBlockSizeKind] values. Its
// zero value is usable and safe for concurrent calls.
type IndexBlockSizeService struct{}

// IndexBlockSizeDefinitions declares the block-size label and metric.
func IndexBlockSizeDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: mysqlschema.IndexBlockSizeKind, DisplayName: "MySQL index block size", Metrics: []schemaext.MetricDefinition{
		{Name: IndexBlockSizeMetric, Help: "Indexes with a MySQL-family KEY_BLOCK_SIZE hint"},
	}}}
}

// ReportValues counts each value whose hint is not zero, reporting the metric
// for every value as the runtime requires. A value of the other
// representation is refused.
func (IndexBlockSizeService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		size, err := reportedBlockSize(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: mysqlschema.IndexBlockSizeKind,
			Counts: []schemaext.MetricCount{{Name: IndexBlockSizeMetric, Value: counted[size != 0]}}})
	}
	return reports, ctx.Err()
}

func reportedBlockSize(value schemaext.Value, representation schemaext.Representation) (uint64, error) {
	switch typed := value.(type) {
	case *mysqlschema.DesiredIndexBlockSize:
		if representation == schemaext.Desired {
			if err := mysqlschema.ValidateDesiredIndexBlockSize(typed); err != nil {
				return 0, err
			}
			return typed.KeyBlockSize, nil
		}
	case *mysqlschema.ObservedIndexBlockSize:
		if representation == schemaext.Observed {
			if err := mysqlschema.ValidateObservedIndexBlockSize(typed); err != nil {
				return 0, err
			}
			return typed.KeyBlockSize, nil
		}
	}
	return 0, fmt.Errorf("%w: MySQL index block size report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
