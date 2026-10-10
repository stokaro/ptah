package mysqlreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// IndexService reports captured index options. Its zero value is usable and
// safe for concurrent calls.
type IndexService struct{}

// IndexDefinitions returns independent omission labels and count metadata.
// The count describes captured values, never the completeness of a read.
func IndexDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: mysqlschema.IndexKind, DisplayName: "MySQL index options", Metrics: []schemaext.MetricDefinition{
		{Name: "mysql_index_options", Help: "MySQL and MariaDB index options an index states"},
	}}}
}

// ReportValues validates the representation and returns one ordered report per
// index value, counting the options it states, as
// [TableService.ReportValues] does.
func (IndexService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		parser, err := reportedParser(value, request.Representation)
		if err != nil {
			return nil, err
		}
		count := 0
		if parser != "" {
			count = 1
		}
		reports = append(reports, schemaext.ValueReport{Kind: mysqlschema.IndexKind,
			Counts: []schemaext.MetricCount{{Name: "mysql_index_options", Value: count}}})
	}
	return reports, ctx.Err()
}

func reportedParser(value schemaext.Value, representation schemaext.Representation) (string, error) {
	switch value := value.(type) {
	case *mysqlschema.DesiredIndex:
		if representation == schemaext.Desired {
			return value.Parser, mysqlschema.ValidateDesiredIndex(value)
		}
	case *mysqlschema.ObservedIndex:
		if representation == schemaext.Observed {
			return value.Parser, mysqlschema.ValidateObservedIndex(value)
		}
	}
	return "", fmt.Errorf("%w: MySQL index options report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
