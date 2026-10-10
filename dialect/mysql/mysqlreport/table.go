package mysqlreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// TableService reports captured table options. Its zero value is usable and
// safe for concurrent calls.
type TableService struct{}

// TableDefinitions returns independent omission labels and count metadata.
// The count describes captured values, never the completeness of a read.
func TableDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: mysqlschema.TableKind, DisplayName: "MySQL table options", Metrics: []schemaext.MetricDefinition{
		{Name: "mysql_table_options", Help: "MySQL and MariaDB table options a table states"},
	}}}
}

// ReportValues validates the representation and returns one ordered report per
// table value, counting the options it states. Errors and cancellation expose
// no partial report. Nil context or invalid values wrap
// schemaext.ErrInvalidValue.
func (TableService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		count, err := statedOptions(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: mysqlschema.TableKind,
			Counts: []schemaext.MetricCount{{Name: "mysql_table_options", Value: count}}})
	}
	return reports, ctx.Err()
}

func statedOptions(value schemaext.Value, representation schemaext.Representation) (int, error) {
	count := func(options ...string) int {
		stated := 0
		for _, option := range options {
			if option != "" {
				stated++
			}
		}
		return stated
	}
	switch value := value.(type) {
	case *mysqlschema.DesiredTable:
		if representation == schemaext.Desired {
			return count(value.Engine, value.AutoIncrement, value.Charset), mysqlschema.ValidateDesiredTable(value)
		}
	case *mysqlschema.ObservedTable:
		if representation == schemaext.Observed {
			return count(value.Charset), mysqlschema.ValidateObservedTable(value)
		}
	}
	return 0, fmt.Errorf("%w: MySQL table options report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
