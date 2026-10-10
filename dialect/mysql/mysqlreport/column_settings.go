package mysqlreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// The column settings metrics. A read reports a character set for every text
// column, so the observed count includes inherited ones.
const (
	ColumnCharsetMetric  = "mysql_column_character_sets"
	ColumnOnUpdateMetric = "mysql_column_on_update_clauses"
)

// ColumnService reports [mysqlschema.ColumnSettingsKind] values.
type ColumnService struct{}

// ColumnDefinitions declares the column settings label and metrics.
func ColumnDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: mysqlschema.ColumnSettingsKind, DisplayName: "MySQL column settings", Metrics: []schemaext.MetricDefinition{
		{Name: ColumnCharsetMetric, Help: "Columns with a captured MySQL-family character set"},
		{Name: ColumnOnUpdateMetric, Help: "Columns with a captured MySQL-family ON UPDATE clause"},
	}}}
}

// ReportValues counts each value's character set and ON UPDATE clause,
// reporting both metrics for every value: the runtime refuses a report that
// leaves one out. A value of the other representation is refused.
func (ColumnService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		settings, err := reportedSettings(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: mysqlschema.ColumnSettingsKind, Counts: []schemaext.MetricCount{
			{Name: ColumnCharsetMetric, Value: counted[settings.Charset != ""]},
			{Name: ColumnOnUpdateMetric, Value: counted[settings.OnUpdate != ""]},
		}})
	}
	return reports, ctx.Err()
}

func reportedSettings(value schemaext.Value, representation schemaext.Representation) (mysqlschema.ColumnSettings, error) {
	switch typed := value.(type) {
	case *mysqlschema.DesiredColumnSettings:
		if representation == schemaext.Desired {
			return mysqlschema.ColumnSettings{Charset: typed.Charset, OnUpdate: typed.OnUpdate}, mysqlschema.ValidateDesiredColumnSettings(typed)
		}
	case *mysqlschema.ObservedColumnSettings:
		if representation == schemaext.Observed {
			return mysqlschema.ColumnSettings{Charset: typed.Charset, OnUpdate: typed.OnUpdate}, mysqlschema.ValidateObservedColumnSettings(typed)
		}
	}
	return mysqlschema.ColumnSettings{}, fmt.Errorf("%w: MySQL column settings report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

// counted is what a value adds to a metric: one when it states what the metric
// counts, zero otherwise.
var counted = map[bool]int{true: 1, false: 0}
