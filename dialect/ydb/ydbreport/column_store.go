package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// ColumnStoreService reports captured column storage without inspecting a
// server. Its zero value is usable and safe for concurrent calls.
type ColumnStoreService struct{}

// ColumnStoreDefinitions returns independent omission labels and count
// metadata. The counts describe captured values, never the completeness of a
// catalog read.
func ColumnStoreDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbschema.ColumnStoreKind, DisplayName: "column storage", Metrics: []schemaext.MetricDefinition{
		{Name: "ydb_column_tables", Help: "YDB tables stored by column"},
		{Name: "ydb_tiered_ttl_tables", Help: "YDB column tables whose TTL moves rows to an external data source"},
	}}}
}

// ReportValues validates the representation and returns one ordered report per
// table value. Errors and cancellation expose no partial report. Nil context
// or invalid values wrap schemaext.ErrInvalidValue; inputs remain unchanged.
func (ColumnStoreService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Representation != schemaext.Desired && request.Representation != schemaext.Observed {
		return nil, fmt.Errorf("%w: reporting requires a schema representation", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		store, err := reportedStore(value, request.Representation)
		if err != nil {
			return nil, err
		}
		tiered := 0
		if store.TTL != nil {
			tiered = 1
		}
		reports = append(reports, schemaext.ValueReport{Kind: ydbschema.ColumnStoreKind, Counts: []schemaext.MetricCount{
			{Name: "ydb_column_tables", Value: 1}, {Name: "ydb_tiered_ttl_tables", Value: tiered},
		}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func reportedStore(value schemaext.Value, representation schemaext.Representation) (ydbschema.ColumnStore, error) {
	switch value := value.(type) {
	case *ydbschema.DesiredColumnStore:
		if representation == schemaext.Desired {
			return value.ColumnStore, ydbschema.ValidateDesiredColumnStore(value)
		}
	case *ydbschema.ObservedColumnStore:
		if representation == schemaext.Observed {
			return value.ColumnStore, ydbschema.ValidateObservedColumnStore(value)
		}
	}
	return ydbschema.ColumnStore{}, fmt.Errorf("%w: YDB column storage report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
