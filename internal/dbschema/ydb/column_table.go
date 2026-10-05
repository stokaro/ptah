package ydb

import (
	"context"
	"fmt"
	"net/url"

	"ptah.run/catalog"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbmonitor"
)

// columnTableSource supplements DescribeTable, which omits all local indexes.
// It is optional for injected sources that describe row tables only.
type columnTableSource interface {
	DescribeColumnTable(context.Context, string) (*ydbcolumn.Description, error)
}

// DescribeColumnTable reads column-table properties through the monitoring
// endpoint with the same credential and redirect policy as the flags page.
func (s *grpcSource) DescribeColumnTable(ctx context.Context, path string) (*ydbcolumn.Description, error) {
	connection := s.connection
	if connection == nil || connection.monitoring == nil {
		return nil, fmt.Errorf("column table %s requires monitoring=http(s)://host:port in the YDB URL: DescribeTable omits local indexes", path)
	}
	ticket, err := connection.Ticket(ctx)
	if err != nil {
		return nil, err
	}
	if ticket != "" && connection.secure && connection.monitoring.Scheme != "https" {
		return nil, fmt.Errorf("column table %s: a TLS connection's credential requires an HTTPS monitoring endpoint", path)
	}
	body, err := ydbmonitor.Read(ctx, connection.monitoring, "column table", "/viewer/json/describe", s.database, ticket, url.Values{"path": {path}, "database": {s.database}})
	if err != nil {
		return nil, err
	}
	return ydbcolumn.Decode(body, path)
}

func (r *Reader) columnTableEntry(ctx context.Context, source Source, schema, name string, db *catalog.Database) error {
	if !r.inScope(schema) {
		return nil
	}
	columnSource, ok := source.(columnTableSource)
	if !ok {
		return fmt.Errorf("column table %s: the metadata source cannot describe local indexes", r.absolute(schema, name))
	}
	metadata, err := columnSource.DescribeColumnTable(ctx, r.absolute(schema, name))
	if err != nil {
		return err
	}
	described, err := source.DescribeTable(ctx, r.absolute(schema, name))
	if err != nil {
		return err
	}
	return r.table(ctx, source, schema, name, described, metadata, db)
}
