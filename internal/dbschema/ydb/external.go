package ydb

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/catalog"
	"ptah.run/dialect/ydb/ydbexternal"
)

// DescribeExternalDataSource describes the external data source at path.
// Measured on 25.1.4.7 and 26.2.1.14, the table service answers with the
// source type, the location and every other option as a property, AUTH_METHOD
// among them.
func (s *grpcSource) DescribeExternalDataSource(
	ctx context.Context,
	path string,
) (*Ydb_Table.DescribeExternalDataSourceResult, error) {
	response, err := s.table.DescribeExternalDataSource(ctx, &Ydb_Table.DescribeExternalDataSourceRequest{Path: path})
	if err != nil {
		return nil, fmt.Errorf("describe YDB external data source %s: %w", path, WithoutStackFrames(err))
	}
	var described Ydb_Table.DescribeExternalDataSourceResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB external data source %s: %w", path, err)
	}
	return &described, nil
}

// DescribeExternalTable describes the external table at path: its data source
// as an absolute path, its location, its columns, and every other option as a
// JSON array.
func (s *grpcSource) DescribeExternalTable(ctx context.Context, path string) (*Ydb_Table.DescribeExternalTableResult, error) {
	response, err := s.table.DescribeExternalTable(ctx, &Ydb_Table.DescribeExternalTableRequest{Path: path})
	if err != nil {
		return nil, fmt.Errorf("describe YDB external table %s: %w", path, WithoutStackFrames(err))
	}
	var described Ydb_Table.DescribeExternalTableResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB external table %s: %w", path, err)
	}
	return &described, nil
}

// authMethodProperty is the property a data source's description carries its
// AUTH_METHOD in.
const authMethodProperty = "AUTH_METHOD"

// externalObject describes the external data source or external table entry
// is, and adds it.
func (r *Reader) externalObject(
	ctx context.Context,
	source Source,
	schema string,
	entry *Ydb_Scheme.Entry,
	db *catalog.Database,
) error {
	name := entry.GetName()
	if entry.GetType() == Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE {
		described, err := source.DescribeExternalDataSource(ctx, r.absolute(schema, name))
		if err != nil {
			return err
		}
		return r.externalDataSource(schema, name, described, db)
	}
	described, err := source.DescribeExternalTable(ctx, r.absolute(schema, name))
	if err != nil {
		return err
	}
	return r.externalTable(schema, name, described, db)
}

// externalDataSource adds one described data source as a feature object. A
// field the pinned protocol buffers do not model refuses the read, as a
// column's does.
func (r *Reader) externalDataSource(
	schema, name string,
	described *Ydb_Table.DescribeExternalDataSourceResult,
	db *catalog.Database,
) error {
	if unknown := unknownFields(described); len(unknown) > 0 {
		return fmt.Errorf("YDB external data source %s carries field %s of its description, which this build "+
			"of Ptah does not read", r.absolute(schema, name), joinNumbers(unknown))
	}
	properties := maps.Clone(described.GetProperties())
	authMethod := properties[authMethodProperty]
	delete(properties, authMethodProperty)
	var err error
	db.FeatureObjects, err = db.FeatureObjects.With(ydbexternal.ObservedSourceObject(schema, name, ydbexternal.DataSource{
		SourceType: described.GetSourceType(),
		Location:   described.GetLocation(),
		AuthMethod: authMethod,
		Options:    ydbexternal.DescribedSourceOptions(properties, r.database),
	}))
	return err
}

// externalTable adds one described external table as a feature object, its
// data source written relative to the root the reader reads where it lies
// under it.
func (r *Reader) externalTable(
	schema, name string,
	described *Ydb_Table.DescribeExternalTableResult,
	db *catalog.Database,
) error {
	if unknown := unknownFields(described); len(unknown) > 0 {
		return fmt.Errorf("YDB external table %s carries field %s of its description, which this build of Ptah "+
			"does not read", r.absolute(schema, name), joinNumbers(unknown))
	}
	table := ydbexternal.Table{
		DataSource: ydbexternal.RelativePath(described.GetDataSourcePath(), r.database),
		Location:   described.GetLocation(),
	}
	for _, meta := range described.GetColumns() {
		columnType, nullable, err := columnType(meta.GetType())
		if err != nil {
			return fmt.Errorf("YDB external table %s column %s: %w", r.absolute(schema, name), meta.GetName(), err)
		}
		table.Columns = append(table.Columns, ydbexternal.Column{Name: meta.GetName(), Type: columnType, NotNull: !nullable})
	}
	for key, value := range described.GetContent() {
		option, err := ydbexternal.DescribedTableOption(key, value)
		if err != nil {
			return fmt.Errorf("YDB external table %s: %w", r.absolute(schema, name), err)
		}
		if table.Options == nil {
			table.Options = make(map[string]string)
		}
		table.Options[strings.ToUpper(key)] = option
	}
	var err error
	db.FeatureObjects, err = db.FeatureObjects.With(ydbexternal.ObservedTableObject(schema, name, table))
	return err
}
