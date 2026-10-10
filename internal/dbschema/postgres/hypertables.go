package postgres

import (
	"context"
	"database/sql"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// hypertableCatalog is the view TimescaleDB publishes its hypertables through.
//
// Like [continuousAggregateCatalog] it exists only where the extension is
// installed, and the query is therefore not run at all where it is not. Asking
// anyway and tolerating the failure aborts the enclosing PostgreSQL
// transaction, which leaves every later read answering SQLSTATE 25P02 rather
// than the question it was asked.
const hypertableCatalog = "timescaledb_information.hypertables"

// dimensionCatalog is where the partitioning columns are published, and it is
// the portable half of this read.
//
// `timescaledb_information.hypertables` grew `primary_dimension` and
// `primary_dimension_type` late. Measured on two releases of the extension in
// the same database, `information_schema.columns` over the view:
//
//	2.14.2  hypertable_schema, hypertable_name, owner, num_dimensions,
//	        num_chunks, compression_enabled, tablespaces
//	2.29.2  ... the same seven, plus primary_dimension, primary_dimension_type
//
// So a projection naming those two fails outright on 2.14.2 with
// `column h.primary_dimension does not exist`, and the failure is not confined
// to this read: it aborts the enclosing PostgreSQL transaction, so every later
// read answers SQLSTATE 25P02.
//
// This view carries the same values and the same columns on both releases --
// `column_name` and `column_type` for `dimension_number = 1` answered
// `time` / `timestamp with time zone` on each, identical to what 2.29.2 reports
// through the newer projection.
const dimensionCatalog = "timescaledb_information.dimensions"

// hypertableQuery reads the hypertables of one schema.
//
// A hypertable is invisible to every ordinary catalog: measured on TimescaleDB
// 2.29.2 / PostgreSQL 17.11, `create_hypertable('conditions', by_range('time'))`
// leaves pg_class reporting relkind 'r' and pg_depend reporting no extension
// ownership at all, so nothing outside this view separates it from a plain
// table. The extension's own catalog is the only evidence there is.
//
// The primary dimension comes from [dimensionCatalog] rather than from the
// newer columns on the hypertable view, because those do not exist on every
// supported release -- the reason is recorded there.
//
// The join is a LEFT JOIN and the two columns are read as nullable, so a
// hypertable whose dimensions the view does not report is described with one
// detail missing rather than failing the whole read.
const hypertableQuery = `
	SELECT
		h.hypertable_schema,
		h.hypertable_name,
		d.column_name,
		d.column_type::text,
		d.time_interval::text,
		h.num_dimensions
	FROM ` + hypertableCatalog + ` h
	LEFT JOIN ` + dimensionCatalog + ` d
		ON d.hypertable_schema = h.hypertable_schema
		AND d.hypertable_name = h.hypertable_name
		AND d.dimension_number = 1
	WHERE h.hypertable_schema = $1
	ORDER BY h.hypertable_name`

// readHypertable is one hypertable a read found, before it is attached to its
// table. observed is nil when the catalog reported no dimension for it.
type readHypertable struct {
	schema, outputSchema, table string
	observed                    *tsschema.ObservedHypertable
}

// readHypertables reads the hypertables of the schemas this read covers, and
// asks nothing at all where the extension is absent.
//
// A failure once the extension IS installed is surfaced rather than swallowed,
// for the reason the aggregate read gives: an empty answer would say "no table
// here is partitioned", and that is a claim a failed read cannot make.
func (r *Reader) readHypertables(ctx context.Context, extensions []catalog.Extension) ([]readHypertable, error) {
	if !hasTimescaleExtension(extensions) {
		return nil, nil
	}
	var hypertables []readHypertable
	for _, schemaName := range r.schemasToRead() {
		schemaHypertables, err := r.readHypertablesForSchema(ctx, schemaName)
		if err != nil {
			return nil, err
		}
		hypertables = append(hypertables, schemaHypertables...)
	}
	return hypertables, nil
}

func (r *Reader) readHypertablesForSchema(ctx context.Context, schemaName string) ([]readHypertable, error) {
	rows, err := r.db.QueryContext(ctx, hypertableQuery, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hypertables []readHypertable
	for rows.Next() {
		var hypertable readHypertable
		var dimension, dimensionType, interval sql.NullString
		var dimensions int
		if err := rows.Scan(
			&hypertable.schema, &hypertable.table,
			&dimension, &dimensionType, &interval, &dimensions,
		); err != nil {
			return nil, err
		}
		hypertable.outputSchema = r.outputSchema(hypertable.schema)
		if dimension.String != "" {
			hypertable.observed = &tsschema.ObservedHypertable{Column: dimension.String, ColumnType: dimensionType.String,
				ChunkInterval: interval.String, Dimensions: max(dimensions, 1)}
		}
		hypertables = append(hypertables, hypertable)
	}
	return hypertables, rows.Err()
}

// attachTimescale records what the TimescaleDB catalog reported on the read
// it belongs to: hypertable settings on their tables, continuous aggregates as
// named objects, and complete coverage of both, because the read asked the
// extension's own catalog -- or established that the extension, and with it
// every hypertable and aggregate, is absent.
//
// A hypertable whose dimension the catalog did not report is recorded as a
// knowledge limit on its table rather than as settings, because a declaration
// needs the column and there is none to carry. A comparison then decides
// nothing about that table's partitioning, and an export refuses it.
func (r *Reader) attachTimescale(schema *catalog.Database, hypertables []readHypertable, aggregates []readAggregate) error {
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres))
	var limits []schemaext.SubjectCoverage
	for _, hypertable := range hypertables {
		index := slices.IndexFunc(schema.Tables, func(table catalog.Table) bool {
			return table.Schema == hypertable.outputSchema && table.Name == hypertable.table
		})
		if index < 0 {
			continue
		}
		if hypertable.observed == nil {
			limits = append(limits, schemaext.SubjectCoverage{Kind: tsschema.HypertableKind,
				Subject:   builder.TableParts(hypertable.outputSchema, hypertable.table),
				Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the catalog reported no dimension for this hypertable"}})
			continue
		}
		facets, err := schema.Tables[index].Facets.With(hypertable.observed)
		if err != nil {
			return err
		}
		schema.Tables[index].Facets = facets
	}
	for _, aggregate := range aggregates {
		var err error
		schema.FeatureObjects, err = schema.FeatureObjects.With(tsschema.ObservedContinuousAggregateObject(aggregate.schema, aggregate.name, aggregate.observed))
		if err != nil {
			return err
		}
	}
	known, err := tsschema.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return err
	}
	if len(limits) > 0 {
		known, err = schemaext.NewCoverage(schemaext.Observed, known.KindRecords(), limits)
		if err != nil {
			return err
		}
	}
	schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(known)
	return err
}
