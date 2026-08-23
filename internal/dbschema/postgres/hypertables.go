package postgres

import (
	"database/sql"

	"go.5x5.cz/ptah/dbschema/types"
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

// readHypertables reads the hypertables of the schemas this read covers, and
// asks nothing at all where the extension is absent.
//
// A failure once the extension IS installed is surfaced rather than swallowed,
// for the reason the aggregate read gives: an empty answer would say "no table
// here is partitioned", and that is a claim a failed read cannot make. The
// consequence of getting it wrong is not a wrong statement but a MISSING note,
// which is the failure this whole read exists to prevent.
func (r *Reader) readHypertables(extensions []types.DBExtension) ([]types.DBHypertable, error) {
	if !hasTimescaleExtension(extensions) {
		return nil, nil
	}
	var hypertables []types.DBHypertable
	for _, schemaName := range r.schemasToRead() {
		schemaHypertables, err := r.readHypertablesForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		hypertables = append(hypertables, schemaHypertables...)
	}
	return hypertables, nil
}

func (r *Reader) readHypertablesForSchema(schemaName string) ([]types.DBHypertable, error) {
	rows, err := r.db.Query(hypertableQuery, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hypertables []types.DBHypertable
	for rows.Next() {
		var hypertable types.DBHypertable
		var dimension, dimensionType, interval sql.NullString
		if err := rows.Scan(
			&hypertable.Schema, &hypertable.Name,
			&dimension, &dimensionType, &interval, &hypertable.Dimensions,
		); err != nil {
			return nil, err
		}
		hypertable.Schema = r.outputSchema(hypertable.Schema)
		hypertable.PrimaryDimension = dimension.String
		hypertable.PrimaryDimensionType = dimensionType.String
		hypertable.ChunkInterval = interval.String
		hypertables = append(hypertables, hypertable)
	}
	return hypertables, rows.Err()
}
