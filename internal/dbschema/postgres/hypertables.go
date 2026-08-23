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

// hypertableQuery reads the hypertables of one schema.
//
// A hypertable is invisible to every ordinary catalog: measured on TimescaleDB
// 2.29.2 / PostgreSQL 17.11, `create_hypertable('conditions', by_range('time'))`
// leaves pg_class reporting relkind 'r' and pg_depend reporting no extension
// ownership at all, so nothing outside this view separates it from a plain
// table. The extension's own catalog is the only evidence there is.
//
// primary_dimension and primary_dimension_type are read as nullable, because
// the view declares them so; a hypertable always has one, and scanning into a
// plain string would fail the whole read the day a shape appears that does not.
const hypertableQuery = `
	SELECT
		h.hypertable_schema,
		h.hypertable_name,
		h.primary_dimension,
		h.primary_dimension_type::text,
		h.num_dimensions
	FROM ` + hypertableCatalog + ` h
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
		var dimension, dimensionType sql.NullString
		if err := rows.Scan(
			&hypertable.Schema, &hypertable.Name,
			&dimension, &dimensionType, &hypertable.Dimensions,
		); err != nil {
			return nil, err
		}
		hypertable.Schema = r.outputSchema(hypertable.Schema)
		hypertable.PrimaryDimension = dimension.String
		hypertable.PrimaryDimensionType = dimensionType.String
		hypertables = append(hypertables, hypertable)
	}
	return hypertables, rows.Err()
}
