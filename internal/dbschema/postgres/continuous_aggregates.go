package postgres

import (
	"errors"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// continuousAggregateCatalog is the view TimescaleDB publishes its continuous
// aggregates through.
//
// It is named as a string rather than joined into the view read because the
// relation only exists where the extension is installed, and a read that
// referenced it unconditionally would fail on every ordinary PostgreSQL
// server. [Reader.readContinuousAggregates] asks for it and treats "undefined
// table" as "this server has no continuous aggregates", which is what the
// answer means.
const continuousAggregateCatalog = "timescaledb_information.continuous_aggregates"

// continuousAggregateQuery reads the continuous aggregates of one schema.
//
// view_definition is the definition as it was WRITTEN, which is the reason
// this read exists at all. To PostgreSQL a continuous aggregate is an ordinary
// view -- pg_class reports relkind 'v' -- and pg_get_viewdef answers with what
// TimescaleDB rewrote it into:
//
//	SELECT device_id, bucket, avg FROM _timescaledb_internal._materialized_hypertable_2
//
// which names an object in a schema the extension owns and resolves nowhere
// else. The catalog keeps the original SELECT beside it.
const continuousAggregateQuery = `
	SELECT
		c.view_schema,
		c.view_name,
		c.hypertable_schema,
		c.hypertable_name,
		c.materialized_only,
		c.view_definition
	FROM ` + continuousAggregateCatalog + ` c
	WHERE c.view_schema = $1
	ORDER BY c.view_name`

// undefinedTableSQLState is PostgreSQL's SQLSTATE for a relation that is not
// there. It is what a server without the TimescaleDB extension answers to a
// query naming the extension's catalog.
const undefinedTableSQLState = "42P01"

// readContinuousAggregates reads the continuous aggregates of the schemas this
// read covers, and returns nothing on a server that has no such catalog.
//
// The absence of the extension is not an error and is not a degradation
// either: a server without TimescaleDB has no continuous aggregates, so an
// empty answer is the whole truth. That is different from the role reads,
// where an empty answer could mean "not permitted to look" -- here the
// relation is missing because the objects are.
func (r *Reader) readContinuousAggregates() ([]types.DBContinuousAggregate, error) {
	var aggregates []types.DBContinuousAggregate
	for _, schemaName := range r.schemasToRead() {
		schemaAggregates, err := r.readContinuousAggregatesForSchema(schemaName)
		if err != nil {
			if isUndefinedTable(err) {
				return nil, nil
			}
			return nil, err
		}
		aggregates = append(aggregates, schemaAggregates...)
	}
	return aggregates, nil
}

func (r *Reader) readContinuousAggregatesForSchema(
	schemaName string,
) ([]types.DBContinuousAggregate, error) {
	rows, err := r.db.Query(continuousAggregateQuery, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var aggregates []types.DBContinuousAggregate
	for rows.Next() {
		var aggregate types.DBContinuousAggregate
		if err := rows.Scan(
			&aggregate.Schema, &aggregate.Name,
			&aggregate.HypertableSchema, &aggregate.HypertableName,
			&aggregate.MaterializedOnly, &aggregate.Definition,
		); err != nil {
			return nil, err
		}
		aggregate.Schema = r.outputSchema(aggregate.Schema)
		aggregate.Definition = strings.TrimSpace(aggregate.Definition)
		aggregates = append(aggregates, aggregate)
	}
	return aggregates, rows.Err()
}

// withoutContinuousAggregates removes the views that are continuous
// aggregates.
//
// They arrive in the view read because pg_class reports them as views, and
// describing one as a view is wrong in both directions. A plan that drops it
// emits DROP VIEW, which TimescaleDB refuses outright -- measured on 2.29.2:
// `cannot drop continuous aggregate using DROP VIEW`, with a hint naming DROP
// MATERIALIZED VIEW -- so the plan cannot apply and is regenerated on every
// run. A plan that creates it emits CREATE VIEW with the rewritten body, which
// names a relation in a schema the extension owns.
//
// The pinned Atlas community binary v1.3.0 describes neither the aggregate nor
// its schema on the same database, which is the same answer arrived at from
// the other side.
func withoutContinuousAggregates(
	views []types.DBView,
	aggregates []types.DBContinuousAggregate,
) []types.DBView {
	if len(aggregates) == 0 {
		return views
	}
	excluded := make(map[string]bool, len(aggregates))
	for _, aggregate := range aggregates {
		excluded[continuousAggregateKey(aggregate.Schema, aggregate.Name)] = true
	}
	kept := make([]types.DBView, 0, len(views))
	for _, view := range views {
		if excluded[continuousAggregateKey(view.Schema, view.Name)] {
			continue
		}
		kept = append(kept, view)
	}
	return kept
}

func continuousAggregateKey(schema, name string) string {
	return strings.ToLower(schema) + "\x00" + strings.ToLower(name)
}

// isUndefinedTable reports whether the server answered that the relation does
// not exist.
//
// It asks the driver for the SQLSTATE rather than matching the message, for
// the reason internal/atlasretry gives about the same interface: the message
// is localized and the code is not. Any other failure is a fault and is
// surfaced, because a server that HAS the extension and refuses the read for
// another reason must not be described as one that has no continuous
// aggregates.
func isUndefinedTable(err error) bool {
	var stateErr interface{ SQLState() string }
	return errors.As(err, &stateErr) && stateErr.SQLState() == undefinedTableSQLState
}
