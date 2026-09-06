package postgres

import (
	"context"
	"strings"

	"ptah.run/catalog"
)

// timescaleExtension is the extension whose presence decides whether the
// catalog below exists at all.
const timescaleExtension = "timescaledb"

// continuousAggregateCatalog is the view TimescaleDB publishes its continuous
// aggregates through.
//
// The relation only exists where the extension is installed, and the query is
// therefore not run at all where it is not. Asking anyway and tolerating the
// failure is what an earlier draft did, and it was wrong in a way no unit test
// showed: a failed statement ABORTS the enclosing PostgreSQL transaction, so
// every later read answered `current transaction is aborted, commands ignored
// until end of transaction block` (SQLSTATE 25P02). Recovering from an error
// inside a transaction is not a degradation -- there is nothing left to
// degrade to.
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

// hasTimescaleExtension reports whether the extension is installed, from the
// extension list this read already took.
//
// Reading the answer off a list already in hand rather than asking the server
// again is what keeps the aggregate query off a database that would refuse it.
// A server whose preset left the extension read out reports none, which is the
// right answer for the two targets that happens to: neither Spanner nor a
// catalog without pg_extension has TimescaleDB.
func hasTimescaleExtension(extensions []catalog.Extension) bool {
	for _, extension := range extensions {
		if strings.EqualFold(extension.Name, timescaleExtension) {
			return true
		}
	}
	return false
}

// readContinuousAggregates reads the continuous aggregates of the schemas this
// read covers, and asks nothing at all where the extension is absent.
//
// A failure once the extension IS installed is surfaced rather than swallowed.
// An empty answer there would say "this server has no continuous aggregates",
// which is a claim a failed read cannot make -- and the objects it would hide
// are exactly the ones a plan must not treat as views.
func (r *Reader) readContinuousAggregates(ctx context.Context,
	extensions []catalog.Extension,
) ([]catalog.ContinuousAggregate, error) {
	if !hasTimescaleExtension(extensions) {
		return nil, nil
	}
	var aggregates []catalog.ContinuousAggregate
	for _, schemaName := range r.schemasToRead() {
		schemaAggregates, err := r.readContinuousAggregatesForSchema(ctx, schemaName)
		if err != nil {
			return nil, err
		}
		aggregates = append(aggregates, schemaAggregates...)
	}
	return aggregates, nil
}

func (r *Reader) readContinuousAggregatesForSchema(ctx context.Context,
	schemaName string,
) ([]catalog.ContinuousAggregate, error) {
	rows, err := r.db.QueryContext(ctx, continuousAggregateQuery, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var aggregates []catalog.ContinuousAggregate
	for rows.Next() {
		var aggregate catalog.ContinuousAggregate
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
	views []catalog.View,
	aggregates []catalog.ContinuousAggregate,
) []catalog.View {
	if len(aggregates) == 0 {
		return views
	}
	excluded := make(map[string]bool, len(aggregates))
	for _, aggregate := range aggregates {
		excluded[continuousAggregateKey(aggregate.Schema, aggregate.Name)] = true
	}
	kept := make([]catalog.View, 0, len(views))
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
