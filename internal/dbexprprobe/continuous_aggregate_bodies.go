package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/config"
	"ptah.run/dbschema"
)

// ContinuousAggregateProbe is one declared TimescaleDB continuous aggregate
// whose SELECT needs the target server's own spelling before it can be compared
// with what the catalog holds.
type ContinuousAggregateProbe struct {
	// Key identifies the aggregate to the caller. It is returned unchanged as
	// the map key and is never sent to the server.
	Key string
	// Schema is where the probe is created. It is the schema the declaration
	// names, so the probe resolves the same objects the real aggregate does --
	// a body that reads an unqualified table finds it through the same
	// search_path.
	Schema string
	// Body is the declared SELECT, without the CREATE prefix and without a
	// trailing semicolon.
	Body string
	// MaterializedOnly is the declared option, nil when the declaration made no
	// choice. It is sent because the catalog reports it beside the definition,
	// and a probe that named a value the declaration did not would answer for a
	// different declaration.
	MaterializedOnly *bool
}

// timescaleAggregateCatalog is the view the normalized definition is read back
// from. It exists only where the extension is installed.
const timescaleAggregateCatalog = "timescaledb_information.continuous_aggregates"

// ResolveContinuousAggregateBodies asks the connected server to normalize each
// declared continuous aggregate's SELECT, so a comparison holds the same
// spelling on both sides.
//
// TimescaleDB rewrites the definition before storing it, and a read-back
// therefore differs from the declaration that produced it: an interval literal
// becomes an interval cast, a column reference gains quotes, and a GROUP BY key
// written by its output name comes back as the whole expression that name stood
// for. Comparing a declaration against that is comparing two different
// languages, and acting on the difference would drop and recreate an aggregate
// that had not changed -- discarding its materialized history each time.
//
// The declaration is put through the same rewrite the catalog form already went
// through: an aggregate with the same body is created under a probe name inside
// a transaction, its stored definition is read back, and the transaction is
// rolled back. Measured on 2.29.2 / PostgreSQL 17.11, the probe's stored
// definition is identical to the real aggregate's, and after the rollback the
// catalog holds neither the probe nor a materialization hypertable for it.
//
// WITH NO DATA is not an optimization here but a requirement: without it the
// server answers `CREATE MATERIALIZED VIEW ... WITH DATA cannot run inside a
// transaction block`, and there would be no probe to read back.
//
// A probe the server refuses -- a body that is not a valid aggregate, a
// hypertable this connection cannot see -- is returned with Resolved false
// rather than omitted, because an absent key and an unresolvable one are
// different facts and only one of them is an aggregate the caller may compare.
//
// A connection without the extension resolves nothing and asks nothing beyond
// the extension list: the catalog above does not exist there, and a failed
// statement would abort the transaction rather than degrade. The extension is
// asked about inside the same rolled-back transaction the probes use, so a
// server without it sees one read and no DDL. A connection pinned to a session
// returns nil before any of that, for the reason the package documentation
// gives: the rollback the probes need would discard the session owner's work.
func ResolveContinuousAggregateBodies(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	probes []ContinuousAggregateProbe,
) (map[string]config.ContinuousAggregateBody, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve continuous aggregate bodies: database connection is nil")
	}
	if len(probes) == 0 {
		return nil, nil
	}
	if !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}

	installed := false
	resolved := make(map[string]config.ContinuousAggregateBody, len(probes))
	ran, err := conn.WithRolledBackTransaction(ctx, "resolve continuous aggregate bodies",
		func(ctx context.Context, tx *sql.Tx) error {
			if err := hasTimescaleExtension(ctx, tx, &installed); err != nil {
				return err
			}
			if !installed {
				return nil
			}
			for i, probe := range probes {
				body, err := resolveOneContinuousAggregateBody(ctx, tx, i, probe)
				if err != nil {
					return err
				}
				resolved[probe.Key] = body
			}
			return nil
		})
	if err != nil || !ran || !installed {
		return nil, err
	}
	return resolved, nil
}

// hasTimescaleExtension asks whether the extension whose catalog the read-back
// lives in is installed on this connection. The question never fails on the
// PostgreSQL family -- pg_extension exists everywhere the dialect gate lets
// through -- so it is safe to ask inside the transaction the probes share.
func hasTimescaleExtension(ctx context.Context, tx *sql.Tx, installed *bool) error {
	const query = `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')`
	if err := tx.QueryRowContext(ctx, query).Scan(installed); err != nil {
		return fmt.Errorf("resolve continuous aggregate bodies: read extension list: %w", err)
	}
	return nil
}

// resolveOneContinuousAggregateBody creates one probe aggregate and reads its
// stored definition back.
//
// Each probe runs inside its own savepoint. A body the server refuses aborts
// the transaction, and without the savepoint the first refused declaration
// would take every later probe with it -- reporting a whole schema as
// uncomparable because one aggregate was. Measured: after ROLLBACK TO SAVEPOINT
// the session answers the next query normally.
func resolveOneContinuousAggregateBody(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe ContinuousAggregateProbe,
) (config.ContinuousAggregateBody, error) {
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(probe.Body), ";"))
	if body == "" {
		return config.ContinuousAggregateBody{}, nil
	}

	name := fmt.Sprintf("ptah_cagg_probe_%d", index)
	qualified := name
	if schema := strings.TrimSpace(probe.Schema); schema != "" {
		qualified = schema + "." + name
	}
	options := "timescaledb.continuous"
	if probe.MaterializedOnly != nil {
		options += fmt.Sprintf(", timescaledb.materialized_only = %t", *probe.MaterializedOnly)
	}
	statement := fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s WITH (%s) AS\n%s\nWITH NO DATA",
		qualified, options, body,
	)

	const savepoint = "ptah_cagg_probe"
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return config.ContinuousAggregateBody{}, fmt.Errorf(
			"resolve continuous aggregate bodies: savepoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			return config.ContinuousAggregateBody{}, fmt.Errorf(
				"resolve continuous aggregate bodies: roll back to savepoint after %q: %w",
				strings.TrimSpace(probe.Key),
				rollbackErr,
			)
		}
		// The declaration is the caller's, and refusing it here would fail a
		// comparison over an aggregate the server will refuse later anyway,
		// with a worse message. Unresolved is the honest answer.
		return config.ContinuousAggregateBody{}, nil
	}

	query := `
		SELECT c.view_definition
		FROM ` + timescaleAggregateCatalog + ` c
		WHERE c.view_name = $1`
	var stored string
	if err := tx.QueryRowContext(ctx, query, name).Scan(&stored); err != nil {
		return config.ContinuousAggregateBody{}, fmt.Errorf(
			"resolve continuous aggregate bodies: read back %q: %w",
			strings.TrimSpace(probe.Key),
			err,
		)
	}

	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
		return config.ContinuousAggregateBody{}, fmt.Errorf(
			"resolve continuous aggregate bodies: release probe: %w", err)
	}
	return config.ContinuousAggregateBody{Body: strings.TrimSpace(stored), Resolved: true}, nil
}
