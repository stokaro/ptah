package devdocker

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbready"
)

// Connectable is the default readiness probe: it opens the provisioned URL with
// the same connector every consumer of a dev database uses, and closes it again.
//
// Readiness is deliberately not a port check or a container-status check. A
// PostgreSQL image accepts TCP connections while it is still initializing and a
// MySQL image reports its container healthy before the database named in the
// URL exists, so both would hand the caller a database that then fails its first
// statement. Opening the URL that the caller will open is the only probe that
// answers the question actually being asked.
//
// A MySQL-family URL that names no database is probed as the whole server, as
// an image the URL names is probed; see [Spec.ReadyURL].
func Connectable(ctx context.Context, rawURL string) error {
	conn, err := dbschema.ConnectToServer(ctx, rawURL)
	if err != nil {
		return err
	}
	return conn.Close()
}

// ydbReadyTable is the table the readiness probe of a `docker://ydb` server
// creates and drops.
const ydbReadyTable = "ptah_ready"

// ydbReady is the readiness probe of a `docker://ydb` server. A YDB server
// answers a query before it takes DDL: measured on local-ydb 26.2.1.14
// started twice with in-memory disks, SELECT Version() answered 1.1 seconds
// after the start and CREATE TABLE answered `database doesn't have storage
// pools at all` for another 0.8 and 1.3 seconds. So the probe connects, then
// creates and drops a table through [ydbready.Until], which waits out that one
// refusal, as the capability probe does before its first schema change.
func ydbReady(ctx context.Context, rawURL string) (err error) {
	conn, err := dbschema.ConnectToServer(ctx, rawURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	table := sqlident.Quote(platform.YDB, ydbReadyTable)
	return ydbready.Until(ctx, func(ctx context.Context) error {
		// #nosec G202 -- the table name is a constant quoted through sqlident.
		if _, err := conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+table+
			" (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"); err != nil {
			return err
		}
		// #nosec G202 -- the table name is a constant quoted through sqlident.
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+table)
		return err
	})
}

// CreateDatabase is the default [DatabaseCreator]. On the PostgreSQL family it
// creates the database when `pg_database` has no row for it, since `CREATE
// DATABASE` there has no IF NOT EXISTS form; on the MySQL family it runs
// `CREATE DATABASE IF NOT EXISTS`, which is what the pinned community binary
// v1.3.0 runs on an image the URL names.
func CreateDatabase(ctx context.Context, serverURL, dialect, database string) (err error) {
	conn, err := dbschema.ConnectToServer(ctx, serverURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	quoted := sqlident.Quote(dialect, database)
	if !platform.IsPostgresFamily(dialect) {
		// #nosec G202 -- the database name is quoted through sqlident.
		_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoted)
		return err
	}
	var exists bool
	if err := conn.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", database,
	).Scan(&exists); err != nil {
		return fmt.Errorf("look up database %q: %w", database, err)
	}
	if exists {
		return nil
	}
	// #nosec G202 -- the database name is quoted through sqlident.
	_, err = conn.ExecContext(ctx, "CREATE DATABASE "+quoted)
	return err
}

// RunBaseline is the default [BaselineRunner]. It splits baseline into
// statements for dialect and runs them in order on one connection, stopping at
// the first the server refuses. A baseline holding only comments runs nothing.
func RunBaseline(ctx context.Context, rawURL, dialect, baseline string) (err error) {
	conn, err := dbschema.ConnectToServer(ctx, rawURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	for i, statement := range sqlutil.SplitStatementsForDialect(dialect, baseline) {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("baseline statement %d: %w", i+1, err)
		}
	}
	return nil
}
