package devdocker

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlident"
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
