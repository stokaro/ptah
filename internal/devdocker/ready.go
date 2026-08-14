package devdocker

import (
	"context"

	"go.5x5.cz/ptah/dbschema"
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
func Connectable(ctx context.Context, rawURL string) error {
	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	if err != nil {
		return err
	}
	return conn.Close()
}
