package schema

import (
	"context"

	"ptah.run/dbschema"
)

// connectTarget opens the --db-url database that `schema plan`, `schema
// apply` and `schema apply --plan` change.
//
// The three read one scope, so a plan is computed, verified and applied
// against the same read. A MySQL or MariaDB URL that names no database is the
// whole server for each of them, as [dbschema.ConnectToServer] opens it; every
// other URL connects as [dbschema.ConnectToDatabase] connects it. Opened with
// ConnectToDatabase, `schema plan` and `schema apply --plan` refuse the server
// URL `schema apply` accepts, so a change to a whole server can be applied but
// never planned for review.
func connectTarget(ctx context.Context, dbURL string) (*dbschema.DatabaseConnection, error) {
	return dbschema.ConnectToServer(ctx, dbURL)
}
