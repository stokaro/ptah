// Package devclean resets disposable development databases between migration
// replay operations.
package devclean

import (
	"context"
	"fmt"

	"go.5x5.cz/ptah/dbschema"
)

type databaseRealmCleaner interface {
	DropDatabaseRealm(context.Context) error
}

// DatabaseRealm removes every user object in the mutation realm represented by
// conn. Dialect writers with a broader realm than their configured schema
// implement DropDatabaseRealm; single-realm writers use DropAllTables.
func DatabaseRealm(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	if conn == nil {
		return fmt.Errorf("clean dev database realm: nil database connection")
	}
	writer := conn.SchemaWriter()
	if cleaner, ok := writer.(databaseRealmCleaner); ok {
		return cleaner.DropDatabaseRealm(ctx)
	}
	return writer.DropAllTables(ctx)
}
