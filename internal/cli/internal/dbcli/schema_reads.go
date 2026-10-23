package dbcli

import "ptah.run/dbschema"

// SuppressSchemaWrites puts the connection's schema writer in dry-run mode and
// returns the function that restores the mode it had. A migrator that reads
// through a dry-run writer inspects the revision table instead of creating it,
// and an absent table reads as no migrations applied.
func SuppressSchemaWrites(conn *dbschema.DatabaseConnection) (restore func()) {
	writer := conn.SchemaWriter()
	previous := writer.IsDryRun()
	writer.SetDryRun(true)
	return func() { writer.SetDryRun(previous) }
}
