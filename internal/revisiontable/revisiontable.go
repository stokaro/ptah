// Package revisiontable holds the names of the tables Ptah's migrator records
// applied migrations in.
//
// It exists so that the migrator and the packages that must reason about the
// migrator's bookkeeping — notably internal/schemaclean, which has to name
// every object a destructive cleanup destroys — read one definition instead of
// each repeating a literal. A second copy of the literal would go stale the
// moment a default changed, and would silently under-report on any setup the
// copy was not written against.
//
// migration/migrator derives its own unexported defaults from these constants,
// so this package is the single source rather than a parallel one.
package revisiontable

const (
	// Ptah is the table Ptah's native revision layout records migrations in.
	Ptah = "schema_migrations"

	// Atlas is the table the Atlas-compatible revision layout records
	// migrations in.
	Atlas = "atlas_schema_revisions"
)

// DefaultNames returns the table a migrator writes revisions to for every
// supported revision-table format, when no explicit table name is configured.
//
// Callers that enumerate bookkeeping tables in a live database must iterate all
// of them rather than picking the one matching the current configuration: a
// database can carry the residue of either format regardless of which format
// the invocation reading it happens to be configured for.
//
// An explicitly configured table name is deliberately absent. Only these
// defaults are special-cased by the dbschema readers, so only these can go
// missing from a schema snapshot; a custom name is read back as an ordinary
// table and needs no restoring.
func DefaultNames() []string {
	return []string{Atlas, Ptah}
}
