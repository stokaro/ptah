package migrator

import (
	"ptah.run/internal/revisiontable"
)

// The migrator's defaults are derived from internal/revisiontable rather than
// spelled out here, so that consumers which must enumerate Ptah's bookkeeping
// tables — internal/schemaclean, which names what a destructive cleanup
// destroys — read the same definition the migrator does instead of a copy that
// can drift away from it.
const (
	defaultPtahMigrationsTable = revisiontable.Ptah
	defaultAtlasRevisionsTable = revisiontable.Atlas
)

// RevisionTableFormat selects the database table layout used for migration
// revision metadata.
type RevisionTableFormat string

const (
	// RevisionTableFormatPtah stores metadata in Ptah's native
	// schema_migrations table layout.
	RevisionTableFormatPtah RevisionTableFormat = "ptah"
	// RevisionTableFormatAtlas stores metadata in Atlas's
	// atlas_schema_revisions table layout.
	RevisionTableFormatAtlas RevisionTableFormat = "atlas"
)

func (f RevisionTableFormat) isAtlas() bool {
	return f == RevisionTableFormatAtlas
}

// nativeRevisionBaseColumns are the columns a native revision table holds in
// every layout. The layout check refuses a table without them
// (migrationsTableUsesLegacyRevisionLayout), and the stranded-history check
// reads a table without them as another tool's table that shares the name.
var nativeRevisionBaseColumns = []string{"version", "description", "applied_at"}

// atlasRevisionColumns are the columns of an Atlas revision table, in the
// order every write names them.
var atlasRevisionColumns = []string{
	"version", "description", "type", "applied", "total", "executed_at", "execution_time",
	"error", "error_stmt", "hash", "partial_hashes", "operator_version",
}

// layoutColumns are the columns a table needs to be a revision history of
// this layout. A table of the history's name without them belongs to another
// tool, such as golang-migrate, whose table is also named schema_migrations.
func (f RevisionTableFormat) layoutColumns() []string {
	if f.isAtlas() {
		return atlasRevisionColumns
	}
	return nativeRevisionBaseColumns
}
