package migrator

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// revisionTableFor returns how dialect keeps the migrator's tables: the
// revision table its descriptor states, or the generic statements for a
// dialect whose descriptor states none, which is also what a Migrator without
// a connection writes. The dialect is folded as platform.NormalizeDialect
// folds it.
func revisionTableFor(dialect string) descriptor.RevisionTable {
	described, found := platform.Lookup(dialect)
	if !found || described.RevisionTable.Create == "" {
		return builtindialects.GenericRevisionTable()
	}
	return described.RevisionTable
}

// revisionTable returns how the connected engine keeps the migrator's tables.
func (m *Migrator) revisionTable() descriptor.RevisionTable {
	return revisionTableFor(m.connectionDialect())
}

// defaultSchemaFor returns the schema an unqualified name resolves to on
// dialect, as its descriptor states it.
func defaultSchemaFor(dialect string) string {
	described, _ := platform.Lookup(dialect)
	return described.DefaultSchema
}

// expandTableStatement fills the placeholders of a statement that creates one
// of the migrator's tables; descriptor.RevisionTable says what each holds.
// The replacement is one pass, so a name that happens to contain a
// placeholder's spelling is written as it is.
func expandTableStatement(statement, table, object, engine string) string {
	return strings.NewReplacer(
		"{table_escaped}", strings.ReplaceAll(table, "'", "''"),
		"{table}", table,
		"{object}", object,
		"{engine}", engine,
	).Replace(statement)
}
