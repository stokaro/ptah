package migrator

import (
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/migratorext"
)

// revisionTables and defaultSchemas hold what each descriptor states about the
// migrator's tables. The migrator asks for the revision table once per
// statement it writes or reads about a revision, where copying the whole
// descriptor through platform.Lookup costs more than the statement it decides.
var (
	revisionTables = descriptortable.New(func(described descriptor.Dialect) descriptor.RevisionTable {
		if described.RevisionTable.Create == "" {
			return builtindialects.GenericRevisionTable()
		}
		return described.RevisionTable
	})
	defaultSchemas = descriptortable.New(func(described descriptor.Dialect) string {
		return described.DefaultSchema
	})
	atomicIndexCreations = descriptortable.New(func(described descriptor.Dialect) bool {
		return described.AtomicIndexCreation
	})
)

// revisionTableFor returns how dialect keeps the migrator's tables: the
// revision table its descriptor states, or the generic statements for a
// dialect whose descriptor states none, which is also what a Migrator without
// a connection writes. The dialect is folded as platform.NormalizeDialect
// folds it. The caller must not modify the answer, which is shared.
func revisionTableFor(dialect string) descriptor.RevisionTable {
	return revisionTables.Get(dialect)
}

// revisionTable returns how the connected engine keeps the migrator's tables.
func (m *Migrator) revisionTable() descriptor.RevisionTable {
	return revisionTableFor(m.connectionDialect())
}

// defaultSchemaFor returns the schema an unqualified name resolves to on
// dialect, as its descriptor states it.
func defaultSchemaFor(dialect string) string {
	return defaultSchemas.Get(dialect)
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

// services returns what the connected engine gives the migrator beyond its
// descriptor; see [servicesFor].
func (m *Migrator) services() migratorext.Services {
	return servicesFor(m.conn)
}

// metadataTable names one of the migrator's tables for a service.
func (m *Migrator) metadataTable(name string) migratorext.Table {
	return migratorext.Table{
		ConfiguredSchema: m.metadataTableSchemaName(),
		ConnectionSchema: m.connectionSchemaName(),
		Name:             name,
		Qualified:        m.qualifiedMetadataTable(name),
		Quote:            m.quoteIdentifier,
	}
}
