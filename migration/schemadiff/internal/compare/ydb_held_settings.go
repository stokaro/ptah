package compare

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// CurrentYDBSettings returns what each YDB row table of database holds of its
// partitioning, read replicas and key bloom filter, and the partitioning of
// each of its global indexes, for every table holding a setting other than
// YDB's documented defaults, sorted by table. Only a YDB read reports such a
// setting, so the list is empty for every other database.
//
// A plan that recreates a table writes these for each setting the declaration
// leaves out; see [difftypes.SchemaDiff.CurrentYDBSettings].
func CurrentYDBSettings(database *catalog.Database) []difftypes.YDBHeldSettings {
	if database == nil {
		return nil
	}
	held := make(map[string]*difftypes.YDBHeldSettings)
	entry := func(schema, table string) *difftypes.YDBHeldSettings {
		name := schemamodel.QualifyTableName(schema, table)
		if held[name] == nil {
			held[name] = &difftypes.YDBHeldSettings{TableName: name}
		}
		return held[name]
	}
	for _, table := range database.Tables {
		if !table.YDBPartitioning.IsZero() {
			entry(table.Schema, table.Name).Partitioning = table.YDBPartitioning.Clone()
		}
	}
	for _, index := range database.Indexes {
		if index.Partitioning.IsZero() || strings.TrimSpace(index.Name) == "" {
			continue
		}
		table := entry(index.Schema, index.TableName)
		if table.Indexes == nil {
			table.Indexes = make(map[string]*ast.IndexPartitioningSpec)
		}
		table.Indexes[index.Name] = index.Partitioning.Clone()
	}
	settings := make([]difftypes.YDBHeldSettings, 0, len(held))
	for _, name := range slices.Sorted(maps.Keys(held)) {
		settings = append(settings, *held[name])
	}
	return settings
}
