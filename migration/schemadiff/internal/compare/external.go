package compare

import (
	"maps"
	"sort"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbexternal"
	"ptah.run/migration/schemadiff/difftypes"
)

// ExternalObjects compares declared YDB external data sources and external
// tables against the ones the database holds, by directory and name, and
// carries every declared external table in the diff for a plan that has to
// create a table again over a data source it replaced.
//
// Two of one kind are equal when [ydbexternal.SameDataSource] or
// [ydbexternal.SameTable] says so, read against the root the database was
// read at: the server keeps a secret's path and a table's data source as an
// absolute path, and writes a table's options as JSON arrays. An object only
// the database holds is a removal only where the desired state claims to
// describe that kind, and one only the declaration holds is a creation only
// where the read looked, as for a secret: CREATE EXTERNAL ... carries no guard
// a plan writes.
func ExternalObjects(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	root := database.DatabasePath
	diff.DeclaredExternalTables = append(diff.DeclaredExternalTables, desired.ExternalTables...)
	compareExternalDataSources(desired, database, diff, cov, root)
	compareExternalTables(desired, database, diff, cov, root)
}

func compareExternalDataSources(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	root string,
) {
	held := make(map[string]catalog.ExternalDataSource, len(database.ExternalDataSources))
	for _, source := range database.ExternalDataSources {
		held[source.QualifiedName()] = source
	}
	declared := make(map[string]bool, len(desired.ExternalDataSources))
	var added difftypes.ExternalDataSourceChanges
	for _, source := range desired.ExternalDataSources {
		name := source.QualifiedName()
		declared[name] = true
		current, exists := held[name]
		switch {
		case !exists:
			added = append(added, source)
		case !ydbexternal.SameDataSource(dataSourceRule(source), currentDataSourceRule(current), root):
			diff.ExternalDataSourcesChanged = append(diff.ExternalDataSourcesChanged, difftypes.ExternalDataSourceChange{
				Declared: source, Current: modelDataSource(current),
			})
		}
	}
	for name, source := range held {
		if declared[name] || !cov.PlansRemoval(coverage.ExternalDataSource, source.Schema, source.Name, name) {
			continue
		}
		diff.ExternalDataSourcesRemoved = append(diff.ExternalDataSourcesRemoved, modelDataSource(source))
	}
	kept, withheld := keepPlannedAdditions(cov, coverage.ExternalDataSource, added,
		func(source schemamodel.ExternalDataSource) (string, []string) {
			return source.Schema, []string{source.Name, source.QualifiedName()}
		},
		func(source schemamodel.ExternalDataSource) string { return source.QualifiedName() },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.ExternalDataSourcesAdded = kept
	sortByName(diff.ExternalDataSourcesAdded, schemamodel.ExternalDataSource.QualifiedName)
	sortByName(diff.ExternalDataSourcesRemoved, schemamodel.ExternalDataSource.QualifiedName)
	sortByName(diff.ExternalDataSourcesChanged, func(change difftypes.ExternalDataSourceChange) string {
		return change.Declared.QualifiedName()
	})
}

func compareExternalTables(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	root string,
) {
	held := make(map[string]catalog.ExternalTable, len(database.ExternalTables))
	for _, table := range database.ExternalTables {
		held[table.QualifiedName()] = table
	}
	declared := make(map[string]bool, len(desired.ExternalTables))
	var added difftypes.ExternalTableChanges
	for _, table := range desired.ExternalTables {
		name := table.QualifiedName()
		declared[name] = true
		current, exists := held[name]
		switch {
		case !exists:
			added = append(added, table)
		case !ydbexternal.SameTable(tableRule(table), tableRule(modelExternalTable(current)), root):
			diff.ExternalTablesChanged = append(diff.ExternalTablesChanged, difftypes.ExternalTableChange{
				Declared: table, Current: modelExternalTable(current),
			})
		}
	}
	for name, table := range held {
		if declared[name] || !cov.PlansRemoval(coverage.ExternalTable, table.Schema, table.Name, name) {
			continue
		}
		diff.ExternalTablesRemoved = append(diff.ExternalTablesRemoved, modelExternalTable(table))
	}
	kept, withheld := keepPlannedAdditions(cov, coverage.ExternalTable, added,
		func(table schemamodel.ExternalTable) (string, []string) {
			return table.Schema, []string{table.Name, table.QualifiedName()}
		},
		func(table schemamodel.ExternalTable) string { return table.QualifiedName() },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.ExternalTablesAdded = kept
	sortByName(diff.ExternalTablesAdded, schemamodel.ExternalTable.QualifiedName)
	sortByName(diff.ExternalTablesRemoved, schemamodel.ExternalTable.QualifiedName)
	sortByName(diff.ExternalTablesChanged, func(change difftypes.ExternalTableChange) string {
		return change.Declared.QualifiedName()
	})
}

// sortByName orders entries by the name each one has.
func sortByName[T any](entries []T, name func(T) string) {
	sort.SliceStable(entries, func(i, j int) bool { return name(entries[i]) < name(entries[j]) })
}

// dataSourceRule is a declared data source in the form ydbexternal compares.
func dataSourceRule(source schemamodel.ExternalDataSource) ydbexternal.DataSource {
	return ydbexternal.DataSource{SourceType: source.SourceType, Location: source.Location,
		AuthMethod: source.AuthMethod, Options: source.Options}
}

// currentDataSourceRule is a data source the database holds in the same form.
func currentDataSourceRule(source catalog.ExternalDataSource) ydbexternal.DataSource {
	return dataSourceRule(modelDataSource(source))
}

// modelDataSource is a data source the database holds, as a declaration of it.
func modelDataSource(source catalog.ExternalDataSource) schemamodel.ExternalDataSource {
	return schemamodel.ExternalDataSource{
		Name: source.Name, Schema: source.Schema, SourceType: source.SourceType, Location: source.Location,
		AuthMethod: source.AuthMethod, Options: maps.Clone(source.Options),
	}
}

// tableRule is an external table in the form ydbexternal compares.
func tableRule(table schemamodel.ExternalTable) ydbexternal.Table {
	rule := ydbexternal.Table{DataSource: table.DataSource, Location: table.Location, Options: table.Options}
	for _, column := range table.Columns {
		rule.Columns = append(rule.Columns, ydbexternal.Column{Name: column.Name, Type: column.Type, NotNull: column.NotNull})
	}
	return rule
}

// modelExternalTable is an external table the database holds, as a
// declaration of it.
func modelExternalTable(table catalog.ExternalTable) schemamodel.ExternalTable {
	model := schemamodel.ExternalTable{
		Name: table.Name, Schema: table.Schema, DataSource: table.DataSource, Location: table.Location,
		Options: maps.Clone(table.Options),
	}
	for _, column := range table.Columns {
		model.Columns = append(model.Columns, schemamodel.ExternalColumn{
			Name: column.Name, Type: column.Type, NotNull: column.NotNull,
		})
	}
	return model
}
