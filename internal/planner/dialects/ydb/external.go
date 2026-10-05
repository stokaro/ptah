package ydb

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
	"ptah.run/internal/ydbexternal"
	"ptah.run/migration/schemadiff/difftypes"
)

// externalPlan is what a plan does to YDB's external data sources and
// external tables, in the two places the plan puts it: drops before every
// other object changes, and creations after the secrets the data sources name.
type externalPlan struct {
	drops     []ast.Node
	creations []ast.Node
}

// planExternal plans the diff's external objects, or refuses one the target
// cannot make before any node is returned.
//
// YDB alters neither object (`Alter operation for EXTERNAL_DATA_SOURCE objects
// is not implemented`), so a changed one is replaced: by CREATE OR REPLACE on
// a target with [capability.ExternalObjectReplace], and otherwise by a DROP
// and a CREATE. A data source with external tables over it is not dropped
// (`Other entities depend on this data source`), so a data source dropped and
// created again takes the declared external tables over it along, dropped
// first and created after. So does one whose source type changes in place:
// measured on 26.2.1.14, CREATE OR REPLACE refuses that while a table reads
// the source, and 25.1.4.7 takes it and leaves the table over a source it
// cannot read.
func (p *Planner) planExternal(diff *difftypes.SchemaDiff) (externalPlan, error) {
	if err := p.refuseExternal(diff); err != nil {
		return externalPlan{}, err
	}
	replace := p.caps.Has(capability.ExternalObjectReplace)
	recreated, replaced := p.externalTablesToWrite(diff)

	var plan externalPlan
	for _, table := range diff.ExternalTablesRemoved {
		plan.drops = append(plan.drops, ast.NewDropExternalTable(table.QualifiedName()))
	}
	for _, name := range slices.Sorted(maps.Keys(recreated)) {
		plan.drops = append(plan.drops, ast.NewDropExternalTable(name))
	}
	for _, source := range diff.ExternalDataSourcesRemoved {
		plan.drops = append(plan.drops, ast.NewDropExternalDataSource(source.QualifiedName()))
	}
	if !replace {
		for _, change := range diff.ExternalDataSourcesChanged {
			plan.drops = append(plan.drops, ast.NewDropExternalDataSource(change.Declared.QualifiedName()))
		}
	}

	for _, source := range diff.ExternalDataSourcesAdded {
		plan.creations = append(plan.creations, modelast.FromExternalDataSource(source, false))
	}
	for _, change := range diff.ExternalDataSourcesChanged {
		plan.creations = append(plan.creations, modelast.FromExternalDataSource(change.Declared, replace))
	}
	for _, table := range diff.ExternalTablesAdded {
		plan.creations = append(plan.creations, modelast.FromExternalTable(table, false))
	}
	for _, name := range slices.Sorted(maps.Keys(recreated)) {
		plan.creations = append(plan.creations, modelast.FromExternalTable(recreated[name], false))
	}
	for _, name := range slices.Sorted(maps.Keys(replaced)) {
		plan.creations = append(plan.creations, modelast.FromExternalTable(replaced[name], true))
	}
	return plan, nil
}

// externalTablesToWrite returns the declared external tables a plan writes
// besides the added ones: recreated, which the plan drops and creates again,
// and replaced, which it replaces in place. A changed table is replaced where
// the target has CREATE OR REPLACE and recreated where it does not, and every
// declared table over a data source the plan drops and creates again is
// recreated with it, unless the plan adds it anyway.
func (p *Planner) externalTablesToWrite(
	diff *difftypes.SchemaDiff,
) (recreated, replaced map[string]schemamodel.ExternalTable) {
	replace := p.caps.Has(capability.ExternalObjectReplace)
	root := diff.CurrentDatabasePath
	recreatedSources := make(map[string]bool)
	for _, change := range diff.ExternalDataSourcesChanged {
		if !replace || change.Declared.SourceType != change.Current.SourceType {
			recreatedSources[sourcePath(change.Declared)] = true
		}
	}
	added := make(map[string]bool, len(diff.ExternalTablesAdded))
	for _, table := range diff.ExternalTablesAdded {
		added[table.QualifiedName()] = true
	}
	recreated = make(map[string]schemamodel.ExternalTable)
	replaced = make(map[string]schemamodel.ExternalTable)
	for _, change := range diff.ExternalTablesChanged {
		name := change.Declared.QualifiedName()
		if replace && !recreatedSources[ydbexternal.RelativePath(change.Declared.DataSource, root)] {
			replaced[name] = change.Declared
			continue
		}
		recreated[name] = change.Declared
	}
	for _, table := range diff.DeclaredExternalTables {
		name := table.QualifiedName()
		if recreatedSources[ydbexternal.RelativePath(table.DataSource, root)] && !added[name] {
			delete(replaced, name)
			recreated[name] = table
		}
	}
	return recreated, replaced
}

// refuseExternal refuses an external object change the target cannot make:
// every change on a target without [capability.ExternalDataSources], and a
// created or replaced object whose declaration the line refuses.
func (p *Planner) refuseExternal(diff *difftypes.SchemaDiff) error {
	if err := refuseOrphanedExternalTables(diff); err != nil {
		return err
	}
	sources := slices.Concat([]schemamodel.ExternalDataSource(diff.ExternalDataSourcesAdded),
		changedSources(diff.ExternalDataSourcesChanged))
	for _, source := range sources {
		if err := planExternalRefusal(ydbexternal.CheckDataSource(source.QualifiedName(),
			ydbexternal.DataSource{SourceType: source.SourceType, Location: source.Location,
				AuthMethod: source.AuthMethod, Options: source.Options}, p.caps)); err != nil {
			return err
		}
	}
	tables := slices.Concat([]schemamodel.ExternalTable(diff.ExternalTablesAdded), changedTables(diff.ExternalTablesChanged))
	for _, table := range tables {
		if err := planExternalRefusal(ydbexternal.CheckTable(table.QualifiedName(), externalTableRule(table), p.caps)); err != nil {
			return err
		}
	}
	for _, source := range diff.ExternalDataSourcesRemoved {
		if err := planExternalRefusal(ydbexternal.CheckDrop("DROP EXTERNAL DATA SOURCE "+source.QualifiedName(), p.caps)); err != nil {
			return err
		}
	}
	for _, table := range diff.ExternalTablesRemoved {
		if err := planExternalRefusal(ydbexternal.CheckDrop("DROP EXTERNAL TABLE "+table.QualifiedName(), p.caps)); err != nil {
			return err
		}
	}
	return nil
}

// refuseOrphanedExternalTables refuses a plan that drops a data source a
// declared external table reads. The plan would keep the table over a source
// that is gone: 25.4.1.15 and later refuse the drop, and 25.1.4.7 to
// 25.3.1.25 take it and leave a table nothing can drop (`path hasn't been
// resolved`).
func refuseOrphanedExternalTables(diff *difftypes.SchemaDiff) error {
	removed := make(map[string]string, len(diff.ExternalDataSourcesRemoved))
	for _, source := range diff.ExternalDataSourcesRemoved {
		removed[sourcePath(source)] = source.QualifiedName()
	}
	for _, table := range diff.DeclaredExternalTables {
		if name, dropped := removed[ydbexternal.RelativePath(table.DataSource, diff.CurrentDatabasePath)]; dropped {
			return refuseFact("external table "+table.QualifiedName(), "it reads data source "+name+
				", which the plan drops; declare the data source or move the table to one the plan keeps")
		}
	}
	return nil
}

func changedSources(changes []difftypes.ExternalDataSourceChange) []schemamodel.ExternalDataSource {
	sources := make([]schemamodel.ExternalDataSource, 0, len(changes))
	for _, change := range changes {
		sources = append(sources, change.Declared)
	}
	return sources
}

func changedTables(changes []difftypes.ExternalTableChange) []schemamodel.ExternalTable {
	tables := make([]schemamodel.ExternalTable, 0, len(changes))
	for _, change := range changes {
		tables = append(tables, change.Declared)
	}
	return tables
}

// externalTableRule is table in the form ydbexternal checks.
func externalTableRule(table schemamodel.ExternalTable) ydbexternal.Table {
	rule := ydbexternal.Table{DataSource: table.DataSource, Location: table.Location, Options: table.Options}
	for _, column := range table.Columns {
		rule.Columns = append(rule.Columns, ydbexternal.Column{Name: column.Name, Type: column.Type, NotNull: column.NotNull})
	}
	return rule
}

// sourcePath is a data source's path relative to the database root, as an
// external table names it.
func sourcePath(source schemamodel.ExternalDataSource) string {
	return strings.Trim(strings.Trim(source.Schema, "/")+"/"+source.Name, "/")
}

// planExternalRefusal turns a refusal into the planner's error.
func planExternalRefusal(refusal *ydbexternal.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
