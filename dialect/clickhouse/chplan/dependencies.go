package chplan

import (
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chkey"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

func assessParent(request featureplan.Request, table featureplan.Table) (string, error) {
	before, after, err := capturedStorage(table)
	if err != nil {
		return "", err
	}
	if table.Action == featureplan.DropTable {
		return "remove storage settings with the table; all table data is lost", nil
	}
	if table.Action != featureplan.AlterTable {
		return "", fmt.Errorf("ClickHouse storage has no plan for parent action %q", table.Action)
	}
	if !chsql.SameTable(before, after) && !slices.ContainsFunc(request.Changes, func(change schemaext.ChangeRecord) bool {
		return change.Subject.Key() == table.Subject.Key() && change.Value.Kind() == chdiff.TableKind
	}) {
		return "", fmt.Errorf("captured storage changed without a corresponding feature change")
	}
	columns := capturedColumns(request, table)
	for _, common := range request.CommonSteps {
		if common.Parent.Key() != table.Subject.Key() {
			continue
		}
		if len(common.Effects) == 0 {
			return "", fmt.Errorf("common operation %v has unknown effects on a ClickHouse table", common.ID)
		}
		for _, effect := range common.Effects {
			if !columnOf(effect.Subject, table.Subject) || effect.Action == plangraph.Read || effect.Action == plangraph.Create {
				continue
			}
			name := effect.Subject.Name.Source
			for _, clause := range []string{before.Engine, before.OrderBy, before.PrimaryKey, before.PartitionBy, before.SampleBy} {
				if usesColumn(request, table, chkey.ReferencedColumns(clause, columns), effect.Subject) {
					return "", fmt.Errorf("column %q is used by ClickHouse storage; its removal or modification requires a storage rewrite", name)
				}
			}
			if usesColumn(request, table, chkey.ReferencedColumns(after.TTL, columns), effect.Subject) {
				return "", fmt.Errorf("column %q is used by the retained TTL; remove or replace that rule before changing the column", name)
			}
		}
	}
	return "retain storage settings and order common column changes around planned TTL rules", nil
}

func capturedColumns(request featureplan.Request, table featureplan.Table) []string {
	var columns []string
	for _, column := range table.Current.Table.Columns {
		columns = append(columns, column.Name)
	}
	for _, column := range table.Desired.Fields {
		columns = append(columns, column.Name)
	}
	for _, common := range request.CommonSteps {
		for _, effect := range common.Effects {
			if columnOf(effect.Subject, table.Subject) {
				columns = append(columns, effect.Subject.Name.Source)
			}
		}
	}
	slices.Sort(columns)
	return slices.Compact(columns)
}

func ttlDependencies(request featureplan.Request, table featureplan.Table, before, after *chschema.ObservedTable, id plangraph.StepID) ([]plangraph.Dependency, []plangraph.Effect, error) {
	columns := capturedColumns(request, table)
	previous, desired := chkey.ReferencedColumns(before.TTL, columns), chkey.ReferencedColumns(after.TTL, columns)
	var edges []plangraph.Dependency
	var effects []plangraph.Effect
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, column := range columns {
		if previous[column] || desired[column] {
			effects = append(effects, plangraph.Effect{Subject: builder.ColumnParts(table.Subject.Schema.Source, table.Subject.Name.Source, column), Action: plangraph.Read})
		}
	}
	for _, common := range request.CommonSteps {
		if common.Parent.Key() != table.Subject.Key() {
			continue
		}
		for _, effect := range common.Effects {
			if !columnOf(effect.Subject, table.Subject) {
				continue
			}
			name := effect.Subject.Name.Source
			switch effect.Action {
			case plangraph.Create:
				if usesColumn(request, table, desired, effect.Subject) {
					edges = append(edges, plangraph.Dependency{Before: common.ID, After: id})
				}
			case plangraph.Drop, plangraph.Alter:
				if usesColumn(request, table, desired, effect.Subject) {
					return nil, nil, fmt.Errorf("column %q is used by the new TTL; its %s cannot be scheduled", name, effect.Action)
				}
				if usesColumn(request, table, previous, effect.Subject) {
					edges = append(edges, plangraph.Dependency{Before: id, After: common.ID})
				}
			}
		}
	}
	return edges, effects, nil
}

func columnOf(column, table objectidentity.ID) bool {
	return column.Kind == objectidentity.KindColumn && column.Catalog.Normalized == table.Catalog.Normalized &&
		column.Schema.Normalized == table.Schema.Normalized && column.Parent.Normalized == table.Name.Normalized
}

func usesColumn(request featureplan.Request, table featureplan.Table, references map[string]bool, column objectidentity.ID) bool {
	builder := objectidentity.NewBuilder(request.Identifiers)
	for name := range references {
		if builder.ColumnParts(table.Subject.Schema.Source, table.Subject.Name.Source, name).Key() == column.Key() {
			return true
		}
	}
	return false
}
