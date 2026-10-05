package compare

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// AdoptUndescribedColumnTables keeps column storage and tiered TTL when a
// source format cannot express them. An explicit Go or YAML declaration still
// controls the storage kind; the input models are never modified.
func AdoptUndescribedColumnTables(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) *schemamodel.Database {
	if desired == nil || database == nil {
		return desired
	}
	held := make(map[tableIdentity]catalog.Table, len(database.Tables))
	for _, table := range database.Tables {
		if table.YDBColumnTable != nil {
			held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
		}
	}
	if len(held) == 0 {
		return desired
	}
	adopted := *desired
	adopted.Tables = slices.Clone(desired.Tables)
	for i, table := range adopted.Tables {
		current, holds := held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)]
		if !holds || table.YDBColumnTable != nil ||
			desired.NotDescribed.DescribesIn(coverage.ColumnTable, table.Schema, tableref.Canonical(table.Schema, table.Name), table.Name) {
			continue
		}
		adopted.Tables[i].YDBColumnTable = current.YDBColumnTable.Clone()
	}
	return &adopted
}
