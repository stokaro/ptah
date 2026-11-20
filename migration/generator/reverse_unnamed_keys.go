package generator

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/objectlookup"
	"ptah.run/internal/tableref"
)

// reverseUnnamedPrimaryKeyLosses is the addition of each primary key the
// forward direction took away without naming it, as the rollback has to put it
// back.
//
// A target that sets Comparison.GeneratedKeyIsColumnFlag reads the key it named
// itself onto the flags of its columns, so a plan that drops it lists no
// constraint removal, and exchanging the two constraint lists leaves the key
// out of the rollback: the down file restored the columns' nullability and
// never the key. The key is added over the columns the pre-change database
// held it on, under the name an unnamed key takes in a comparison, which is
// what the forward direction names a key it adds.
func reverseUnnamedPrimaryKeyLosses(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) difftypes.ConstraintAdditions {
	described, found := platform.Lookup(dialect)
	if !found || !described.Comparison.GeneratedKeyIsColumnFlag || dbSchema == nil {
		return nil
	}
	var additions difftypes.ConstraintAdditions
	for _, table := range diff.TablesModified {
		loses := slices.ContainsFunc(table.ColumnsModified, func(column difftypes.ColumnDiff) bool {
			return column.Changes["primary_key"] == "true -> false"
		})
		if !loses || slices.ContainsFunc(diff.ConstraintsRemoved, func(removed difftypes.ConstraintRemovalInfo) bool {
			return strings.EqualFold(removed.Type, "PRIMARY KEY") && objectlookup.Same(table.TableName, removed.TableName, semantics)
		}) {
			continue
		}
		columns := heldPrimaryKeyColumns(dbSchema, table.TableName, semantics)
		if len(columns) == 0 {
			continue
		}
		name := defaultPrimaryKeyName(table.TableName)
		additions = append(additions, difftypes.ConstraintAdditionInfo{
			Name:      name,
			TableName: table.TableName,
			Type:      "PRIMARY KEY",
			Columns:   columns,
			Identity:  constraintscope.Identity(semantics, table.TableName, name),
		})
	}
	return additions
}

// heldPrimaryKeyColumns names the columns of table the pre-change read marked
// as its primary key, in the order the table lists them.
func heldPrimaryKeyColumns(dbSchema *catalog.Database, table string, semantics identifier.Semantics) []string {
	for _, held := range dbSchema.Tables {
		if !objectlookup.Same(table, held.QualifiedName(), semantics) {
			continue
		}
		var columns []string
		for _, column := range held.Columns {
			if column.IsPrimaryKey {
				columns = append(columns, column.Name)
			}
		}
		return columns
	}
	return nil
}

// defaultPrimaryKeyName is the name a comparison gives a primary key nobody
// named on table, which is what the planner then writes for the key it adds.
func defaultPrimaryKeyName(table string) string {
	if ref, ok := tableref.Parse(table); ok {
		table = ref.Name
	}
	return table + "_pkey"
}
