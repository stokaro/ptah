package mysql

import (
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/descriptortable"
)

// oneForeignKeyPerReference holds each descriptor's OneForeignKeyPerReference.
var oneForeignKeyPerReference = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.Grammar.Constraints.OneForeignKeyPerReference
})

// releaseForeignKeysOverReplacements adds to plan the foreign keys the diff
// removes that a foreign key it adds replaces, on a target that holds one key
// per column list and reference. Oracle answers the ADD with ORA-02275 while
// the old key stands, whatever name either has, so the old key goes first.
//
// A replacement is an addition and a removal on one table with the same
// columns pointing at the same columns of the same table. The names may
// differ: a key the server named SYS_C009299 is replaced by one the declaration
// names, and a key the comparison pairs by name and finds changed is
// replaced under one name. The rest of the definition does not matter, since
// a key with another delete rule is the case that needs the order.
func (p *Planner) releaseForeignKeysOverReplacements(diff *difftypes.SchemaDiff, plan *columnTypeForeignKeyPlan) {
	if diff == nil || !oneForeignKeyPerReference.Get(p.targetDialect()) {
		return
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	added := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, add := range diff.ConstraintsAdded {
		if strings.EqualFold(add.Type, "FOREIGN KEY") {
			added[foreignKeyReferenceKey(semantics, add.TableName, add.Columns, add.ForeignTable, add.ForeignColumns)] = struct{}{}
		}
	}
	for _, info := range diff.ForeignKeysRemovedWithTables {
		if _, done := plan.dropped[info.Identity]; done {
			continue
		}
		key := foreignKeyReferenceKey(semantics, info.TableName, info.Columns, info.ForeignTable, info.ForeignColumns)
		if _, replaced := added[key]; !replaced {
			continue
		}
		plan.drops = append(plan.drops, p.dropConstraintNode(difftypes.ConstraintRemovalInfo{
			Name: info.Name, TableName: info.TableName, Type: "FOREIGN KEY",
		}))
		plan.dropped[info.Identity] = struct{}{}
	}
}

// foreignKeyReferenceKey names the key a foreign key holds on a table under
// the target's identity rules: its columns and the columns it points at. Two
// keys with one key are the ones such a target refuses to hold together.
func foreignKeyReferenceKey(
	semantics identifier.Semantics, table string, columns []string, foreignTable string, foreignColumns []string,
) string {
	var key strings.Builder
	key.WriteString(semantics.QualifiedTableIdentityKey(table))
	for _, column := range columns {
		key.WriteString("\x00" + semantics.ColumnIdentityKey(column))
	}
	key.WriteString("\x01" + semantics.QualifiedTableIdentityKey(foreignTable))
	for _, column := range foreignColumns {
		key.WriteString("\x00" + semantics.ColumnIdentityKey(column))
	}
	return key.String()
}
