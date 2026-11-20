package mysql

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/objectlookup"
)

// holdsOneKeyPerColumnList reports whether dialect's server holds one primary
// or unique key per ordered column list of a table, which its descriptor says
// by naming the error a second key answers ([descriptor.Dialect.SecondKeyRefusal]).
// A key over the same columns in another order is another list.
func holdsOneKeyPerColumnList(dialect string) bool {
	return secondKeyRefusals.Get(dialect) != ""
}

// secondKeyRefusals holds each descriptor's SecondKeyRefusal.
var secondKeyRefusals = descriptortable.New(func(described descriptor.Dialect) string {
	return described.SecondKeyRefusal
})

// keyHolderDrops answers the statements that drop, ahead of the constraint
// additions, each key holding the ordered column list of a primary or unique
// key the plan adds to the same table, and the removals those statements carry
// out, for the removal step to skip. On a server that holds one key per list,
// a key replaced by another over the same columns fails at its ADD while the
// old key stands, so the old key goes first. Two kinds of key hold a list:
//
//   - a key the plan removes, whose columns are read from the table the plan
//     runs against ([difftypes.SchemaDiff.ObservedConstraintHosts]);
//   - the key Oracle named for a column alone, which the read carries on the
//     column's flag rather than as a constraint. It has no name the plan could
//     drop, so `DROP UNIQUE (column)` finds it by its column.
//
// A removal under the addition's own name is a changed definition, which the
// constraint step replaces itself. A removal in alreadyDropped left earlier in
// the plan, and is not dropped again.
func (p *Planner) keyHolderDrops(
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
	alreadyDropped map[constraintHostKey]struct{},
) ([]ast.Node, map[constraintHostKey]struct{}) {
	holders := keyHolders{diff: diff, semantics: semantics, dropped: maps.Clone(alreadyDropped), hidden: make(map[string]bool)}
	if holders.dropped == nil {
		holders.dropped = make(map[constraintHostKey]struct{})
	}
	if diff == nil || !holdsOneKeyPerColumnList(p.targetDialect()) {
		return nil, holders.dropped
	}
	var drops []ast.Node
	for _, add := range diff.ConstraintsAdded {
		if !isKeyType(add.Type) {
			continue
		}
		observation := holders.observed(add.TableName)
		for _, removal := range holders.removedOver(add, observation) {
			drops = append(drops, p.dropConstraintNode(removal))
		}
		if holders.dropsHiddenKey(add, observation) {
			drops = append(drops, &ast.AlterTableNode{Name: add.TableName, Operations: []ast.AlterOperation{
				&ast.ModifyColumnOperation{
					Column:            &ast.ColumnNode{Name: add.Columns[0]},
					PreviousUnique:    true,
					HasPreviousUnique: true,
				},
			}})
		}
	}
	return drops, holders.dropped
}

// releaseForeignKeysOverHeldKeys adds to plan, on a server that holds one key
// per column list, the foreign keys the diff removes or changes that reference
// a table whose keys the plan adds or removes. They leave ahead of the column
// and key changes: [Planner.keyHolderDrops] drops a key ahead of the constraint
// removals, and Oracle answers the drop of a key a foreign key references with
// ORA-02273. A foreign key the comparison rebuilds over a replaced key comes
// back with the other additions, once the new key is there. Each of these
// foreign keys leaves in this plan anyway, so the earlier drop costs nothing.
func (p *Planner) releaseForeignKeysOverHeldKeys(diff *difftypes.SchemaDiff, plan *columnTypeForeignKeyPlan) {
	if diff == nil || !holdsOneKeyPerColumnList(p.targetDialect()) {
		return
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	keyed := make(map[string]bool)
	for _, add := range diff.ConstraintsAdded {
		if isKeyType(add.Type) {
			keyed[semantics.QualifiedTableIdentityKey(add.TableName)] = true
		}
	}
	for _, removal := range diff.ConstraintsRemoved {
		if isKeyType(removal.Type) {
			keyed[semantics.QualifiedTableIdentityKey(removal.TableName)] = true
		}
	}
	for _, info := range diff.ForeignKeysRemovedWithTables {
		if _, done := plan.dropped[info.Identity]; done || !keyed[semantics.QualifiedTableIdentityKey(info.ForeignTable)] {
			continue
		}
		plan.drops = append(plan.drops, p.dropConstraintNode(difftypes.ConstraintRemovalInfo{
			Name: info.Name, TableName: info.TableName, Type: "FOREIGN KEY",
		}))
		plan.dropped[info.Identity] = struct{}{}
	}
}

// keyHolders finds the keys that hold the column lists of the keys a plan
// adds, and remembers the ones already dropped.
type keyHolders struct {
	diff      *difftypes.SchemaDiff
	semantics identifier.Semantics
	// dropped are the removals a drop carries out.
	dropped map[constraintHostKey]struct{}
	// hidden are the table and column of each column key dropped by its
	// column.
	hidden map[string]bool
}

// observed is the observation of table. The observation spells its table as
// the server reads it, with the schema, and an addition as the declaration
// does.
func (h keyHolders) observed(table string) *schemacapture.TableObservation {
	for i := range h.diff.ObservedConstraintHosts {
		if objectlookup.Same(table, h.diff.ObservedConstraintHosts[i].Table.QualifiedName(), h.semantics) {
			return &h.diff.ObservedConstraintHosts[i]
		}
	}
	return nil
}

// columnList is columns as the server compares them, in order.
func (h keyHolders) columnList(columns []string) []string {
	keys := make([]string, len(columns))
	for i, column := range columns {
		keys[i] = h.semantics.ColumnIdentityKey(column)
	}
	return keys
}

// identity is the identity of a constraint of table. A diff the comparison
// built carries each identity; one built by hand does not, and it is filled
// the way the comparison fills it.
func (h keyHolders) identity(table, name string, carried difftypes.ConstraintIdentity) difftypes.ConstraintIdentity {
	if carried == (difftypes.ConstraintIdentity{}) {
		return constraintscope.Identity(h.semantics, table, name)
	}
	return carried
}

// removedOver answers the key removals of add's table over add's column list,
// not yet dropped, and records them as dropped. A removal's columns are the
// ones it carries, or else the ones the observed table holds under its name.
func (h keyHolders) removedOver(add difftypes.ConstraintAdditionInfo, observation *schemacapture.TableObservation) []difftypes.ConstraintRemovalInfo {
	list := h.columnList(add.Columns)
	added := h.identity(add.TableName, add.Name, add.Identity)
	var removals []difftypes.ConstraintRemovalInfo
	for _, removal := range h.diff.ConstraintsRemoved {
		removed := h.identity(removal.TableName, removal.Name, removal.Identity)
		if _, done := h.dropped[removed]; done || removed == added ||
			!isKeyType(removal.Type) || !objectlookup.Same(add.TableName, removal.TableName, h.semantics) {
			continue
		}
		columns := removal.Columns
		if held, found := heldConstraint(observation, removal.Name, h.semantics); len(columns) == 0 && found {
			columns = held.ColumnNamesOrDefault()
		}
		if slices.Equal(h.columnList(columns), list) {
			removals = append(removals, removal)
			h.dropped[removed] = struct{}{}
		}
	}
	return removals
}

// dropsHiddenKey reports whether add, a key over one column, needs the key
// Oracle named for that column dropped first, and records it as dropped. A
// column the plan modifies to give up its UNIQUE already loses that key in the
// column step, which runs earlier, and a second `DROP UNIQUE` answers ORA-02442.
func (h keyHolders) dropsHiddenKey(add difftypes.ConstraintAdditionInfo, observation *schemacapture.TableObservation) bool {
	if len(add.Columns) != 1 || observation == nil {
		return false
	}
	column := h.semantics.ColumnIdentityKey(add.Columns[0])
	key := h.semantics.QualifiedTableIdentityKey(add.TableName) + "\x00" + column
	if h.hidden[key] || !holdsHiddenColumnKey(observation, column, h.semantics) ||
		h.columnStepDropsKey(add.TableName, column) {
		return false
	}
	h.hidden[key] = true
	return true
}

// columnStepDropsKey reports whether the column step drops the key of column,
// an identity key, on table: the column gives up its UNIQUE, and no UNIQUE the
// plan adds or drops over the column alone makes that change instead
// ([uniqueColumnChangeOwnedByConstraint]), which is when the step keeps it.
func (h keyHolders) columnStepDropsKey(table, column string) bool {
	for i := range h.diff.TablesModified {
		tableDiff := &h.diff.TablesModified[i]
		if !objectlookup.Same(tableDiff.TableName, table, h.semantics) {
			continue
		}
		for _, colDiff := range tableDiff.ColumnsModified {
			if h.semantics.ColumnIdentityKey(colDiff.ColumnName) == column &&
				previousColumnNullable(colDiff.Changes["unique"]) &&
				!uniqueColumnChangeOwnedByConstraint(h.diff, tableDiff, colDiff.ColumnName, h.semantics) {
				return true
			}
		}
	}
	return false
}

// columnKeysRestored answers the statements that drop a UNIQUE the plan
// removes over a column alone and give the column its own key back, on a
// server that holds one key per column list, and the removals they carry out,
// for the removal step to skip. The target declares the column unique and
// names no key over it, so the key it wants is the one Oracle names for the
// column, which `ADD UNIQUE (column)` creates once the named key is gone. A
// rollback of a named key that replaced an Oracle-named one has this shape:
// without it the column is left with no key at all. Both statements run ahead
// of the constraint additions, so a foreign key the plan adds back over the
// column references the column's own key rather than the one about to go,
// which Oracle would refuse to drop with ORA-02273. A removal in alreadyDropped
// left earlier in the plan, and is not dropped again.
func (p *Planner) columnKeysRestored(
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
	alreadyDropped map[constraintHostKey]struct{},
) ([]ast.Node, map[constraintHostKey]struct{}) {
	dropped := make(map[constraintHostKey]struct{})
	if diff == nil || !holdsOneKeyPerColumnList(p.targetDialect()) {
		return nil, dropped
	}
	var restored []ast.Node
	seen := make(map[string]bool)
	for _, removal := range diff.ConstraintsRemoved {
		if !strings.EqualFold(removal.Type, "UNIQUE") || len(removal.Columns) != 1 {
			continue
		}
		identity := removal.Identity
		if identity == (difftypes.ConstraintIdentity{}) {
			identity = constraintscope.Identity(semantics, removal.TableName, removal.Name)
		}
		column := semantics.ColumnIdentityKey(removal.Columns[0])
		key := semantics.QualifiedTableIdentityKey(removal.TableName) + "\x00" + column
		if _, done := alreadyDropped[identity]; done || seen[key] {
			continue
		}
		declaration, found := declaredHost(diff.DeclaredConstraintHosts, removal.TableName, semantics)
		if !found || !declaresOwnColumnKey(declaration, column, semantics) {
			continue
		}
		seen[key] = true
		dropped[identity] = struct{}{}
		restored = append(restored, p.dropConstraintNode(removal), &ast.AlterTableNode{Name: removal.TableName, Operations: []ast.AlterOperation{
			&ast.AddConstraintOperation{Constraint: ast.NewUniqueConstraint("", removal.Columns[0])},
		}})
	}
	return restored, dropped
}

// declaredHost finds the declaration of table among the constraint hosts.
func declaredHost(hosts []schemacapture.TableDeclaration, table string, semantics identifier.Semantics) (schemacapture.TableDeclaration, bool) {
	for _, host := range hosts {
		if objectlookup.Same(host.Table.QualifiedName(), table, semantics) {
			return host, true
		}
	}
	return schemacapture.TableDeclaration{}, false
}

// declaresOwnColumnKey reports whether the declaration makes column unique by
// itself, with no primary key and no constraint naming a key over it alone.
func declaresOwnColumnKey(declaration schemacapture.TableDeclaration, column string, semantics identifier.Semantics) bool {
	unique := slices.ContainsFunc(declaration.Fields, func(field schemamodel.Field) bool {
		return field.Unique && !field.Primary && semantics.ColumnIdentityKey(field.Name) == column
	})
	return unique && !slices.ContainsFunc(declaration.Constraints, func(constraint schemamodel.Constraint) bool {
		return isKeyType(constraint.Type) && len(constraint.Columns) == 1 && semantics.ColumnIdentityKey(constraint.Columns[0]) == column
	})
}

// isKeyType reports whether a constraint type is a primary or unique key.
func isKeyType(constraintType string) bool {
	return strings.EqualFold(constraintType, "UNIQUE") || strings.EqualFold(constraintType, "PRIMARY KEY")
}

// heldConstraint finds the constraint named name in the observed table, which
// may be nil.
func heldConstraint(observation *schemacapture.TableObservation, name string, semantics identifier.Semantics) (catalog.Constraint, bool) {
	if observation == nil {
		return catalog.Constraint{}, false
	}
	for _, held := range observation.Constraints {
		if semantics.IndexIdentityKey(held.Name) == semantics.IndexIdentityKey(name) {
			return held, true
		}
	}
	return catalog.Constraint{}, false
}

// holdsHiddenColumnKey reports whether the observed table holds a key over
// column alone that the read carries on the column's flag and not as a
// constraint: the column reads unique, and no observed UNIQUE holds it alone.
func holdsHiddenColumnKey(observation *schemacapture.TableObservation, column string, semantics identifier.Semantics) bool {
	unique := slices.ContainsFunc(observation.Table.Columns, func(held catalog.Column) bool {
		return held.IsUnique && semantics.ColumnIdentityKey(held.Name) == column
	})
	return unique && !slices.ContainsFunc(observation.Constraints, func(held catalog.Constraint) bool {
		columns := held.ColumnNamesOrDefault()
		return strings.EqualFold(held.Type, "UNIQUE") && len(columns) == 1 && semantics.ColumnIdentityKey(columns[0]) == column
	})
}
