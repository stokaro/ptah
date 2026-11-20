package mysql

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectlookup"
)

// holdsOneKeyPerColumnList reports whether dialect's server holds one primary
// or unique key per ordered column list of a table. Oracle does, and answers a
// second with ORA-02261, measured on Oracle Free 23; a key over the same
// columns in another order is another list. MySQL, MariaDB and SQL Server take
// a second key over the same columns.
func holdsOneKeyPerColumnList(dialect string) bool {
	return dialect == platform.Oracle
}

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
// constraint step replaces itself.
func (p *Planner) keyHolderDrops(
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) ([]ast.Node, map[constraintHostKey]struct{}) {
	dropped := make(map[constraintHostKey]struct{})
	if diff == nil || !holdsOneKeyPerColumnList(p.targetDialect()) {
		return nil, dropped
	}
	observed := make(map[string]*schemacapture.TableObservation, len(diff.ObservedConstraintHosts))
	for i := range diff.ObservedConstraintHosts {
		observation := &diff.ObservedConstraintHosts[i]
		observed[semantics.QualifiedTableIdentityKey(observation.Table.QualifiedName())] = observation
	}
	columnList := func(columns []string) []string {
		keys := make([]string, len(columns))
		for i, column := range columns {
			keys[i] = semantics.ColumnIdentityKey(column)
		}
		return keys
	}
	var drops []ast.Node
	hiddenDropped := make(map[string]bool)
	for _, add := range diff.ConstraintsAdded {
		if !isKeyType(add.Type) {
			continue
		}
		observation := observed[semantics.QualifiedTableIdentityKey(add.TableName)]
		list := columnList(add.Columns)
		for _, removal := range diff.ConstraintsRemoved {
			if _, done := dropped[removal.Identity]; done || removal.Identity == add.Identity ||
				!isKeyType(removal.Type) || !objectlookup.Same(add.TableName, removal.TableName, semantics) {
				continue
			}
			columns := removal.Columns
			if len(columns) == 0 && observation != nil {
				if held, found := heldConstraint(observation, removal.Name, semantics); found {
					columns = held.ColumnNamesOrDefault()
				}
			}
			if !slices.Equal(columnList(columns), list) {
				continue
			}
			drops = append(drops, p.dropConstraintNode(removal))
			dropped[removal.Identity] = struct{}{}
		}
		if len(list) != 1 || observation == nil {
			continue
		}
		hidden := semantics.QualifiedTableIdentityKey(add.TableName) + "\x00" + list[0]
		if !hiddenDropped[hidden] && holdsHiddenColumnKey(observation, list[0], semantics) {
			hiddenDropped[hidden] = true
			drops = append(drops, &ast.AlterTableNode{Name: add.TableName, Operations: []ast.AlterOperation{
				&ast.ModifyColumnOperation{
					Column:            &ast.ColumnNode{Name: add.Columns[0]},
					PreviousUnique:    true,
					HasPreviousUnique: true,
				},
			}})
		}
	}
	return drops, dropped
}

// columnKeysRestoredAfterDrops answers the statements that give a column its
// own key back after the plan drops a UNIQUE over that column alone, on a
// server that holds one key per column list. The target declares the column
// unique and names no key over it, so the key it wants is the one Oracle names
// for the column, which `ADD UNIQUE (column)` creates. A rollback of a named
// key that replaced an Oracle-named one has this shape: without it the column
// is left with no key at all.
func (p *Planner) columnKeysRestoredAfterDrops(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []ast.Node {
	if diff == nil || !holdsOneKeyPerColumnList(p.targetDialect()) {
		return nil
	}
	var restored []ast.Node
	seen := make(map[string]bool)
	for _, removal := range diff.ConstraintsRemoved {
		if !strings.EqualFold(removal.Type, "UNIQUE") || len(removal.Columns) != 1 {
			continue
		}
		column := semantics.ColumnIdentityKey(removal.Columns[0])
		key := semantics.QualifiedTableIdentityKey(removal.TableName) + "\x00" + column
		if seen[key] {
			continue
		}
		declaration, found := declaredHost(diff.DeclaredConstraintHosts, removal.TableName, semantics)
		if !found || !declaresOwnColumnKey(declaration, column, semantics) {
			continue
		}
		seen[key] = true
		restored = append(restored, &ast.AlterTableNode{Name: removal.TableName, Operations: []ast.AlterOperation{
			&ast.AddConstraintOperation{Constraint: ast.NewUniqueConstraint("", removal.Columns[0])},
		}})
	}
	return restored
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

// heldConstraint finds the constraint named name in the observed table.
func heldConstraint(observation *schemacapture.TableObservation, name string, semantics identifier.Semantics) (catalog.Constraint, bool) {
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
