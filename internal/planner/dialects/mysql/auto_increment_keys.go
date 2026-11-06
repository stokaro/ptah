package mysql

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/keyrelease"
)

// autoIncrementKeys is the plain indexes that have to share a statement with the
// AUTO_INCREMENT column they are the key of. MySQL and MariaDB answer 1075 to a statement
// that leaves such a column without a key, and everywhere else a plan writes an index as a
// statement of its own: the table or the column first and the index after it, or the index
// gone and the column after it. Dropped first, the column takes a one-column index with it,
// so the drop then answers 1091, and only one statement does both.
type autoIncrementKeys struct {
	semantics identifier.Semantics
	// gained is, for each column that becomes AUTO_INCREMENT, the one addition that gives it
	// its key. lost is, for each column that goes or stops being AUTO_INCREMENT, the removals
	// over it.
	gained map[string]difftypes.IndexChange
	lost   map[string][]difftypes.IndexRef
	// written is every index above, for the steps that would otherwise write it.
	written map[difftypes.IndexRef]struct{}
}

// autoIncrementKeysOf answers diff's keys of this kind on the MySQL family, and none on the
// other dialects of this planner.
func (p *Planner) autoIncrementKeysOf(diff *difftypes.SchemaDiff) autoIncrementKeys {
	keys := autoIncrementKeys{
		semantics: diff.EffectiveIdentifierSemantics(p.targetDialect()),
		gained:    make(map[string]difftypes.IndexChange),
		lost:      make(map[string][]difftypes.IndexRef),
		written:   make(map[difftypes.IndexRef]struct{}),
	}
	if !p.isMySQLFamily() {
		return keys
	}
	for _, creation := range diff.TablesAdded {
		for _, field := range creation.Fields {
			if field.AutoInc && !field.Primary && !field.Unique && !declaresPrimaryKeyOver(creation.Constraints, field.Name, keys.semantics) {
				keys.gain(diff, creation.Table.QualifiedName(), field.Name)
			}
		}
	}
	for i := range diff.TablesModified {
		table := &diff.TablesModified[i]
		for _, field := range table.ColumnsAdded {
			if field.AutoInc && !field.Primary && !field.Unique {
				keys.gain(diff, table.TableName, field.Name)
			}
		}
		for _, column := range table.ColumnsModified {
			if column.Changes["auto_increment"] == "false -> true" && !gainsAutoIncrementKey(column) {
				keys.gain(diff, table.TableName, column.ColumnName)
			}
			if column.Changes["auto_increment"] == "true -> false" && !keyrelease.GivesUpAutoIncrementKey(column) {
				keys.lose(diff, table, column.ColumnName)
			}
		}
		for _, field := range table.ColumnsRemoved {
			if field.AutoInc {
				keys.lose(diff, table, field.Name)
			}
		}
	}
	return keys
}

func (k autoIncrementKeys) key(table, column string) string {
	return k.semantics.QualifiedTableIdentityKey(table) + "\x00" + k.semantics.ColumnIdentityKey(column)
}

func (k autoIncrementKeys) gain(diff *difftypes.SchemaDiff, table, column string) {
	for _, change := range diff.IndexesAdded {
		if k.semantics.QualifiedTableIdentityKey(change.TableName) == k.semantics.QualifiedTableIdentityKey(table) &&
			k.sameColumn(firstColumnOfIndex(change.Index), column) {
			k.gained[k.key(table, column)] = change
			k.written[difftypes.IndexRef{Name: change.Index.Name, TableName: change.TableName}] = struct{}{}
			return
		}
	}
}

func (k autoIncrementKeys) lose(diff *difftypes.SchemaDiff, table *difftypes.TableDiff, column string) {
	for _, ref := range diff.IndexRemovals() {
		if k.semantics.QualifiedTableIdentityKey(ref.TableName) != k.semantics.QualifiedTableIdentityKey(table.TableName) {
			continue
		}
		for i := range table.Current.Indexes {
			index := &table.Current.Indexes[i]
			first := index.Columns
			if index.Name == ref.Name && len(first) > 0 && k.sameColumn(first[0], column) {
				k.lost[k.key(table.TableName, column)] = append(k.lost[k.key(table.TableName, column)], ref)
				k.written[ref] = struct{}{}
			}
		}
	}
}

func (k autoIncrementKeys) sameColumn(a, b string) bool {
	return a != "" && k.semantics.ColumnIdentityKey(a) == k.semantics.ColumnIdentityKey(b)
}

// gainsAKey reports a table one of whose added columns takes its key with it.
func (k autoIncrementKeys) gainsAKey(table *difftypes.TableDiff) bool {
	return slices.ContainsFunc(table.ColumnsAdded, func(field schemamodel.Field) bool {
		_, ok := k.gained[k.key(table.TableName, field.Name)]
		return ok
	})
}

// writes reports an index one of these statements holds.
func (k autoIncrementKeys) writes(ref difftypes.IndexRef) bool {
	_, ok := k.written[ref]
	return ok
}

// gainedBy is the index a statement over column has to add.
func (k autoIncrementKeys) gainedBy(table, column string) (*ast.AddIndexOperation, bool) {
	change, ok := k.gained[k.key(table, column)]
	if !ok {
		return nil, false
	}
	return &ast.AddIndexOperation{Index: indexOfChange(change)}, true
}

// insideNewTable is what a new table's CREATE TABLE holds of these indexes.
func (k autoIncrementKeys) insideNewTable(creation difftypes.TableCreation) []*ast.IndexNode {
	var indexes []*ast.IndexNode
	for _, field := range creation.Fields {
		if change, ok := k.gained[k.key(creation.Table.QualifiedName(), field.Name)]; ok {
			indexes = append(indexes, indexOfChange(change))
		}
	}
	return indexes
}

// droppedWith is the drops a statement dropping column has to hold.
func (k autoIncrementKeys) droppedWith(table, column string) []ast.AlterOperation {
	refs := k.lost[k.key(table, column)]
	operations := make([]ast.AlterOperation, 0, len(refs))
	for _, ref := range refs {
		// A plain index is dropped as a unique key's is: DROP INDEX, in a clause.
		operations = append(operations, &ast.DropConstraintOperation{ConstraintName: ref.Name, Unique: true})
	}
	return operations
}

func indexOfChange(change difftypes.IndexChange) *ast.IndexNode {
	index := modelast.FromIndex(change.Index)
	index.Table = change.TableName
	index.IfNotExists = false
	return index
}

// firstColumnOfIndex is the column a key has to start with to count as a column's key.
func firstColumnOfIndex(index schemamodel.Index) string {
	if len(index.Parts) > 0 {
		return index.Parts[0].Name
	}
	if len(index.Fields) > 0 {
		return index.Fields[0]
	}
	return ""
}

func declaresPrimaryKeyOver(constraints []schemamodel.Constraint, column string, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(constraints, func(constraint schemamodel.Constraint) bool {
		return isPrimaryKeyConstraint(constraint) && slices.ContainsFunc(constraint.Columns, func(name string) bool {
			return semantics.ColumnIdentityKey(name) == semantics.ColumnIdentityKey(column)
		})
	})
}

// oneStatement is an ALTER TABLE whose operations hold only together.
func oneStatement(table string, operations ...ast.AlterOperation) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: table, Operations: operations, OneStatement: slices.Clone(operations)}
}
