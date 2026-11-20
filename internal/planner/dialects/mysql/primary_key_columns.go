package mysql

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/modelast"
	"ptah.run/internal/objectlookup"
	"ptah.run/internal/planner/columnchange"
)

// generatedKeysAreColumnFlags holds each descriptor's
// Comparison.GeneratedKeyIsColumnFlag.
var generatedKeysAreColumnFlags = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.Comparison.GeneratedKeyIsColumnFlag
})

// primaryKeysDroppedFirst holds each descriptor's
// Grammar.Constraints.PrimaryKeyDroppedFirst.
var primaryKeysDroppedFirst = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.Grammar.Constraints.PrimaryKeyDroppedFirst
})

// losesPrimaryKey reports whether the change takes the primary key away from
// its column.
func losesPrimaryKey(column difftypes.ColumnDiff) bool {
	return column.Changes["primary_key"] == "true -> false"
}

// losesUnnamedPrimaryKey reports whether a column of the table gives up the
// primary key while the diff removes no primary key of the table by name.
//
// That is the key the server named for a column, which a target that sets
// Comparison.GeneratedKeyIsColumnFlag reads onto the column's flag and not as a
// constraint. MODIFY leaves such a key as it was: measured on Oracle Free 23.26,
// `MODIFY (id NUMBER(10))` keeps the key, and a plan that wrote only that never
// converged. `DROP PRIMARY KEY` finds the key with no name, so the plan drops it
// ahead of the column changes and of a key it adds over other columns.
func (p *Planner) losesUnnamedPrimaryKey(diff *difftypes.SchemaDiff, tableDiff *difftypes.TableDiff, semantics identifier.Semantics) bool {
	if !generatedKeysAreColumnFlags.Get(p.targetDialect()) || !slices.ContainsFunc(tableDiff.ColumnsModified, losesPrimaryKey) {
		return false
	}
	return !slices.ContainsFunc(diff.ConstraintsRemoved, func(info difftypes.ConstraintRemovalInfo) bool {
		return strings.EqualFold(info.Type, "PRIMARY KEY") && objectlookup.Same(tableDiff.TableName, info.TableName, semantics)
	})
}

// keyOwnedNotNull reports whether the column loses a primary key that is the
// only reason it is NOT NULL ([difftypes.ColumnDiff.CurrentNotNullFromKey]).
// The key's removal then decides the column's nullability by itself.
func keyOwnedNotNull(column difftypes.ColumnDiff) bool {
	return column.CurrentNotNullFromKey && losesPrimaryKey(column)
}

// statesNotNullBeforeKeyLoss answers the statement that gives each column the
// plan takes a key from, and that is NOT NULL only through the key, a NOT NULL
// of its own where the declaration wants one.
//
// Measured on Oracle Free 23.26, DROP PRIMARY KEY makes such a column nullable,
// and MODIFY (id NOT NULL) then succeeds. Before the drop it succeeds as well,
// since a key column accepts the clause and adds the column's own constraint,
// which the drop leaves in place. After it, the same clause on a column that
// already holds the constraint answers ORA-01442, so only the read can say
// which of the two a column is, and the statement goes ahead of the drop in
// either case. One plan then converges for a column declared `NOT NULL` and
// for one declared `PRIMARY KEY`.
func (p *Planner) statesNotNullBeforeKeyLoss(tableDiff *difftypes.TableDiff, diff *difftypes.SchemaDiff) []ast.Node {
	var statements []ast.Node
	for _, column := range tableDiff.ColumnsModified {
		if !keyOwnedNotNull(column) || column.Desired.Nullable || column.Desired.Name == "" {
			continue
		}
		field := column.Desired
		field.Primary, field.Unique = false, false
		modify := &ast.ModifyColumnOperation{
			Column:              modelast.FromField(field, diff.DeclaredUserTypes.Enums, p.targetDialect()),
			PreviousNullable:    true,
			HasPreviousNullable: true,
			Changed:             columnchange.Properties(difftypes.ColumnDiff{Changes: map[string]string{"nullable": "true -> false"}}),
			HasChanged:          true,
		}
		statements = append(statements, &ast.AlterTableNode{Name: tableDiff.TableName, Operations: []ast.AlterOperation{modify}})
	}
	return statements
}

// dropUnnamedPrimaryKey appends what a table that loses a key Oracle named
// needs ahead of its column changes: the NOT NULL a column held only through
// the key, then the key's drop. It reports whether the drop was written.
func (p *Planner) dropUnnamedPrimaryKey(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	tableDiff *difftypes.TableDiff,
	semantics identifier.Semantics,
) ([]ast.Node, bool) {
	result = append(result, p.statesNotNullBeforeKeyLoss(tableDiff, diff)...)
	if !p.losesUnnamedPrimaryKey(diff, tableDiff, semantics) {
		return result, false
	}
	return append(result, &ast.AlterTableNode{Name: tableDiff.TableName, Operations: []ast.AlterOperation{
		&ast.DropConstraintOperation{PrimaryKey: true},
	}}), true
}

// columnsWithoutKeyOwnedChanges returns the modified columns without what the
// removal of their key already does: the primary_key entry where the plan drops
// the key with a statement of its own, and the nullable entry of a column that
// is NOT NULL through the key alone and is declared nullable, which the removal
// makes so. MODIFY (id NULL) on it answers ORA-01451 once the key is gone. A
// column with nothing else to change is left out, and one with only a comment
// change stays for the comment.
func columnsWithoutKeyOwnedChanges(columns []difftypes.ColumnDiff, keyDropped bool) []difftypes.ColumnDiff {
	kept := make([]difftypes.ColumnDiff, 0, len(columns))
	for _, column := range columns {
		dropKey := keyDropped && losesPrimaryKey(column)
		dropNullable := keyOwnedNotNull(column) && column.Changes["nullable"] == "false -> true"
		if !dropKey && !dropNullable {
			kept = append(kept, column)
			continue
		}
		column.Changes = maps.Clone(column.Changes)
		if dropKey {
			delete(column.Changes, "primary_key")
		}
		if dropNullable {
			delete(column.Changes, "nullable")
		}
		if len(column.Changes) > 0 || column.CommentChange != nil {
			kept = append(kept, column)
		}
	}
	return kept
}

// primaryKeyRemovalsBeforeAdditions answers the removals of a table's primary
// key that go ahead of the additions, for a target that sets
// Grammar.Constraints.PrimaryKeyDroppedFirst: a table that gains a primary key
// in the plan and loses its old one under another name. Measured on Oracle Free
// 23.26, the ADD answers ORA-02260 while the old key stands, and the removal
// step runs after the additions.
func (p *Planner) primaryKeyRemovalsBeforeAdditions(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []difftypes.ConstraintRemovalInfo {
	if !primaryKeysDroppedFirst.Get(p.targetDialect()) {
		return nil
	}
	var removals []difftypes.ConstraintRemovalInfo
	for _, info := range diff.ConstraintsRemoved {
		if !strings.EqualFold(info.Type, "PRIMARY KEY") || info.TableName == "" {
			continue
		}
		replaced := slices.ContainsFunc(diff.ConstraintsAdded, func(add difftypes.ConstraintAdditionInfo) bool {
			return strings.EqualFold(add.Type, "PRIMARY KEY") && objectlookup.Same(add.TableName, info.TableName, semantics) &&
				!objectlookup.Contains(diff.TablesAdded.Names(), add.TableName, semantics)
		})
		if replaced {
			removals = append(removals, info)
		}
	}
	return removals
}
