// Package notnullname keeps a migration plan from giving a NOT NULL constraint a
// name another column of the table still holds.
//
// PostgreSQL 18 keeps a NOT NULL as a named constraint, and RENAME COLUMN leaves
// the name as it was. So display_name can hold t_full_name_not_null in one
// database while full_name holds it in another, and the plan between them adds
// display_name under a name full_name keeps until the plan's removals run. The
// server answers `constraint "t_full_name_not_null" for relation "t" already
// exists`. A column whose constraint the plan renames holds its name in the same
// way until that rename runs.
//
// The plan keeps its order, because a removal goes last for a reason: the views,
// routines and policies that read a removed column move to what the plan adds
// first, and with DROP COLUMN ahead of them the server refuses the drop. [Free]
// moves only the name, with a RENAME CONSTRAINT ahead of the statement that wants
// it. Nothing depends on a constraint's name, and the rename is the one way out
// that holds for every column: on PostgreSQL 18.6 DROP CONSTRAINT and DROP NOT
// NULL are refused on a column in a primary key, on an identity column and on a
// column in the index of a replica identity, while RENAME CONSTRAINT takes all
// three and reaches inheriting tables and partitions as well.
//
// A statement that lists the holder's drop ahead of the addition taking its name
// needs none of this. PostgreSQL runs the drops of one ALTER TABLE ahead of its
// additions, which is how a plan [ptah.run/core/planext.StatementShapeCatalog]
// merges already runs, and written one by one the drop still goes first. [Free]
// therefore reads a plan after its statements have their final grouping.
//
// A target that keeps no such name is left alone without asking its capabilities:
// its reader reports no name to hold, and its renderer refuses a declared name on
// an added column.
package notnullname

import (
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/internal/pgname"
)

// Free returns nodes with a RENAME CONSTRAINT ahead of each statement that takes
// a NOT NULL name a removed column, or a column whose constraint the plan renames,
// still holds at that point. A later rename of the moved constraint starts from
// its interim name, which it changes in place. Nodes that need none of this come
// back as they are.
func Free(nodes []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	tables := heldNames(diff)
	if len(tables) == 0 {
		return nodes
	}
	out := make([]ast.Node, 0, len(nodes))
	for _, node := range nodes {
		if alter, ok := node.(*ast.AlterTableNode); ok {
			for _, name := range tables[alter.Name].freedFor(alter) {
				out = append(out, &ast.AlterTableNode{
					Name:       alter.Name,
					Operations: []ast.AlterOperation{&ast.RenameConstraintOperation{From: name, To: pgname.Interim(name)}},
				})
			}
		}
		out = append(out, node)
	}
	return out
}

// Unfree takes back what [Free] did to nodes, for a pass that moves their
// statements and then asks [Free] again: a plan that merges a column's drop with
// the addition taking its name has no need for the rename.
func Unfree(nodes []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	tables := heldNames(diff)
	if len(tables) == 0 {
		return nodes
	}
	out := make([]ast.Node, 0, len(nodes))
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok || tables[alter.Name] == nil || len(alter.Operations) != 1 {
			out = append(out, node)
			continue
		}
		rename, ok := alter.Operations[0].(*ast.RenameConstraintOperation)
		if !ok {
			out = append(out, node)
			continue
		}
		if tables[alter.Name].held[rename.From] && rename.To == pgname.Interim(rename.From) {
			continue
		}
		for name := range tables[alter.Name].held {
			if rename.From == pgname.Interim(name) {
				rename.From = name
			}
		}
		out = append(out, node)
	}
	return out
}

// holders is who holds a name one table's plan gives away.
type holders struct {
	// removed is the name each removed column holds.
	removed map[string]string
	// held is the names a later statement of the plan frees.
	held map[string]bool
	// moved is the names [Free] has moved to their interim name.
	moved map[string]bool
}

// heldNames reads from diff, by table as its statements name it, the names that
// leave their holder during the plan. A table that gives none away has no entry.
func heldNames(diff *difftypes.SchemaDiff) map[string]*holders {
	if diff == nil {
		return nil
	}
	tables := make(map[string]*holders)
	for _, table := range diff.TablesModified {
		found := holders{
			removed: make(map[string]string),
			held:    make(map[string]bool),
			moved:   make(map[string]bool),
		}
		for _, column := range table.ColumnsRemoved {
			if column.NotNullConstraintName != "" {
				found.removed[column.Name] = column.NotNullConstraintName
				found.held[column.NotNullConstraintName] = true
			}
		}
		for _, column := range table.ColumnsModified {
			if change := column.NotNullConstraintNameChange; change != nil && change.Current != "" {
				found.held[change.Current] = true
			}
		}
		if len(found.held) > 0 {
			tables[table.TableName] = &found
		}
	}
	return tables
}

// freedFor advances the table's state over one statement and answers the names
// that have to move before it runs. The operations are read in the order they run
// when each is a statement of its own, and a statement that merges them runs its
// drops first, so a drop listed ahead of the addition taking its name is safe in
// both forms.
func (h *holders) freedFor(alter *ast.AlterTableNode) []string {
	if h == nil {
		return nil
	}
	var freed []string
	take := func(name string) {
		if h.held[name] {
			freed = append(freed, name)
			delete(h.held, name)
			h.moved[name] = true
		}
	}
	for _, operation := range alter.Operations {
		switch op := operation.(type) {
		case *ast.DropColumnOperation:
			delete(h.held, h.removed[op.ColumnName])
		case *ast.AddColumnOperation:
			if op.Column != nil {
				take(op.Column.NotNullConstraintName)
			}
		case *ast.ModifyColumnOperation:
			take(op.NotNullConstraintName)
		case *ast.RenameConstraintOperation:
			take(op.To)
			delete(h.held, op.From)
			if h.moved[op.From] {
				op.From = pgname.Interim(op.From)
			}
		}
	}
	return freed
}
