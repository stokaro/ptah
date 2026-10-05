package ydb

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/migration/schemadiff/difftypes"
)

// A comment on YDB is a user attribute: a table's, a column's and an index's
// are attributes of the table, a view's of the view (internal/ydbcomment).
// YDB keeps an attribute when the column or the index it names is dropped or
// renamed, and drops it with the table or the view (measured on 25.1.4.7 and
// 26.2.1.14). So a plan writes a comment that changed, moves a renamed
// index's comment to its new key, and removes the comment of a column or an
// index it drops; an object the plan creates writes its own comment after the
// statement that creates it.

// tableComments writes the comment changes of one table the plan changes in
// place: its own comment, its columns' comments, and the removal of the
// comment of each column it drops, which YDB would otherwise keep under the
// column's name. A column the plan adds carries its comment, which the
// renderer writes after the ADD COLUMN.
func tableComments(tableDiff difftypes.TableDiff) []ast.AlterOperation {
	var operations []ast.AlterOperation
	if change := tableDiff.CommentChange; change != nil {
		operations = append(operations, &ast.SetCommentOperation{Comment: change.Desired, HasCurrent: change.Current != ""})
	}
	for _, colDiff := range tableDiff.ColumnsModified {
		if change := colDiff.CommentChange; change != nil {
			operations = append(operations, &ast.SetCommentOperation{
				Column: colDiff.ColumnName, Comment: change.Desired, HasCurrent: change.Current != "",
			})
		}
	}
	for _, column := range tableDiff.ColumnsRemoved {
		if column.Comment != "" {
			operations = append(operations, &ast.SetCommentOperation{Column: column.Name, HasCurrent: true})
		}
	}
	return operations
}

// indexComments writes the index comments the comparison recorded, after
// every index the plan adds, renames or drops is in place. A renamed index's
// comment is written under its new name and removed from its old one. An
// index of a table the plan drops or rebuilds is left out: its comments go
// with the table, or come with the new one. So is an index the plan adds with
// the comment it is to have, since the addition writes it; one it drops and
// adds again without a comment is not, because the comment the old index left
// is still on the table.
func indexComments(
	diff *difftypes.SchemaDiff,
	removedTables map[string]bool,
	rebuilds map[string]*tableRebuild,
	semantics identifier.Semantics,
) []ast.Node {
	added := make(map[difftypes.IndexRef]string, len(diff.IndexesAdded))
	for _, change := range diff.IndexesAdded {
		ref := difftypes.IndexRef{TableName: semantics.TableIdentityKey(change.TableName), Name: change.Index.Name}
		added[ref] = change.Index.Comment
	}
	var nodes []ast.Node
	for _, change := range diff.IndexCommentsChanged {
		key := semantics.TableIdentityKey(change.TableName)
		if _, rebuilt := rebuilds[key]; rebuilt || removedTables[key] {
			continue
		}
		if written, isAdded := added[difftypes.IndexRef{TableName: key, Name: change.Name}]; isAdded &&
			change.From == "" && change.Desired != "" && written == change.Desired {
			continue
		}
		if change.Desired != "" || change.From == "" {
			nodes = append(nodes, ast.NewObjectComment(ast.CommentedIndex, change.Name, change.Desired).SetTable(change.TableName))
		}
		if change.From != "" && change.Current != "" {
			nodes = append(nodes, ast.NewObjectComment(ast.CommentedIndex, change.From, "").SetTable(change.TableName))
		}
	}
	return nodes
}

// viewComments writes the comment of each view the plan keeps whose comment
// changed. A view the plan creates, or drops and creates again because its
// query changed, is a new object: its CREATE VIEW carries the declared
// comment, which the renderer writes after it, and it starts with none.
func viewComments(diff *difftypes.SchemaDiff) []ast.Node {
	created := make(map[string]bool, len(diff.ViewsAdded)+len(diff.ViewsModified))
	for _, view := range diff.ViewsAdded {
		created[view.Name] = true
	}
	for _, viewDiff := range diff.ViewsModified {
		created[viewDiff.ViewName] = true
		created[viewDiff.Desired.Name] = true
	}
	var nodes []ast.Node
	for _, change := range diff.ObjectCommentsChanged {
		if change.Kind != difftypes.CommentedView || created[change.Name] {
			continue
		}
		nodes = append(nodes, ast.NewObjectComment(ast.CommentedView, change.Name, change.Desired))
	}
	return nodes
}

// commentKinds name the key each kind of object comment a comparison reports
// needs. YDB keeps a view's comment; every other kind is an object YDB does
// not have, and its key is false on every YDB line.
var commentKinds = map[difftypes.CommentedObjectKind]capability.Capability{
	difftypes.CommentedView:          capability.ViewComments,
	difftypes.CommentedSequence:      capability.SequenceComments,
	difftypes.CommentedDomain:        capability.DomainComments,
	difftypes.CommentedCompositeType: capability.TypeComments,
	difftypes.CommentedRangeType:     capability.TypeComments,
	difftypes.CommentedEnumType:      capability.TypeComments,
	difftypes.CommentedExtension:     capability.ExtensionComments,
	difftypes.CommentedFunction:      capability.FunctionComments,
	difftypes.CommentedProcedure:     capability.ProcedureComments,
	difftypes.CommentedMatView:       capability.MaterializedViewComments,
	difftypes.CommentedTrigger:       capability.TriggerComments,
	difftypes.CommentedPolicy:        capability.PolicyComments,
}

// refuseComments refuses, before anything is planned, a comment change the
// target cannot keep: a table's, a column's or an index's without
// [capability.CommentAttributes], a view's without [capability.ViewComments],
// a constraint's, which YDB does not name, and any other object's by its key.
func (p *Planner) refuseComments(diff *difftypes.SchemaDiff) error {
	if !p.caps.Has(capability.CommentAttributes) {
		for _, tableDiff := range diff.TablesModified {
			if len(tableComments(tableDiff)) > 0 {
				return refuseKey(capability.CommentAttributes, fmt.Sprintf("changing the comments of table %q", tableDiff.TableName))
			}
		}
		if len(diff.IndexCommentsChanged) > 0 {
			change := diff.IndexCommentsChanged[0]
			return refuseKey(capability.CommentAttributes, fmt.Sprintf("changing the comment of index %q of table %q",
				change.Name, change.TableName))
		}
	}
	if len(diff.ConstraintCommentsChanged) > 0 {
		change := diff.ConstraintCommentsChanged[0]
		return p.keyed(capability.ConstraintComments, "constraint comment",
			fmt.Sprintf("changing the comment of constraint %q of table %q", change.Name, change.TableName))
	}
	for _, change := range diff.ObjectCommentsChanged {
		key, known := commentKinds[change.Kind]
		subject := fmt.Sprintf("changing the comment of %s %s", change.Kind, change.Name)
		switch {
		case !known:
			return refuseFact(subject, "the YDB planner comments no such object")
		case key != capability.ViewComments:
			return p.keyed(key, string(change.Kind)+" comment", subject)
		case !p.caps.Has(key):
			return refuseKey(key, subject)
		}
	}
	return nil
}

// withoutPlannedComments is diff without the comment changes the planner
// writes, for the check that refuses a family nobody named.
func withoutPlannedComments(diff *difftypes.SchemaDiff) []difftypes.ObjectCommentChange {
	return slices.DeleteFunc(slices.Clone(diff.ObjectCommentsChanged), func(change difftypes.ObjectCommentChange) bool {
		return change.Kind == difftypes.CommentedView
	})
}
