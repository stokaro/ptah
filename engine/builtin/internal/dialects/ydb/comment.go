package ydb

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/renderdiag"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbcomment"
)

// YQL has no COMMENT statement, so a comment is a user attribute of the table
// or view it belongs to, and a column's and an index's comment is an attribute
// of its table (see [ydbcomment]). The renderer writes each as Ptah's own
// COMMENT ON statement, which Ptah's YDB connection runs through the table
// service: a plan and a migration file stay text, and another client running
// the file gets YDB's parse error rather than a schema without its comments.
// A comment follows the statement that creates its object, because YDB sets
// an attribute only on an object that exists, and a change of attributes
// cannot share a request with another change (`Mixed alter is unsupported`).

// commentKeys name the capability that says a target keeps each object's
// comment.
var commentKeys = map[ydbcomment.Object]capability.Capability{
	ydbcomment.Table:  capability.CommentAttributes,
	ydbcomment.Column: capability.CommentAttributes,
	ydbcomment.Index:  capability.CommentAttributes,
	ydbcomment.View:   capability.ViewComments,
}

// commentStatement writes the COMMENT ON statement that gives the object
// statement names its comment, or removes the comment where it is empty. It
// refuses on a target without the key that keeps the object's comment, and a
// comment YDB cannot hold, naming subject.
func (r *Renderer) commentStatement(statement ydbcomment.Statement, subject string) (string, error) {
	if key := commentKeys[statement.Object]; !r.caps.Has(key) {
		return "", refuseKey(key, "the comment on "+subject)
	}
	if reason := ydbcomment.Refusal(statement.Object, statement.Name, statement.Comment); reason != "" {
		return "", refuseFact("the comment on "+subject, reason)
	}
	text, err := statement.Text()
	if err != nil {
		return "", refuseFact("the comment on "+subject, err.Error())
	}
	return text + ";", nil
}

// tableComments writes the comments a new table, its columns and its indexes
// carry, in that order, after the statements that create them. indexes are
// the indexes the CREATE TABLE writes, the unique ones its UNIQUE constraints
// render as included. YDB keeps at most [ydbcomment.MaxObjectBytes] of
// attributes on the table, so comments that take more are refused before any
// statement is written.
func (r *Renderer) tableComments(node *ast.CreateTableNode, indexes []*ast.IndexNode) ([]string, error) {
	path := ydbscheme.ObjectPath(node.Name)
	subject := tableref.Phrase(node.Name)
	comments := ydbcomment.Comments{Own: node.Comment}
	var statements []string
	add := func(statement ydbcomment.Statement, subject string) error {
		if statement.Comment == "" {
			return nil
		}
		text, err := r.commentStatement(statement, subject)
		if err != nil {
			return err
		}
		statements = append(statements, text)
		return nil
	}
	if err := add(ydbcomment.Statement{Object: ydbcomment.Table, Path: path, Comment: node.Comment}, subject); err != nil {
		return nil, err
	}
	for _, column := range node.Columns {
		if comments.Columns == nil {
			comments.Columns = make(map[string]string)
		}
		comments.Columns[column.Name] = column.Comment
		statement := ydbcomment.Statement{Object: ydbcomment.Column, Path: path, Name: column.Name, Comment: column.Comment}
		if err := add(statement, fmt.Sprintf("column %q of %s", column.Name, subject)); err != nil {
			return nil, err
		}
	}
	for _, index := range indexes {
		if comments.Indexes == nil {
			comments.Indexes = make(map[string]string)
		}
		comments.Indexes[index.Name] = index.Comment
		statement := ydbcomment.Statement{Object: ydbcomment.Index, Path: path, Name: index.Name, Comment: index.Comment}
		if err := add(statement, fmt.Sprintf("index %q of %s", index.Name, subject)); err != nil {
			return nil, err
		}
	}
	if reason := comments.SizeRefusal(subject); reason != "" {
		return nil, refuseFact(subject, reason)
	}
	return statements, nil
}

// setComment writes the statement a SetCommentOperation asks for: the table's
// own comment, or a column's.
func (r *Renderer) setComment(table string, op *ast.SetCommentOperation) ([]string, error) {
	statement := ydbcomment.Statement{Object: ydbcomment.Table, Path: ydbscheme.ObjectPath(table), Comment: op.Comment}
	subject := tableref.Phrase(table)
	if op.Column != "" {
		statement.Object = ydbcomment.Column
		statement.Name = op.Column
		subject = fmt.Sprintf("column %q of %s", op.Column, subject)
	}
	text, err := r.commentStatement(statement, subject)
	if err != nil {
		return nil, err
	}
	return []string{text}, nil
}

// objectCommentKeys name the capability that says a target keeps a comment of
// each kind an ObjectCommentNode names. A view's and an index's comment is a
// YDB attribute; every other kind is one YDB does not have.
var objectCommentKeys = map[ast.CommentedObject]capability.Capability{
	ast.CommentedView:             capability.ViewComments,
	ast.CommentedIndex:            capability.CommentAttributes,
	ast.CommentedSequence:         capability.SequenceComments,
	ast.CommentedDomain:           capability.DomainComments,
	ast.CommentedType:             capability.TypeComments,
	ast.CommentedExtension:        capability.ExtensionComments,
	ast.CommentedFunction:         capability.FunctionComments,
	ast.CommentedProcedure:        capability.ProcedureComments,
	ast.CommentedMaterializedView: capability.MaterializedViewComments,
	ast.CommentedTrigger:          capability.TriggerComments,
	ast.CommentedPolicy:           capability.PolicyComments,
}

// renderObjectComment writes the comment of a view or an index that already
// exists, and refuses every other kind by the key that names it.
func (r *Renderer) renderObjectComment(node *ast.ObjectCommentNode) error {
	subject := strings.ToLower(string(node.Object)) + " " + node.Name
	var statement ydbcomment.Statement
	switch node.Object {
	case ast.CommentedView:
		statement = ydbcomment.Statement{Object: ydbcomment.View, Path: ydbscheme.ObjectPath(node.Name), Comment: node.Comment}
	case ast.CommentedIndex:
		if strings.TrimSpace(node.Table) == "" {
			return refuseFact("the comment on "+subject, "YDB keeps an index's comment on its table, and the statement names none")
		}
		subject = fmt.Sprintf("index %q of %s", node.Name, tableref.Phrase(node.Table))
		statement = ydbcomment.Statement{Object: ydbcomment.Index, Path: ydbscheme.ObjectPath(node.Table), Name: node.Name, Comment: node.Comment}
	default:
		key, known := objectCommentKeys[node.Object]
		if !known {
			return refuseFact("COMMENT ON "+string(node.Object)+" "+node.Name, "the YDB renderer comments no such object")
		}
		return r.keyed(key, strings.ToLower(string(node.Object))+" comment", "COMMENT ON "+string(node.Object)+" "+node.Name)
	}
	text, err := r.commentStatement(statement, subject)
	if err != nil {
		return err
	}
	r.w.WriteLine(text)
	return nil
}

// recordSchemaComment records a schema's comment as left out: a Ptah schema is
// a YDB directory, and a directory holds no attribute (measured on 25.1.4.7
// and 26.2.1.14: AlterTable on one answers `PathNotTable`), which
// [capability.SchemaComments] records. The directory itself needs no
// statement, so the comment is said rather than refused, as a key's name is.
// A target claiming the key is refused, since this renderer writes no
// statement for it.
func (r *Renderer) recordSchemaComment(node *ast.CreateSchemaNode) error {
	switch {
	case node.Comment == "":
		return nil
	case r.caps.Has(capability.SchemaComments):
		return refuseUnwritten("schema comment", "the comment on schema "+node.Name)
	}
	r.sink.RecordLostComment(renderdiag.SchemaKind, node.Name, node.Comment)
	return nil
}
