package atlasschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// escapedLiterals names each string literal the plan's nodes at indexes render
// from the model that holds a backslash: a column's literal default, a column,
// table or index comment, and an enum or set value in a column's type. These
// are the literals the MySQL-family renderer writes for the default sql_mode,
// where `'a\\b'` holds one backslash. A body taken from the user's own SQL --
// a view, a routine, a trigger, a CHECK or a default expression -- is not one
// of them: it is sent as the user wrote it.
//
// A backslash cannot stand outside a string literal in a MySQL-family column
// type, so a type that holds one holds it in an enum or set value.
func escapedLiterals(nodes []ast.Node, indexes []int) []string {
	var found []string
	note := func(subject, text string) {
		if strings.Contains(text, `\`) && !slices.Contains(found, subject) {
			found = append(found, subject)
		}
	}
	column := func(table string, column *ast.ColumnNode) {
		if column == nil {
			return
		}
		name := fmt.Sprintf("column %s.%s", table, column.Name)
		if column.Default.HasLiteral() {
			note("the default of "+name, column.Default.Value)
		}
		note("the comment of "+name, column.Comment)
		note("the type of "+name, column.Type)
	}
	index := func(index *ast.IndexNode) {
		if index != nil {
			note(fmt.Sprintf("the comment of index %s on %s", index.Name, index.Table), index.Comment)
		}
	}
	for _, i := range indexes {
		switch node := nodes[i].(type) {
		case *ast.CreateTableNode:
			for _, c := range node.Columns {
				column(node.Name, c)
			}
			for _, x := range node.Indexes {
				index(x)
			}
			note("the comment of table "+node.Name, node.Comment)
		case *ast.AlterTableNode:
			for _, operation := range slices.Concat(node.Operations, node.OneStatement) {
				switch op := operation.(type) {
				case *ast.AddColumnOperation:
					column(node.Name, op.Column)
				case *ast.ModifyColumnOperation:
					column(node.Name, op.Column)
				case *ast.SetCommentOperation:
					note(commentSubject(node.Name, op.Column), op.Comment)
				case *ast.AddIndexOperation:
					index(op.Index)
				case *ast.ReplaceIndexOperation:
					index(op.Index)
				}
			}
		case *ast.IndexNode:
			index(node)
		}
	}
	return found
}

// commentSubject names what a comment operation on table sets the comment of.
func commentSubject(table, column string) string {
	if column == "" {
		return "the comment of table " + table
	}
	return fmt.Sprintf("the comment of column %s.%s", table, column)
}

// refuseEscapedLiterals refuses a plan that renders a backslash in a string
// literal, for a session whose sql_mode names NO_BACKSLASH_ESCAPES. Ptah
// writes the literal for the default sql_mode, so the session would read each
// doubled backslash as two characters and store what the model does not
// declare. SQL a user edited, and a saved plan file, are sent as written and
// are not refused.
func (p ApplyPlan) refuseEscapedLiterals() error {
	if !p.capabilities.Has(capability.NoBackslashEscapes) || len(p.escaped) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: the session's sql_mode names NO_BACKSLASH_ESCAPES, and the plan writes a backslash in %s; "+
			"Ptah writes a string literal for the default sql_mode, so the server would store each backslash twice. "+
			"Apply on a session without NO_BACKSLASH_ESCAPES, or write the literal yourself with --edit",
		ptaherr.ErrUnsupportedFeature, strings.Join(p.escaped, ", "),
	)
}
