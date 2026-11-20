// Package bufwriter provides the small line-oriented output buffer the dialect
// renderers share for accumulating generated SQL.
package bufwriter

import (
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
)

type Writer struct {
	output strings.Builder
}

// WriteLinef writes the formatted line and a newline. When format starts with
// `--` the line is a comment, and the text arguments it formats in are declared
// text that must not end it: each string, error and [fmt.Stringer] argument goes
// through [sqlutil.CommentText] first, so a line break in it is written as an
// escape, and a word a directive reader acts on is broken by
// [sqlutil.NeutralizeDirectives]. The format string itself is the renderer's own.
func (w *Writer) WriteLinef(format string, args ...any) {
	if strings.HasPrefix(format, "--") {
		args = commentSafeArgs(args)
	}
	line := fmt.Sprintf(format, args...)
	if strings.HasPrefix(format, "--") {
		line = sqlutil.NeutralizeDirectives(line)
	}
	w.WriteLine(line)
}

// commentSafeArgs returns args with each text argument made safe to sit in a
// `--` comment. The slice passed in is not changed.
func commentSafeArgs(args []any) []any {
	safe := make([]any, len(args))
	for i, arg := range args {
		switch typed := arg.(type) {
		case string:
			safe[i] = sqlutil.CommentText(typed)
		case error:
			safe[i] = commentSafeText(typed, typed.Error)
		case fmt.Stringer:
			safe[i] = commentSafeText(typed, typed.String)
		default:
			safe[i] = arg
		}
	}
	return safe
}

// commentSafeText returns the text of value made comment-safe. A nil pointer
// inside the interface is left for fmt to print as `<nil>`, as it does for any
// argument, rather than called, which would panic.
func commentSafeText(value any, text func() string) any {
	if reflected := reflect.ValueOf(value); reflected.Kind() == reflect.Pointer && reflected.IsNil() {
		return value
	}
	return sqlutil.CommentText(text())
}

// WriteLine writes s and a newline.
func (w *Writer) WriteLine(s string) {
	_, err := fmt.Fprint(&w.output, s+"\n")
	if err != nil {
		slog.Error("error writing line", "err", err)
	}
}

// Write writes a string to the output
func (w *Writer) Write(s string) {
	_, err := fmt.Fprint(&w.output, s)
	if err != nil {
		slog.Error("error writing line", "err", err)
	}
}

// Writef writes a formatted string to the output
func (w *Writer) Writef(format string, args ...any) {
	_, err := fmt.Fprintf(&w.output, format, args...)
	if err != nil {
		slog.Error("error writing line", "err", err)
	}
}

func (w *Writer) Output() string {
	return w.output.String()
}

// WriteAlterTable writes node with write, which renders operations as statements of their
// own. prefix is what starts each of them, `ALTER TABLE "t" `. Where node holds one
// statement's order and all of it fits one ALTER TABLE, it writes that. Otherwise it writes
// node's operations as they are: the order of one statement may hold only inside one
// statement, so it never reaches a file as several. So do a node whose two lists no longer
// say the same changes, because something rewrote one of them.
//
// caps is the capability set the text is rendered for, which decides how a string in it
// ends; see [JoinAlterTable].
func (w *Writer) WriteAlterTable(
	node *ast.AlterTableNode, prefix, dialect string, caps capability.Capabilities, write func([]ast.AlterOperation) error,
) error {
	if len(node.OneStatement) == 0 || !sameChanges(node) {
		return write(node.Operations)
	}
	start := w.output.Len()
	if err := write(node.OneStatement); err != nil {
		return err
	}
	joined, ok := JoinAlterTable(w.output.String()[start:], prefix, dialect, caps)
	if ok {
		w.replaceFrom(start, joined)
		return nil
	}
	w.replaceFrom(start, "")
	return write(node.Operations)
}

// sameChanges reports that OneStatement holds the operations of Operations, less a change
// of a column that a later one of that column replaces.
func sameChanges(node *ast.AlterTableNode) bool {
	for _, operation := range node.OneStatement {
		if !slices.Contains(node.Operations, operation) {
			return false
		}
	}
	for _, operation := range node.Operations {
		if slices.Contains(node.OneStatement, operation) {
			continue
		}
		replaced, ok := operation.(*ast.ModifyColumnOperation)
		if !ok || !slices.ContainsFunc(node.OneStatement, func(kept ast.AlterOperation) bool {
			modify, ok := kept.(*ast.ModifyColumnOperation)
			return ok && modify.Column.Name == replaced.Column.Name
		}) {
			return false
		}
	}
	return true
}

// replaceFrom puts text in place of what the writer took in after it held start bytes.
func (w *Writer) replaceFrom(start int, text string) {
	held := w.output.String()[:start]
	w.output.Reset()
	w.output.WriteString(held)
	w.output.WriteString(text)
}

func (w *Writer) Reset() {
	w.output.Reset()
}

// JoinAlterTable writes the statements of text, what one table's operations render,
// as one ALTER TABLE whose clauses are separated by commas. prefix is what starts each
// statement, `ALTER TABLE "t" `, and dialect and caps say how the text quotes: a
// literal rendered for a session whose sql_mode names NO_BACKSLASH_ESCAPES is read as
// that session reads it. A comment line stays ahead of the statement.
//
// It answers false for text holding a statement that does not start with prefix. Those
// statements cannot be one, so the caller writes the operations in the order that runs
// one by one rather than in the order it asked to join.
func JoinAlterTable(text, prefix, dialect string, caps capability.Capabilities) (string, bool) {
	var comments strings.Builder
	var clauses []string
	for _, statement := range sqlutil.SplitSQLStatementsForSession(text, dialect, caps) {
		for strings.HasPrefix(statement, "-- ") {
			line, rest, _ := strings.Cut(statement, "\n")
			comments.WriteString(line + "\n")
			statement = strings.TrimSpace(rest)
		}
		if statement == "" {
			continue
		}
		clause, ok := strings.CutPrefix(statement, prefix)
		if !ok {
			return "", false
		}
		clauses = append(clauses, clause)
	}
	if len(clauses) == 0 {
		return text, true
	}
	return comments.String() + prefix + strings.Join(clauses, ", ") + ";" + text[len(strings.TrimRight(text, "\n")):], true
}
