// Package bufwriter provides the small line-oriented output buffer the dialect
// renderers share for accumulating generated SQL.
package bufwriter

import (
	"fmt"
	"log/slog"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/sqlutil"
)

type Writer struct {
	output strings.Builder
}

func (w *Writer) WriteLinef(format string, args ...any) {
	_, err := fmt.Fprintf(&w.output, format+"\n", args...)
	if err != nil {
		slog.Error("error writing line", "err", err)
	}
}

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
// statement, so it never reaches a file as several.
func (w *Writer) WriteAlterTable(node *ast.AlterTableNode, prefix, dialect string, write func([]ast.AlterOperation) error) error {
	if len(node.OneStatement) == 0 {
		return write(node.Operations)
	}
	start := w.output.Len()
	if err := write(node.OneStatement); err != nil {
		return err
	}
	joined, ok := JoinAlterTable(w.output.String()[start:], prefix, dialect)
	if ok {
		w.replaceFrom(start, joined)
		return nil
	}
	w.replaceFrom(start, "")
	return write(node.Operations)
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
// statement, `ALTER TABLE "t" `, and dialect says how the text quotes. A comment line
// stays ahead of the statement.
//
// It answers false for text holding a statement that does not start with prefix. Those
// statements cannot be one, so the caller writes the operations in the order that runs
// one by one rather than in the order it asked to join.
func JoinAlterTable(text, prefix, dialect string) (string, bool) {
	var comments strings.Builder
	var clauses []string
	for _, statement := range sqlutil.SplitSQLStatementsForDialect(text, dialect) {
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
