package atlasschema

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/core/renderer"
)

// Annotations returns an independent snapshot of non-executable plan notes.
func (p ApplyPlan) Annotations() []renderer.Annotation { return slices.Clone(p.annotations) }

// Annotations returns notes positioned among the prepared execution units.
func (p ApplyRuntimePlan) Annotations() []renderer.Annotation { return p.plan.Annotations() }

func (c applyComputation) executionAnnotations() []renderer.Annotation {
	result := c.Annotations()
	index := c.dataIndex()
	for i := range result {
		if result[i].Before >= index {
			result[i].Before += len(c.dataStatements)
		}
	}
	return result
}

// annotatedSQL is a display projection. Execution uses the captured statement
// bytes, never this text. Schema fingerprints describe schema state and do not
// include annotations; the saved document digest includes every annotation.
func annotatedSQL(statements []string, annotations []renderer.Annotation) string {
	var out strings.Builder
	next := 0
	for i := 0; i <= len(statements); i++ {
		for next < len(annotations) && annotations[next].Before == i {
			out.WriteString(renderer.Comment(annotations[next].Text))
			next++
		}
		if i < len(statements) {
			out.WriteString(FormatMigrationSQL(statements[i : i+1]))
		}
	}
	return out.String()
}

// WriteUnitReports lists notes and SQL or native summaries in plan order.
// Native payloads are never exposed as text that could be executed as SQL.
func WriteUnitReports(out io.Writer, units []ApplyUnitReport, annotations []renderer.Annotation) {
	next := 0
	for i := 0; i <= len(units); i++ {
		for next < len(annotations) && annotations[next].Before == i {
			fmt.Fprint(out, renderer.Comment(annotations[next].Text))
			next++
		}
		if i == len(units) {
			break
		}
		unit := units[i]
		for _, input := range unit.Inputs {
			fmt.Fprintf(out, "Execution input: %s %s\n", input.Source, input.Name)
		}
		if unit.Native == nil {
			fmt.Fprintln(out, unit.SQL)
			continue
		}
		native := unit.Native
		fmt.Fprintf(out, "Native operation: %s\n  %s: %s (%s)\n", native.Kind, native.Subject, native.Reason, native.Severity)
		if native.Access != "" {
			fmt.Fprintf(out, "  Access %s: %s\n", native.Access, native.AccessReason)
		}
	}
}
