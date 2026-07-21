package mssql

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
)

func (r *Renderer) VisitUpsert(node *ast.UpsertNode) error {
	if err := validateUpsert(node); err != nil {
		return err
	}
	if comment := renderUpsertComment(node.Comment); comment != "" {
		r.w.WriteLinef("-- %s", comment)
	}

	r.w.WriteLinef("MERGE INTO %s WITH (HOLDLOCK) AS target", escapeQualifiedIdentifier(node.Table))
	r.w.WriteLinef(
		"USING (VALUES (%s)) AS source (%s)",
		strings.Join(trimmedList(node.Values), ", "),
		strings.Join(escapeIdentifierList(node.InsertColumns), ", "),
	)
	r.w.WriteLinef("ON %s", renderUpsertMatch(node))
	r.w.WriteLinef("WHEN MATCHED%s THEN", renderUpsertPredicate(node.UpdatePredicate))
	r.w.WriteLinef("    UPDATE SET %s", strings.Join(renderUpsertAssignments(node.UpdateAssignments), ", "))
	r.w.WriteLinef("WHEN NOT MATCHED%s THEN", renderUpsertPredicate(node.InsertPredicate))
	r.w.WriteLinef("    INSERT (%s)", strings.Join(escapeIdentifierList(node.InsertColumns), ", "))
	r.w.WriteLinef("    VALUES (%s);", strings.Join(renderUpsertSourceValues(node.InsertColumns), ", "))
	return nil
}

func validateUpsert(node *ast.UpsertNode) error {
	if node == nil {
		return fmt.Errorf("upsert node is nil")
	}
	if strings.TrimSpace(node.Table) == "" {
		return fmt.Errorf("upsert table is required")
	}
	if len(node.InsertColumns) == 0 {
		return fmt.Errorf("upsert insert columns are required")
	}
	if len(node.InsertColumns) != len(node.Values) {
		return fmt.Errorf("upsert insert columns and values length mismatch: %d columns, %d values",
			len(node.InsertColumns), len(node.Values))
	}
	if len(node.MatchColumns) == 0 {
		return fmt.Errorf("upsert match columns are required")
	}
	if len(node.UpdateAssignments) == 0 {
		return fmt.Errorf("upsert update assignments are required")
	}
	if err := validateUpsertColumns(node); err != nil {
		return err
	}
	return validateUpsertAssignments(node.UpdateAssignments)
}

func validateUpsertColumns(node *ast.UpsertNode) error {
	insertColumns := make(map[string]struct{}, len(node.InsertColumns))
	for _, column := range node.InsertColumns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("upsert insert column is empty")
		}
		normalized := normalizedUpsertIdentifier(column)
		if _, ok := insertColumns[normalized]; ok {
			return fmt.Errorf("upsert insert column %q is duplicated", column)
		}
		insertColumns[normalized] = struct{}{}
	}
	for _, value := range node.Values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("upsert value expression is empty")
		}
	}
	matchColumns := make(map[string]struct{}, len(node.MatchColumns))
	for _, column := range node.MatchColumns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("upsert match column is empty")
		}
		normalized := normalizedUpsertIdentifier(column)
		if _, ok := matchColumns[normalized]; ok {
			return fmt.Errorf("upsert match column %q is duplicated", column)
		}
		matchColumns[normalized] = struct{}{}
		if _, ok := insertColumns[normalized]; !ok {
			return fmt.Errorf("upsert match column %q must also be an insert column", column)
		}
	}
	return nil
}

func validateUpsertAssignments(assignments []ast.UpsertAssignment) error {
	updateColumns := make(map[string]struct{}, len(assignments))
	for _, assignment := range assignments {
		if strings.TrimSpace(assignment.Column) == "" {
			return fmt.Errorf("upsert update assignment column is empty")
		}
		normalized := normalizedUpsertIdentifier(assignment.Column)
		if _, ok := updateColumns[normalized]; ok {
			return fmt.Errorf("upsert update assignment column %q is duplicated", assignment.Column)
		}
		updateColumns[normalized] = struct{}{}
		if strings.TrimSpace(assignment.Expression) == "" {
			return fmt.Errorf("upsert update assignment expression is empty")
		}
	}
	return nil
}

func renderUpsertMatch(node *ast.UpsertNode) string {
	parts := make([]string, 0, len(node.MatchColumns)+1)
	for _, column := range node.MatchColumns {
		escaped := escapeIdentifier(column)
		parts = append(parts, "target."+escaped+" = source."+escaped)
	}
	return strings.Join(parts, " AND ")
}

func renderUpsertPredicate(predicate string) string {
	if trimmed := strings.TrimSpace(predicate); trimmed != "" {
		return " AND (" + trimmed + ")"
	}
	return ""
}

func renderUpsertComment(comment string) string {
	withoutLineBreaks := strings.NewReplacer("\r", " ", "\n", " ").Replace(comment)
	return strings.Join(strings.Fields(withoutLineBreaks), " ")
}

func normalizedUpsertIdentifier(identifier string) string {
	return unquoteIdentifier(strings.TrimSpace(identifier))
}

func renderUpsertAssignments(assignments []ast.UpsertAssignment) []string {
	rendered := make([]string, len(assignments))
	for i, assignment := range assignments {
		rendered[i] = escapeIdentifier(assignment.Column) + " = " + strings.TrimSpace(assignment.Expression)
	}
	return rendered
}

func renderUpsertSourceValues(columns []string) []string {
	values := make([]string, len(columns))
	for i, column := range columns {
		values[i] = "source." + escapeIdentifier(column)
	}
	return values
}

func trimmedList(values []string) []string {
	trimmed := make([]string, len(values))
	for i, value := range values {
		trimmed[i] = strings.TrimSpace(value)
	}
	return trimmed
}
