package schemaprep

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/tableref"
)

// TableCheckConstraints returns the model constraints declared by a table's
// checks list. An explicit CHECK with the same expression supersedes the list
// entry, and explicit constraint names reserve their table-local namespace.
func TableCheckConstraints(table schemamodel.Table, declared []schemamodel.Constraint) []schemamodel.Constraint {
	if len(table.Checks) == 0 {
		return nil
	}
	spelled := declaredCheckExpressions(table, declared)
	taken := declaredConstraintNames(table, declared)
	constraints := make([]schemamodel.Constraint, 0, len(table.Checks))
	ordinal := 0
	for _, check := range table.Checks {
		expression := strings.TrimSpace(check)
		if expression == "" {
			continue
		}
		if _, superseded := spelled[expression]; superseded {
			continue
		}
		name := tableCheckConstraintName(table.Name, ordinal)
		for {
			if _, used := taken[name]; !used {
				break
			}
			ordinal++
			name = tableCheckConstraintName(table.Name, ordinal)
		}
		taken[name] = struct{}{}
		ordinal++
		constraints = append(constraints, schemamodel.Constraint{
			StructName:      table.StructName,
			Name:            name,
			Type:            "CHECK",
			Table:           table.QualifiedName(),
			CheckExpression: expression,
		})
	}
	return constraints
}

func declaredConstraintNames(table schemamodel.Table, declared []schemamodel.Constraint) map[string]struct{} {
	taken := make(map[string]struct{}, len(declared))
	for _, constraint := range declared {
		if !constraintBelongsToTable(constraint, table) {
			continue
		}
		if name := strings.TrimSpace(constraint.Name); name != "" {
			taken[name] = struct{}{}
		}
	}
	return taken
}

func declaredCheckExpressions(table schemamodel.Table, declared []schemamodel.Constraint) map[string]struct{} {
	spelled := make(map[string]struct{}, len(declared))
	for _, constraint := range declared {
		if strings.EqualFold(constraint.Type, "CHECK") && constraintBelongsToTable(constraint, table) {
			spelled[strings.TrimSpace(constraint.CheckExpression)] = struct{}{}
		}
	}
	return spelled
}

func tableCheckConstraintName(tableName string, ordinal int) string {
	leaf := tableName
	if ref, ok := tableref.Parse(tableName); ok {
		leaf = ref.Name
	}
	if ordinal == 0 {
		return leaf + "_check"
	}
	return fmt.Sprintf("%s_check%d", leaf, ordinal)
}

func constraintBelongsToTable(constraint schemamodel.Constraint, table schemamodel.Table) bool {
	if constraint.Table != "" {
		return constraint.Table == table.Name || constraint.Table == table.QualifiedName()
	}
	return constraint.StructName == table.StructName
}

func isForeignKeyConstraint(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "FOREIGN KEY")
}
