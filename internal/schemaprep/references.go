// Package schemaprep contains transformations and derived values that stay in
// the desired-schema model. It deliberately does not depend on core/ast.
package schemaprep

import (
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
)

// ForeignKeyReference is the model-side interpretation of a field's foreign
// reference. AST adapters may copy it into their own node type at the boundary.
type ForeignKeyReference struct {
	Table  string
	Column string
	// Columns carries a composite reference when the source syntax supplies
	// one. Empty means Column is authoritative.
	Columns []string
}

// ReferencedColumns returns Columns, or the legacy single Column as a slice.
func (r *ForeignKeyReference) ReferencedColumns() []string {
	if r == nil {
		return nil
	}
	if len(r.Columns) > 0 {
		return r.Columns
	}
	if r.Column == "" {
		return nil
	}
	return []string{r.Column}
}

// ParseForeignKeyReference parses "table(column)" or the shorthand "table",
// whose referenced column is "id". It returns nil for an empty or malformed
// parenthesized reference.
func ParseForeignKeyReference(foreign string) *ForeignKeyReference {
	foreign = strings.TrimSpace(foreign)
	if foreign == "" {
		return nil
	}

	if strings.ContainsAny(foreign, "()") {
		open := strings.IndexByte(foreign, '(')
		close := strings.LastIndexByte(foreign, ')')
		if open <= 0 || close != len(foreign)-1 ||
			strings.Count(foreign, "(") != 1 || strings.Count(foreign, ")") != 1 {
			return nil
		}
		table := strings.TrimSpace(foreign[:open])
		column := strings.TrimSpace(foreign[open+1 : close])
		if table == "" || column == "" {
			return nil
		}
		return &ForeignKeyReference{
			Table:  table,
			Column: column,
		}
	}
	return &ForeignKeyReference{
		Table:  foreign,
		Column: "id",
	}
}

// GenerateForeignKeyName returns the conventional field foreign-key name.
func GenerateForeignKeyName(tableName, fieldName string) string {
	return "fk_" + strings.ToLower(tableName) + "_" + strings.ToLower(fieldName)
}

// EnumsFor returns the declared enums named by the supplied fields, preserving
// declaration order and returning each enum at most once.
func EnumsFor(fields []schemamodel.Field, enums []schemamodel.Enum) []schemamodel.Enum {
	var needed []schemamodel.Enum
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		enum := declaredEnum(field.Type, enums)
		if enum == nil || seen[enum.Name] {
			continue
		}
		seen[enum.Name] = true
		needed = append(needed, *enum)
	}
	return needed
}

func declaredEnum(fieldType string, enums []schemamodel.Enum) *schemamodel.Enum {
	for i := range enums {
		if enums[i].Name == fieldType {
			return &enums[i]
		}
	}
	return nil
}
