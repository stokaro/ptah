package dbmlrender

import (
	"fmt"
	"sort"
	"strings"

	"ptah.run/core/schemamodel"
)

// references renders one Ref per foreign key, sorted by the line itself so the
// order is a property of the content rather than of the schema's field order.
func (b *builder) references() []string {
	tables := b.selected()
	byStruct := make(map[string]schemamodel.Table, len(tables))
	for _, table := range tables {
		byStruct[table.StructName] = table
	}

	lines := make([]string, 0, 4)
	for _, field := range b.db.Fields {
		table, known := byStruct[field.StructName]
		if !known || field.Foreign == "" {
			continue
		}
		line, ok := reference(table, field)
		if !ok {
			continue
		}
		lines = append(lines, line)
	}
	sort.Strings(lines)
	return lines
}

// reference renders one `Ref`, and reports whether the declaration named a
// target it could read.
//
// A foreign key is written `table(column)`, optionally schema-qualified. A
// declaration this cannot parse is skipped rather than guessed at: emitting a
// Ref to a target nobody named would put a relationship in the diagram that the
// database does not have.
func reference(table schemamodel.Table, field schemamodel.Field) (string, bool) {
	targetTable, targetColumn, ok := parseForeign(field.Foreign)
	if !ok {
		return "", false
	}
	targetSchema, targetTable := splitQualified(targetTable)
	name := ""
	if field.ForeignKeyName != "" {
		name = " " + quote(field.ForeignKeyName) + ":"
	}
	line := fmt.Sprintf("Ref%s %s.%s > %s.%s",
		name,
		qualified(table.Schema, table.Name), quote(field.Name),
		qualified(targetSchema, targetTable), quote(targetColumn))

	settings := make([]string, 0, 2)
	if action := strings.ToLower(strings.TrimSpace(field.OnDelete)); action != "" {
		settings = append(settings, "delete: "+action)
	}
	if action := strings.ToLower(strings.TrimSpace(field.OnUpdate)); action != "" {
		settings = append(settings, "update: "+action)
	}
	if len(settings) == 0 {
		return line, true
	}
	return line + " [" + strings.Join(settings, ", ") + "]", true
}

// parseForeign reads the `table(column)` spelling a foreign key declaration
// uses.
func parseForeign(declaration string) (table, column string, ok bool) {
	trimmed := strings.TrimSpace(declaration)
	open := strings.Index(trimmed, "(")
	if open <= 0 || !strings.HasSuffix(trimmed, ")") {
		return "", "", false
	}
	table = strings.TrimSpace(trimmed[:open])
	column = strings.TrimSpace(trimmed[open+1 : len(trimmed)-1])
	if table == "" || column == "" {
		return "", "", false
	}
	return table, column, true
}

// splitQualified separates a `schema.table` spelling, leaving a bare name in
// the default schema.
func splitQualified(name string) (schema, table string) {
	dot := strings.Index(name, ".")
	if dot <= 0 {
		return "", name
	}
	return name[:dot], name[dot+1:]
}
