package dbmlrender

import (
	"fmt"
	"sort"
	"strings"

	"ptah.run/core/schemamodel"
)

// references renders one Ref per foreign key, sorted by the line itself so the
// order is a property of the content rather than of the schema's field order.
//
// A key is carried either by its column, as `Foreign`, or by the table, as a
// FOREIGN KEY constraint: a composite key always, and a key over one column
// when that column carries another one already. Both are written, so a table
// with two keys over one column keeps both (stokaro/ptah#3917).
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
		line, ok := columnReference(table, field)
		if !ok {
			continue
		}
		lines = append(lines, line)
	}
	for _, table := range tables {
		for _, constraint := range b.constraintsOf(table, constraintForeignKey) {
			line, ok := constraintReference(table, constraint)
			if !ok {
				continue
			}
			lines = append(lines, line)
		}
	}
	sort.Strings(lines)
	return lines
}

// columnReference renders the `Ref` a column's own foreign key declares, and
// reports whether the declaration named a target it could read.
//
// A foreign key is written `table(column)`, optionally schema-qualified. A
// declaration this cannot parse is skipped rather than guessed at: emitting a
// Ref to a target nobody named would put a relationship in the diagram that the
// database does not have.
func columnReference(table schemamodel.Table, field schemamodel.Field) (string, bool) {
	targetTable, targetColumn, ok := parseForeign(field.Foreign)
	if !ok {
		return "", false
	}
	targetSchema, targetTable := splitQualified(targetTable)
	return referenceLine(
		field.ForeignKeyName,
		qualified(table.Schema, table.Name), []string{field.Name},
		qualified(targetSchema, targetTable), []string{targetColumn},
		field.OnDelete, field.OnUpdate,
	), true
}

// constraintReference renders the `Ref` a FOREIGN KEY constraint declares, and
// reports whether it names columns on both sides, in equal number.
func constraintReference(table schemamodel.Table, constraint schemamodel.Constraint) (string, bool) {
	targetColumns := constraint.ForeignColumnsOrDefault()
	targetTable := strings.TrimSpace(constraint.ForeignTable)
	if len(constraint.Columns) == 0 || len(targetColumns) != len(constraint.Columns) || targetTable == "" {
		return "", false
	}
	targetSchema, targetTable := splitQualified(targetTable)
	return referenceLine(
		constraint.Name,
		qualified(table.Schema, table.Name), constraint.Columns,
		qualified(targetSchema, targetTable), targetColumns,
		constraint.OnDelete, constraint.OnUpdate,
	), true
}

// referenceLine writes one `Ref`. One column on each side is written
// `table.column`; more are written `table.(a, b)`, DBML's composite form.
func referenceLine(name, from string, fromColumns []string, to string, toColumns []string, onDelete, onUpdate string) string {
	label := ""
	if name != "" {
		label = " " + quote(name) + ":"
	}
	line := fmt.Sprintf("Ref%s %s.%s > %s.%s", label, from, endpointColumns(fromColumns), to, endpointColumns(toColumns))

	settings := make([]string, 0, 2)
	if action := strings.ToLower(strings.TrimSpace(onDelete)); action != "" {
		settings = append(settings, "delete: "+action)
	}
	if action := strings.ToLower(strings.TrimSpace(onUpdate)); action != "" {
		settings = append(settings, "update: "+action)
	}
	if len(settings) == 0 {
		return line
	}
	return line + " [" + strings.Join(settings, ", ") + "]"
}

func endpointColumns(columns []string) string {
	if len(columns) == 1 {
		return quote(columns[0])
	}
	return columnList(columns)
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
