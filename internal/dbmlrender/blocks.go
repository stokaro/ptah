package dbmlrender

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
)

// enums renders one block per enum type, sorted by (schema, name).
func (b *builder) enums() []string {
	enums := slices.Clone(b.db.Enums)
	sort.Slice(enums, func(i, j int) bool {
		if enums[i].Schema != enums[j].Schema {
			return enums[i].Schema < enums[j].Schema
		}
		return enums[i].Name < enums[j].Name
	})
	blocks := make([]string, 0, len(enums))
	for _, enum := range enums {
		var out strings.Builder
		fmt.Fprintf(&out, "Enum %s {\n", qualified(enum.Schema, enum.Name))
		for _, value := range enum.Values {
			fmt.Fprintf(&out, "  %s\n", quote(value))
		}
		out.WriteString("}")
		blocks = append(blocks, out.String())
	}
	return blocks
}

// tables renders one block per table, sorted by (schema, name).
//
// Columns keep declaration order. The schema states it, and a renderer that
// sorted them would be rewriting the schema rather than writing it down.
func (b *builder) tables() []string {
	tables := b.selected()
	blocks := make([]string, 0, len(tables))
	for _, table := range tables {
		var out strings.Builder
		fmt.Fprintf(&out, "Table %s {\n", qualified(table.Schema, table.Name))
		for _, field := range b.fieldsOf(table) {
			fmt.Fprintf(&out, "  %s\n", column(field))
		}
		if indexes := b.indexesOf(table); len(indexes) > 0 {
			out.WriteString("\n  Indexes {\n")
			for _, line := range indexes {
				fmt.Fprintf(&out, "    %s\n", line)
			}
			out.WriteString("  }\n")
		}
		if table.Comment != "" {
			fmt.Fprintf(&out, "\n  Note: %s\n", quoteNote(table.Comment))
		}
		out.WriteString("}")
		blocks = append(blocks, out.String())
	}
	return blocks
}

// selected is the tables this render covers, sorted by identity.
func (b *builder) selected() []schemamodel.Table {
	tables := make([]schemamodel.Table, 0, len(b.db.Tables))
	for _, table := range b.db.Tables {
		if !b.covers(table.Name) {
			continue
		}
		tables = append(tables, table)
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Name < tables[j].Name
	})
	return tables
}

// covers applies the include and exclude lists. Exclude wins, so a name in both
// is excluded rather than resolved by list order.
func (b *builder) covers(name string) bool {
	if slices.Contains(b.opts.ExcludeTables, name) {
		return false
	}
	if len(b.opts.IncludeTables) == 0 {
		return true
	}
	return slices.Contains(b.opts.IncludeTables, name)
}

// fieldsOf is the table's columns, in declaration order.
func (b *builder) fieldsOf(table schemamodel.Table) []schemamodel.Field {
	fields := make([]schemamodel.Field, 0, 8)
	for _, field := range b.db.Fields {
		if field.StructName != table.StructName {
			continue
		}
		fields = append(fields, field)
	}
	return fields
}

// column renders one column line with its settings.
func column(field schemamodel.Field) string {
	line := fmt.Sprintf("%s %s", quote(field.Name), columnType(field))
	settings := columnSettings(field)
	if len(settings) == 0 {
		return line
	}
	return line + " [" + strings.Join(settings, ", ") + "]"
}

// columnType is the column's declared type, quoted when it names an enum so a
// reader can tell a type name from a keyword.
func columnType(field schemamodel.Field) string {
	if len(field.Enum) > 0 {
		return quote(field.Type)
	}
	return field.Type
}

// columnSettings is the bracketed list, in a fixed order so the same column
// always renders the same way.
func columnSettings(field schemamodel.Field) []string {
	settings := make([]string, 0, 6)
	if field.Primary {
		settings = append(settings, "pk")
	}
	if field.AutoInc {
		settings = append(settings, "increment")
	}
	if field.Unique {
		settings = append(settings, "unique")
	}
	if !field.Nullable {
		settings = append(settings, "not null")
	}
	if def, ok := defaultSetting(field); ok {
		settings = append(settings, def)
	}
	if field.Comment != "" {
		settings = append(settings, "note: "+quoteNote(field.Comment))
	}
	return settings
}

// defaultSetting keeps a literal default and an expression default apart.
//
// DBML spells an expression in backticks and a literal in quotes, and the
// difference is not cosmetic: `default: 'now()'` is the six-character string,
// and `default: `now()“ is the call. A renderer that emitted one for the other
// would change what the column does.
func defaultSetting(field schemamodel.Field) (string, bool) {
	if field.DefaultExpr != "" {
		return "default: `" + field.DefaultExpr + "`", true
	}
	if field.DefaultSet {
		return "default: " + quoteNote(field.Default), true
	}
	return "", false
}

// indexesOf renders the table's indexes, sorted by name.
func (b *builder) indexesOf(table schemamodel.Table) []string {
	lines := make([]string, 0, 4)
	for _, index := range b.db.Indexes {
		if index.StructName != table.StructName {
			continue
		}
		lines = append(lines, indexLine(index))
	}
	sort.Strings(lines)
	return lines
}

// indexLine renders one index entry.
func indexLine(index schemamodel.Index) string {
	columns := make([]string, 0, len(index.Fields))
	for _, field := range index.Fields {
		columns = append(columns, quote(field))
	}
	settings := make([]string, 0, 3)
	if index.Unique {
		settings = append(settings, "unique")
	}
	if index.Type != "" {
		settings = append(settings, "type: "+strings.ToLower(index.Type))
	}
	if index.Name != "" {
		settings = append(settings, "name: "+quote(index.Name))
	}
	line := "(" + strings.Join(columns, ", ") + ")"
	if len(settings) == 0 {
		return line
	}
	return line + " [" + strings.Join(settings, ", ") + "]"
}
