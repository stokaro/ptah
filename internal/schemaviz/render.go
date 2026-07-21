// Package schemaviz renders goschema databases as graph descriptions.
package schemaviz

import (
	"fmt"
	"html"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/convert/fromschema"
)

const (
	FormatDOT     = "dot"
	FormatMermaid = "mermaid"
	ThemeLight    = "light"
	ThemeDark     = "dark"
)

// Options configures schema graph rendering.
type Options struct {
	Format         string
	IncludeColumns bool
	ExcludeTables  []string
	Theme          string
}

type relationship struct {
	From  string
	To    string
	Label string
}

// Render emits a schema graph in DOT or Mermaid erDiagram format.
func Render(db *goschema.Database, opts Options) ([]byte, error) {
	opts = normalizeOptions(opts)
	if opts.Format != FormatDOT && opts.Format != FormatMermaid {
		return nil, fmt.Errorf("unsupported visualization format %q: expected dot or mermaid", opts.Format)
	}
	if opts.Theme != ThemeLight && opts.Theme != ThemeDark {
		return nil, fmt.Errorf("unsupported visualization theme %q: expected light or dark", opts.Theme)
	}
	if db == nil {
		return nil, fmt.Errorf("schema is required")
	}
	model := buildModel(db, opts.ExcludeTables)
	switch opts.Format {
	case FormatDOT:
		return []byte(renderDOT(model, opts)), nil
	default:
		return []byte(renderMermaid(model, opts)), nil
	}
}

type graphModel struct {
	Tables        []goschema.Table
	FieldsByTable map[string][]goschema.Field
	Relationships []relationship
}

func normalizeOptions(opts Options) Options {
	opts.Format = strings.ToLower(strings.TrimSpace(opts.Format))
	if opts.Format == "" {
		opts.Format = FormatMermaid
	}
	opts.Theme = strings.ToLower(strings.TrimSpace(opts.Theme))
	if opts.Theme == "" {
		opts.Theme = ThemeLight
	}
	return opts
}

func buildModel(db *goschema.Database, excludeTables []string) graphModel {
	excluded := tableSet(excludeTables)
	tables := make([]goschema.Table, 0, len(db.Tables))
	tableByStruct := make(map[string]goschema.Table)
	tableNames := make(map[string]struct{})
	for _, table := range db.Tables {
		if isExcludedTable(excluded, table.QualifiedName(), table.Name) {
			continue
		}
		tables = append(tables, table)
		tableByStruct[table.StructName] = table
		tableNames[table.QualifiedName()] = struct{}{}
	}

	fields := fromschema.ProcessEmbeddedFields(db.EmbeddedFields, db.Fields)
	fieldsByTable := make(map[string][]goschema.Field)
	seenFields := make(map[string]struct{})
	for _, field := range fields {
		table, ok := tableByStruct[field.StructName]
		if !ok {
			continue
		}
		tableName := table.QualifiedName()
		key := tableName + "\x00" + field.Name
		if _, ok := seenFields[key]; ok {
			continue
		}
		seenFields[key] = struct{}{}
		fieldsByTable[tableName] = append(fieldsByTable[tableName], field)
	}

	relationships := fieldRelationships(tables, fieldsByTable, tableNames)
	relationships = append(relationships, constraintRelationships(tables, db.Constraints, tableNames)...)
	relationships = uniqueRelationships(relationships)
	slices.SortFunc(relationships, func(left, right relationship) int {
		for _, cmp := range []int{
			strings.Compare(left.From, right.From),
			strings.Compare(left.To, right.To),
			strings.Compare(left.Label, right.Label),
		} {
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	})

	return graphModel{
		Tables:        tables,
		FieldsByTable: fieldsByTable,
		Relationships: relationships,
	}
}

func fieldRelationships(tables []goschema.Table, fieldsByTable map[string][]goschema.Field, tableNames map[string]struct{}) []relationship {
	relationships := make([]relationship, 0)
	for _, table := range tables {
		for _, field := range fieldsByTable[table.QualifiedName()] {
			if strings.TrimSpace(field.Foreign) == "" {
				continue
			}
			refTable := resolveReferenceTable(tableNames, table, field.Foreign)
			if refTable == "" {
				continue
			}
			label := field.ForeignKeyName
			if label == "" {
				label = fromschema.GenerateForeignKeyName(table.Name, field.Name)
			}
			relationships = append(relationships, relationship{
				From:  table.QualifiedName(),
				To:    refTable,
				Label: label,
			})
		}
	}
	return relationships
}

func constraintRelationships(tables []goschema.Table, constraints []goschema.Constraint, tableNames map[string]struct{}) []relationship {
	tableByStruct := make(map[string]goschema.Table)
	tableByName := make(map[string]goschema.Table)
	for _, table := range tables {
		tableByStruct[table.StructName] = table
		tableByName[table.QualifiedName()] = table
		tableByName[table.Name] = table
	}
	relationships := make([]relationship, 0)
	for _, constraint := range constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") || constraint.ForeignTable == "" {
			continue
		}
		table, ok := tableForConstraint(tableByStruct, tableByName, constraint)
		if !ok {
			continue
		}
		refTable := resolveReferenceTable(tableNames, table, constraint.ForeignTable)
		if refTable == "" {
			continue
		}
		label := constraint.Name
		if label == "" {
			label = fromschema.GenerateForeignKeyName(table.Name, strings.Join(constraint.Columns, "_"))
		}
		relationships = append(relationships, relationship{
			From:  table.QualifiedName(),
			To:    refTable,
			Label: label,
		})
	}
	return relationships
}

func uniqueRelationships(relationships []relationship) []relationship {
	seen := make(map[relationship]struct{}, len(relationships))
	out := make([]relationship, 0, len(relationships))
	for _, rel := range relationships {
		if _, ok := seen[rel]; ok {
			continue
		}
		seen[rel] = struct{}{}
		out = append(out, rel)
	}
	return out
}

func tableForConstraint(
	tableByStruct map[string]goschema.Table,
	tableByName map[string]goschema.Table,
	constraint goschema.Constraint,
) (goschema.Table, bool) {
	if constraint.Table != "" {
		if table, ok := tableByName[constraint.Table]; ok {
			return table, true
		}
	}
	table, ok := tableByStruct[constraint.StructName]
	return table, ok
}

func resolveReferenceTable(tableNames map[string]struct{}, current goschema.Table, foreign string) string {
	refTable := strings.TrimSpace(foreign)
	if table, _, ok := strings.Cut(refTable, "("); ok {
		refTable = strings.TrimSpace(table)
	}
	if refTable == "" {
		return ""
	}
	if _, ok := tableNames[refTable]; ok {
		return refTable
	}
	qualified := goschema.QualifyTableName(current.Schema, refTable)
	if _, ok := tableNames[qualified]; ok {
		return qualified
	}
	var match string
	for tableName := range tableNames {
		if tableName == refTable || strings.HasSuffix(tableName, "."+refTable) {
			if match != "" {
				return ""
			}
			match = tableName
		}
	}
	return match
}

func renderDOT(model graphModel, opts Options) string {
	var b strings.Builder
	b.WriteString("digraph ptah_schema {\n")
	b.WriteString("  graph [rankdir=LR, pad=0.2, nodesep=0.6, ranksep=0.8")
	if opts.Theme == ThemeDark {
		b.WriteString(", bgcolor=\"#111827\"")
	}
	b.WriteString("];\n")
	if opts.Theme == ThemeDark {
		b.WriteString("  node [shape=plain, margin=0, fontname=\"Helvetica\"];\n")
		b.WriteString("  edge [color=\"#60a5fa\", fontcolor=\"#dbeafe\"];\n")
	} else {
		b.WriteString("  node [shape=plain, margin=0, fontname=\"Helvetica\"];\n")
		b.WriteString("  edge [color=\"#2563eb\", fontcolor=\"#1e3a8a\"];\n")
	}
	for _, table := range model.Tables {
		fmt.Fprintf(&b, "  %q [label=<\n%s\n  >];\n", table.QualifiedName(), dotTableLabel(table, model.FieldsByTable[table.QualifiedName()], opts))
	}
	for _, rel := range model.Relationships {
		fmt.Fprintf(&b, "  %q -> %q [label=%q];\n", rel.From, rel.To, rel.Label)
	}
	b.WriteString("}\n")
	return b.String()
}

func dotTableLabel(table goschema.Table, fields []goschema.Field, opts Options) string {
	borderColor := "#64748b"
	headerColor := "#e2e8f0"
	rowColor := "#f8fafc"
	headerTextColor := "#0f172a"
	rowTextColor := "#334155"
	if opts.Theme == ThemeDark {
		borderColor = "#94a3b8"
		headerColor = "#374151"
		rowColor = "#1f2937"
		headerTextColor = "#f9fafb"
		rowTextColor = "#e5e7eb"
	}

	var b strings.Builder
	fmt.Fprintf(
		&b,
		"    <TABLE BORDER=\"1\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"6\" COLOR=\"%s\">\n",
		borderColor,
	)
	fmt.Fprintf(
		&b,
		"      <TR><TD BGCOLOR=\"%s\"><FONT COLOR=\"%s\"><B>%s</B></FONT></TD></TR>\n",
		headerColor,
		headerTextColor,
		escapeHTML(table.QualifiedName()),
	)
	if opts.IncludeColumns {
		for _, field := range fields {
			fmt.Fprintf(
				&b,
				"      <TR><TD ALIGN=\"LEFT\" BGCOLOR=\"%s\"><FONT COLOR=\"%s\">%s</FONT></TD></TR>\n",
				rowColor,
				rowTextColor,
				escapeHTML(fieldLabel(field)),
			)
		}
	}
	b.WriteString("    </TABLE>")
	return b.String()
}

func renderMermaid(model graphModel, opts Options) string {
	var b strings.Builder
	if opts.Theme == ThemeDark {
		b.WriteString("%%{init: {\"theme\": \"dark\"}}%%\n")
	}
	b.WriteString("erDiagram\n")
	names := mermaidNames(model.Tables)
	for _, table := range model.Tables {
		name := names[table.QualifiedName()]
		if !opts.IncludeColumns {
			fmt.Fprintf(&b, "  %s {\n  }\n", name)
			continue
		}
		fmt.Fprintf(&b, "  %s {\n", name)
		for _, field := range model.FieldsByTable[table.QualifiedName()] {
			fmt.Fprintf(&b, "    %s %s%s\n", mermaidType(field.Type), mermaidIdentifier(field.Name), mermaidFieldKeySuffix(field))
		}
		b.WriteString("  }\n")
	}
	for _, rel := range model.Relationships {
		fmt.Fprintf(&b, "  %s ||--o{ %s : %q\n", names[rel.To], names[rel.From], rel.Label)
	}
	return b.String()
}

func mermaidNames(tables []goschema.Table) map[string]string {
	used := make(map[string]struct{}, len(tables))
	names := make(map[string]string, len(tables))
	for _, table := range tables {
		tableName := table.QualifiedName()
		base := mermaidIdentifier(tableName)
		name := base
		for suffix := 2; ; suffix++ {
			if _, ok := used[name]; !ok {
				break
			}
			name = fmt.Sprintf("%s_%d", base, suffix)
		}
		used[name] = struct{}{}
		names[tableName] = name
	}
	return names
}

func fieldLabel(field goschema.Field) string {
	labels := make([]string, 0, 2)
	if field.Primary {
		labels = append(labels, "PK")
	}
	if field.Foreign != "" {
		labels = append(labels, "FK")
	}
	suffix := ""
	if len(labels) > 0 {
		suffix = " " + strings.Join(labels, " ")
	}
	return fmt.Sprintf("%s : %s%s", field.Name, field.Type, suffix)
}

func mermaidFieldKeySuffix(field goschema.Field) string {
	labels := make([]string, 0, 2)
	if field.Primary {
		labels = append(labels, "PK")
	}
	if field.Foreign != "" {
		labels = append(labels, "FK")
	}
	if len(labels) == 0 {
		return ""
	}
	return " " + strings.Join(labels, ",")
}

func mermaidType(sqlType string) string {
	sqlType = strings.TrimSpace(sqlType)
	if sqlType == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(" ", "_", "(", "_", ")", "", ",", "_")
	return mermaidIdentifier(replacer.Replace(sqlType))
}

func mermaidIdentifier(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	value := b.String()
	if value == "" {
		return "_"
	}
	first := value[0]
	if first >= '0' && first <= '9' {
		return "_" + value
	}
	return value
}

func escapeHTML(value string) string {
	return html.EscapeString(value)
}

func tableSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			set[name] = struct{}{}
		}
	}
	return set
}

func isExcludedTable(excluded map[string]struct{}, names ...string) bool {
	if len(excluded) == 0 {
		return false
	}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := excluded[name]; ok {
			return true
		}
		if _, table, ok := strings.Cut(name, "."); ok {
			if _, ok := excluded[table]; ok {
				return true
			}
		}
	}
	return false
}
