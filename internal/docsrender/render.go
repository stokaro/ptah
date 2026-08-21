// Package docsrender turns a desired schema into Markdown reference
// documentation.
//
// It is the local answer to a hosted documentation service: the same schema
// Ptah already reads for every other export target, written out as prose a
// reader can commit beside the code (stokaro/ptah#1712).
package docsrender

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

// Options selects what the document covers.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
	// Title heads the document. Empty uses a neutral default rather than
	// inventing a project name.
	Title string
}

// Result is the rendered document and anything worth telling the caller.
type Result struct {
	Data        []byte
	Diagnostics []string
}

const defaultTitle = "Schema reference"

// Render writes the document.
//
// Every column of every selected table appears. That is deliberate and unlike
// the API targets, which project a public shape: documentation that hid a
// column would describe a schema the reader does not have.
func Render(db *goschema.Database, opts Options) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}
	tables := schemaexport.SelectTables(db, schemaexport.Options{
		IncludeTables: opts.IncludeTables,
		ExcludeTables: opts.ExcludeTables,
	})

	var out strings.Builder
	title := opts.Title
	if title == "" {
		title = defaultTitle
	}
	fmt.Fprintf(&out, "# %s\n", title)

	var diagnostics []string
	if len(tables) == 0 {
		// An empty document is a worse answer than an empty document that says
		// so: the reader cannot tell "no tables" from "the filter matched none".
		out.WriteString("\nNo tables are selected.\n")
		diagnostics = append(diagnostics, "no tables matched the selection")
		return Result{Data: []byte(out.String()), Diagnostics: diagnostics}, nil
	}

	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	slices.Sort(names)
	out.WriteString("\n")
	for _, name := range names {
		fmt.Fprintf(&out, "- [%s](#%s)\n", name, anchor(name))
	}

	for _, name := range names {
		writeTable(&out, db, tableByName(tables, name))
	}
	writeEnums(&out, db)
	return Result{Data: []byte(out.String()), Diagnostics: diagnostics}, nil
}

func tableByName(tables []goschema.Table, name string) goschema.Table {
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	return goschema.Table{}
}

func writeTable(out *strings.Builder, db *goschema.Database, table goschema.Table) {
	fmt.Fprintf(out, "\n## %s\n", table.Name)
	if table.Comment != "" {
		fmt.Fprintf(out, "\n%s\n", table.Comment)
	}
	out.WriteString("\n| Column | Type | Null | Default | Key | Comment |\n")
	out.WriteString("| --- | --- | --- | --- | --- | --- |\n")
	for _, field := range db.Fields {
		if field.StructName != table.StructName {
			continue
		}
		fmt.Fprintf(out, "| %s | %s | %s | %s | %s | %s |\n",
			cell(field.Name), cell(field.Type), nullability(field),
			cell(defaultOf(field)), cell(keyOf(field)), cell(field.Comment))
	}
	writeIndexes(out, db, table)
}

func writeIndexes(out *strings.Builder, db *goschema.Database, table goschema.Table) {
	var lines []string
	for _, index := range db.Indexes {
		if index.StructName != table.StructName {
			continue
		}
		kind := "index"
		if index.Unique {
			kind = "unique index"
		}
		lines = append(lines, fmt.Sprintf("- `%s` — %s on %s", index.Name, kind,
			strings.Join(index.Fields, ", ")))
	}
	if len(lines) == 0 {
		return
	}
	slices.Sort(lines)
	out.WriteString("\n**Indexes**\n\n")
	out.WriteString(strings.Join(lines, "\n"))
	out.WriteString("\n")
}

func writeEnums(out *strings.Builder, db *goschema.Database) {
	if len(db.Enums) == 0 {
		return
	}
	out.WriteString("\n## Enums\n\n")
	names := make([]string, 0, len(db.Enums))
	byName := make(map[string]goschema.Enum, len(db.Enums))
	for _, enum := range db.Enums {
		names = append(names, enum.Name)
		byName[enum.Name] = enum
	}
	slices.Sort(names)
	for _, name := range names {
		fmt.Fprintf(out, "- `%s` — %s\n", name, strings.Join(byName[name].Values, ", "))
	}
}

// defaultOf reports the column's default in the form it was declared.
//
// A non-empty Default counts on its own, and DefaultSet is consulted only to
// keep a deliberate empty-string default. The two are not equivalent: the Go
// annotation parser sets DefaultSet and the YAML loader does not, so requiring
// it would have documented every YAML-declared default as absent.
func defaultOf(field goschema.Field) string {
	if field.DefaultExpr != "" {
		return field.DefaultExpr
	}
	if field.Default != "" || field.DefaultSet {
		return field.Default
	}
	return ""
}

// keyOf names what makes the column special, most significant first.
func keyOf(field goschema.Field) string {
	var parts []string
	if field.Primary {
		parts = append(parts, "PK")
	}
	if field.Unique || field.UniqueExpr != "" {
		parts = append(parts, "unique")
	}
	if field.Foreign != "" {
		parts = append(parts, "FK → "+field.Foreign)
	}
	return strings.Join(parts, ", ")
}

// nullability spells a column's nullability for the table.
func nullability(field goschema.Field) string {
	if field.Nullable {
		return "yes"
	}
	return "no"
}

// cell escapes the one character that would break a Markdown table row, and
// renders an empty value as an em dash so a column is never silently blank.
func cell(value string) string {
	value = strings.ReplaceAll(value, "|", `\|`)
	value = strings.ReplaceAll(value, "\n", " ")
	if strings.TrimSpace(value) == "" {
		return "—"
	}
	return value
}

// anchor is the GitHub-flavored heading anchor for a table name.
func anchor(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ', r == '_', r == '-':
			b.WriteRune('-')
		}
	}
	return b.String()
}
