package schemadoc

import (
	"fmt"
	"html"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// Options selects what the document covers and what it is called.
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

// Render writes one self-contained HTML document.
//
// Every column of every selected table appears. That is deliberate and unlike
// the API export targets, which project a public shape: documentation that hid
// a column would describe a schema the reader does not have.
func Render(db *goschema.Database, opts Options) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}
	doc := build(db, opts)
	if doc.Title == "" {
		doc.Title = defaultTitle
	}

	var diagnostics []string
	if len(doc.Tables) == 0 {
		// An empty document is a worse answer than an empty document that says
		// so: the reader cannot tell "no tables" from "the filter matched none".
		diagnostics = append(diagnostics, "no tables matched the selection")
	}

	var out strings.Builder
	writeHead(&out, doc)
	out.WriteString(`<body><div class="layout">`)
	writeSidebar(&out, doc)
	out.WriteString(`<main class="content">`)
	writeOverview(&out, doc)
	writeDiagram(&out, doc)
	writeTables(&out, doc)
	writeEnums(&out, doc)
	out.WriteString(`<div class="footer">Rendered by Ptah from the declared schema. This file is self-contained: opening it fetches nothing.</div>`)
	out.WriteString(`</main></div></body></html>`)
	return Result{Data: []byte(out.String()), Diagnostics: diagnostics}, nil
}

func writeHead(out *strings.Builder, doc document) {
	out.WriteString("<!doctype html>\n<html lang=\"en\">\n<head>\n")
	out.WriteString(`<meta charset="utf-8">`)
	out.WriteString(`<meta name="viewport" content="width=device-width, initial-scale=1">`)
	fmt.Fprintf(out, "<title>%s</title>", escapeText(doc.Title))
	fmt.Fprintf(out, "<style>%s</style>", documentCSS)
	out.WriteString("\n</head>\n")
}

func writeSidebar(out *strings.Builder, doc document) {
	out.WriteString(`<aside class="sidebar">`)
	fmt.Fprintf(out, `<div class="brand">%s</div>`, escapeText(doc.Title))
	fmt.Fprintf(out, `<div class="brand-sub">%s</div>`, plural(len(doc.Tables), "table", "tables"))
	out.WriteString(`<nav class="nav">`)
	if len(doc.Tables) > 0 {
		out.WriteString(`<div class="nav-group"><div class="nav-title">Tables</div>`)
		for _, table := range doc.Tables {
			fmt.Fprintf(out, `<a href="#%s">%s</a>`, anchor(table.Name), escapeText(table.Name))
		}
		out.WriteString(`</div>`)
	}
	if len(doc.Enums) > 0 {
		out.WriteString(`<div class="nav-group"><div class="nav-title">Enums</div>`)
		for _, enum := range doc.Enums {
			fmt.Fprintf(out, `<a href="#%s">%s</a>`, anchor("enum-"+enum.Name), escapeText(enum.Name))
		}
		out.WriteString(`</div>`)
	}
	out.WriteString(`</nav></aside>`)
}

func writeOverview(out *strings.Builder, doc document) {
	fmt.Fprintf(out, `<h1>%s</h1>`, escapeText(doc.Title))
	out.WriteString(`<div class="stats">`)
	writeStat(out, len(doc.Tables), "tables")
	writeStat(out, countColumns(doc), "columns")
	writeStat(out, len(doc.Relations), "references")
	writeStat(out, len(doc.Enums), "enums")
	out.WriteString(`</div>`)
	if len(doc.Tables) == 0 {
		out.WriteString(`<div class="card"><div class="empty">No tables are selected.</div></div>`)
	}
}

func writeStat(out *strings.Builder, value int, label string) {
	fmt.Fprintf(out, `<div class="stat"><div class="stat-n">%d</div><div class="stat-l">%s</div></div>`,
		value, escapeText(label))
}

func writeDiagram(out *strings.Builder, doc document) {
	svg := renderERD(doc)
	if svg == "" {
		return
	}
	out.WriteString(`<h2 id="diagram">Diagram</h2>`)
	out.WriteString(`<div class="card"><div class="erd">` + svg + `</div></div>`)
}

func writeTables(out *strings.Builder, doc document) {
	if len(doc.Tables) == 0 {
		return
	}
	out.WriteString(`<h2 id="tables">Tables</h2>`)
	for _, table := range doc.Tables {
		writeTableCard(out, table)
	}
}

func writeTableCard(out *strings.Builder, table tableDoc) {
	fmt.Fprintf(out, `<section id="%s" class="card">`, anchor(table.Name))
	out.WriteString(`<div class="card-head">`)
	fmt.Fprintf(out, `<h3>%s</h3>`, escapeText(table.Name))
	if table.Comment != "" {
		fmt.Fprintf(out, `<span class="card-note">%s</span>`, escapeText(table.Comment))
	}
	out.WriteString(`</div>`)
	writeColumns(out, table)
	writeIndexes(out, table)
	out.WriteString(`</section>`)
}

func writeColumns(out *strings.Builder, table tableDoc) {
	if len(table.Columns) == 0 {
		out.WriteString(`<div class="empty">This table declares no columns.</div>`)
		return
	}
	out.WriteString(`<div class="scroller"><table><thead><tr>`)
	for _, header := range []string{"Column", "Type", "Null", "Key", "Default", "References", "Comment"} {
		fmt.Fprintf(out, `<th>%s</th>`, header)
	}
	out.WriteString(`</tr></thead><tbody>`)
	for _, column := range table.Columns {
		out.WriteString(`<tr>`)
		fmt.Fprintf(out, `<td class="name">%s</td>`, escapeText(column.Name))
		fmt.Fprintf(out, `<td class="type">%s</td>`, escapeText(column.Type))
		fmt.Fprintf(out, `<td>%s</td>`, nullTag(column))
		fmt.Fprintf(out, `<td>%s</td>`, keyTag(column.Key))
		fmt.Fprintf(out, `<td class="type">%s</td>`, escapeText(column.Default))
		fmt.Fprintf(out, `<td>%s</td>`, foreignTag(column.Foreign))
		fmt.Fprintf(out, `<td>%s</td>`, escapeText(column.Comment))
		out.WriteString(`</tr>`)
	}
	out.WriteString(`</tbody></table></div>`)
}

func writeIndexes(out *strings.Builder, table tableDoc) {
	if len(table.Indexes) == 0 {
		return
	}
	out.WriteString(`<div class="scroller"><table><thead><tr><th>Index</th><th>Columns</th><th>Unique</th></tr></thead><tbody>`)
	for _, index := range table.Indexes {
		out.WriteString(`<tr>`)
		fmt.Fprintf(out, `<td class="name">%s</td>`, escapeText(index.Name))
		fmt.Fprintf(out, `<td class="type">%s</td>`, escapeText(index.Columns))
		fmt.Fprintf(out, `<td>%s</td>`, uniqueTag(index))
		out.WriteString(`</tr>`)
	}
	out.WriteString(`</tbody></table></div>`)
}

func writeEnums(out *strings.Builder, doc document) {
	if len(doc.Enums) == 0 {
		return
	}
	out.WriteString(`<h2 id="enums">Enums</h2>`)
	for _, enum := range doc.Enums {
		fmt.Fprintf(out, `<section id="%s" class="card"><div class="card-head"><h3>%s</h3>`,
			anchor("enum-"+enum.Name), escapeText(enum.Name))
		fmt.Fprintf(out, `<span class="card-note">%s</span></div>`, plural(len(enum.Values), "value", "values"))
		out.WriteString(`<div class="scroller"><table><tbody>`)
		for _, value := range enum.Values {
			fmt.Fprintf(out, `<tr><td class="name">%s</td></tr>`, escapeText(value))
		}
		out.WriteString(`</tbody></table></div></section>`)
	}
}

// The tag helpers keep the conditionals out of the writers, so a row renders as
// one expression per cell.

func nullTag(column columnDoc) string {
	if column.Nullable {
		return `<span class="tag null">null</span>`
	}
	return `<span class="tag">not null</span>`
}

func keyTag(key string) string {
	if key == "" {
		return ""
	}
	return `<span class="tag key">` + escapeText(key) + `</span>`
}

func uniqueTag(index indexDoc) string {
	if !index.Unique {
		return ""
	}
	return `<span class="tag key">unique</span>`
}

func foreignTag(foreign string) string {
	if foreign == "" {
		return ""
	}
	target, ok := foreignTarget(foreign)
	if !ok {
		return `<span class="tag fk">` + escapeText(foreign) + `</span>`
	}
	return fmt.Sprintf(`<a class="tag fk" href="#%s">%s</a>`, anchor(target), escapeText(foreign))
}

func countColumns(doc document) int {
	total := 0
	for _, table := range doc.Tables {
		total += len(table.Columns)
	}
	return total
}

func plural(count int, one, many string) string {
	if count == 1 {
		return fmt.Sprintf("%d %s", count, one)
	}
	return fmt.Sprintf("%d %s", count, many)
}

// escapeText is the only way text from a schema reaches the page. A column
// comment is authored text and can contain anything.
func escapeText(value string) string {
	return html.EscapeString(value)
}

// anchor builds a fragment identifier from an object name.
func anchor(name string) string {
	var out strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
		default:
			out.WriteByte('-')
		}
	}
	return out.String()
}
