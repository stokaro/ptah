package schemadoc

import (
	"fmt"
	"html"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/htmlstyle"
)

// ChangeKind is what a comparison found about one table.
//
// The three values are what a schema comparison can say about a table and
// nothing more. A column that changed marks its table changed: the diagram
// draws tables, so a mark it cannot place is a mark it cannot show, and the
// table card below the diagram is where the column-level detail already is.
type ChangeKind string

// The marks a comparison can carry. An unrecognized value marks nothing rather
// than rendering as one of these, because a document that guessed would show a
// table as removed on the strength of a typo.
const (
	ChangeAdded   ChangeKind = "added"
	ChangeRemoved ChangeKind = "removed"
	ChangeChanged ChangeKind = "changed"
)

// valid reports whether the renderer knows this mark.
func (c ChangeKind) valid() bool {
	return c == ChangeAdded || c == ChangeRemoved || c == ChangeChanged
}

// Options selects what the document covers and what it is called.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
	// Changes marks tables with what a comparison found about them, keyed by
	// table name. An empty map renders exactly the document that rendered
	// before marks existed.
	//
	// The document takes the marks as data rather than comparing two schemas
	// itself, for the reason [ptah.run/internal/schemaviz.Options.Annotations]
	// gives about findings: a renderer that knew how to diff would have to
	// learn the next comparison too. It also means the caller decides what a
	// removed table is -- this package draws one schema, so a table that only
	// the earlier state had is one the caller put into the schema it passed.
	Changes map[string]ChangeKind
	// Title heads the document. Empty uses a neutral default rather than
	// inventing a project name.
	Title string
	// Source names where the schema was read from, for the line under the
	// title. It is a name rather than a path: the document is meant to be
	// shareable by copying, and a copied file that carries the exporter's
	// filesystem layout says more about the machine than about the schema.
	// Empty leaves the line saying what it must, that this is a declared
	// schema and not a database.
	Source string
}

// Result is the rendered document and anything worth telling the caller.
type Result struct {
	Data        []byte
	Diagnostics []string
}

const defaultTitle = "Schema reference"

// The three characters the document sets in text rather than in markup. They
// are named because a raw string literal does not process \u escapes, and a
// backtick-quoted `\u2014` reaches the page as those six characters.
const (
	emDash     = "\u2014"
	rightArrow = "\u2192"
	middleDot  = "\u00b7"
)

// Render writes one self-contained HTML document.
//
// Every column of every selected table appears. That is deliberate and unlike
// the API export targets, which project a public shape: documentation that hid
// a column would describe a schema the reader does not have.
func Render(db *schemamodel.Database, opts Options) (Result, error) {
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
	writeBody(&out, doc)
	writeFooter(&out)
	out.WriteString(`</main></div></body></html>`)
	return Result{Data: []byte(out.String()), Diagnostics: diagnostics}, nil
}

// Stylesheet is the appearance this package renders with, for a caller that
// composes its own page out of the same parts.
//
// It is exported so the dashboard in stokaro/ptah#1863 shares one design rather
// than growing a second one that drifts from this. A copy would look the same
// on the day it was made and not the day after.
func Stylesheet() string { return stylesheet() }

// Page is the schema's own sections -- navigation, overview, diagram, tables
// and enums -- without the document that wraps them.
//
// A caller that serves a live view puts its own panels above these and supplies
// the surrounding html, head and body itself. Everything here is the same
// markup Render emits, so the two views cannot disagree about what a schema
// looks like.
func Page(db *schemamodel.Database, opts Options) (sidebar, content string, err error) {
	if db == nil {
		return "", "", fmt.Errorf("schema database is nil")
	}
	doc := build(db, opts)
	if doc.Title == "" {
		doc.Title = defaultTitle
	}
	var nav, body strings.Builder
	writeNav(&nav, doc)
	writeBody(&body, doc)
	return nav.String(), body.String(), nil
}

// writeBody writes the sections both views share, so adding one reaches both.
func writeBody(out *strings.Builder, doc document) {
	writeDiagram(out, doc)
	writeTables(out, doc)
	writeEnums(out, doc)
}

func writeHead(out *strings.Builder, doc document) {
	out.WriteString("<!doctype html>\n<html lang=\"en\">\n<head>\n")
	out.WriteString(`<meta charset="utf-8">`)
	out.WriteString(`<meta name="viewport" content="width=device-width, initial-scale=1">`)
	fmt.Fprintf(out, "<title>%s</title>", escapeText(doc.Title))
	fmt.Fprintf(out, "<style>%s</style>", stylesheet())
	out.WriteString("\n</head>\n")
}

func writeSidebar(out *strings.Builder, doc document) {
	out.WriteString(`<aside class="sidebar">`)
	fmt.Fprintf(out, `<div class="brand">%s</div>`, escapeText(doc.Title))
	fmt.Fprintf(out, `<div class="brand-sub">%s</div>`, escapeText(counts(doc)))
	writeNav(out, doc)
	out.WriteString(`</aside>`)
}

// writeNav is the navigation alone, without the rail that holds it.
//
// A caller that supplies its own rail -- the live dashboard does, with its own
// title and database address above this -- takes this and not writeSidebar. The
// two used to be one function, which is how the dashboard came to nest an
// <aside class="sidebar"> inside its own and print the title twice.
func writeNav(out *strings.Builder, doc document) {
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
	out.WriteString(`</nav>`)
}

// counts is the sidebar's one-line summary of what the document holds.
func counts(doc document) string {
	summary := plural(len(doc.Tables), "table", "tables")
	if len(doc.Enums) == 0 {
		return summary
	}
	return summary + " " + middleDot + " " + plural(len(doc.Enums), "enum", "enums")
}

func writeOverview(out *strings.Builder, doc document) {
	fmt.Fprintf(out, `<h1>%s</h1>`, escapeText(doc.Title))
	fmt.Fprintf(out, `<div class="lede">%s</div>`, provenance(doc.Source))
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

// provenance is the line under the title.
//
// It says what the document is not, because that is the mistake a reader of a
// schema reference makes: this describes what the sources declare, and a
// database may hold something else entirely. The page it is documented on warns
// about exactly this, and the page does not travel with the file.
func provenance(source string) string {
	if source == "" {
		return "Declared schema " + middleDot + " not a live database"
	}
	return "Declared schema " + middleDot + " " + escapeText(source) + " " + middleDot + " not a live database"
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
	out.WriteString(`<div class="card"><div class="erd">` + svg)
	writeChangeLegend(out, doc)
	out.WriteString(`<div class="erd-note">Left to right by dependency: a table sits right of everything it references, ` +
		`so this order is one the tables can be created in.</div></div></div>`)
}

// writeChangeLegend names the colors, and only when the diagram uses them.
//
// A legend on an unmarked document would describe three states none of its
// tables are in. It lists only the kinds actually present for the same reason:
// a reader who sees "removed" in the key looks for a removed table.
func writeChangeLegend(out *strings.Builder, doc document) {
	present := make(map[ChangeKind]bool, 3)
	for _, table := range doc.Tables {
		present[table.Change] = true
	}
	shown := make([]ChangeKind, 0, 3)
	for _, kind := range []ChangeKind{ChangeAdded, ChangeChanged, ChangeRemoved} {
		if present[kind] {
			shown = append(shown, kind)
		}
	}
	if len(shown) == 0 {
		return
	}
	out.WriteString(`<div class="erd-legend">`)
	for _, kind := range shown {
		fmt.Fprintf(out, `<span class="chg chg-%s">%s</span>`, string(kind), changeLabel(kind))
	}
	out.WriteString(`</div>`)
}

// changeLabel is the word a reader sees for a mark.
func changeLabel(kind ChangeKind) string {
	switch kind {
	case ChangeAdded:
		return "added"
	case ChangeRemoved:
		return "removed"
	case ChangeChanged:
		return "changed"
	}
	return ""
}

func writeTables(out *strings.Builder, doc document) {
	if len(doc.Tables) == 0 {
		return
	}
	out.WriteString(`<h2 id="tables">Tables</h2>`)
	declared := declaredEnums(doc)
	for _, table := range doc.Tables {
		writeTableCard(out, table, declared)
	}
}

// declaredEnums is the set of enum names this document defines a section for.
//
// A column whose type names one links to it; a column whose type does not is
// left alone, so a link is never offered to a section that is not there.
func declaredEnums(doc document) map[string]bool {
	declared := make(map[string]bool, len(doc.Enums))
	for _, enum := range doc.Enums {
		declared[enum.Name] = true
	}
	return declared
}

func writeTableCard(out *strings.Builder, table tableDoc, declared map[string]bool) {
	fmt.Fprintf(out, `<section id="%s" class="card">`, anchor(table.Name))
	out.WriteString(`<div class="card-head">`)
	fmt.Fprintf(out, `<h3>%s</h3>`, escapeText(table.Name))
	if table.Change != "" {
		fmt.Fprintf(out, `<span class="chg chg-%s">%s</span>`, string(table.Change), changeLabel(table.Change))
	}
	if table.Comment != "" {
		fmt.Fprintf(out, `<span class="card-note">%s</span>`, escapeText(table.Comment))
	}
	out.WriteString(`</div>`)
	writeColumns(out, table, declared)
	writeIndexes(out, table)
	out.WriteString(`</section>`)
}

func writeColumns(out *strings.Builder, table tableDoc, declared map[string]bool) {
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
		fmt.Fprintf(out, `<td class="type">%s</td>`, typeCell(column.Type, declared))
		fmt.Fprintf(out, `<td>%s</td>`, nullTag(column))
		fmt.Fprintf(out, `<td>%s</td>`, keyTag(column.Key))
		fmt.Fprintf(out, `<td class="type">%s</td>`, escapeText(column.Default))
		fmt.Fprintf(out, `<td>%s</td>`, foreignTag(column.Foreign))
		fmt.Fprintf(out, `<td class="comment">%s</td>`, escapeText(column.Comment))
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
		out.WriteString(`<div class="values">`)
		for _, value := range enum.Values {
			fmt.Fprintf(out, `<span>%s</span>`, escapeText(value))
		}
		out.WriteString(`</div></section>`)
	}
}

// The tag helpers keep the conditionals out of the writers, so a row renders as
// one expression per cell.

// nullTag tags the exception and not the rule.
//
// Most columns are NOT NULL, so tagging every one of them puts a pill on nearly
// every row and leaves the reader scanning for the absence of one. The dash is
// what a reader's eye passes over; the tag is what it stops on.
func nullTag(column columnDoc) string {
	if column.Nullable {
		return `<span class="tag null">null</span>`
	}
	return `<span class="none">` + emDash + `</span>`
}

// keyTag fills the primary key and outlines everything else, because a table
// has one primary key and a reader looking for it should not have to read the
// word.
func keyTag(key string) string {
	switch key {
	case "":
		return ""
	case "primary":
		return `<span class="tag key">primary</span>`
	default:
		return `<span class="tag">` + escapeText(key) + `</span>`
	}
}

func uniqueTag(index indexDoc) string {
	if !index.Unique {
		return ""
	}
	return `<span class="tag">unique</span>`
}

// typeCell links a column typed by a declared enum to that enum's section.
func typeCell(columnType string, declared map[string]bool) string {
	if !declared[columnType] {
		return escapeText(columnType)
	}
	return fmt.Sprintf(`<a href="#%s">%s</a>`, anchor("enum-"+columnType), escapeText(columnType))
}

// foreignTag renders a reference as the reference it is rather than as a chip:
// an arrow, the target, and a link to it when the target is in this document.
func foreignTag(foreign string) string {
	if foreign == "" {
		return ""
	}
	target, ok := foreignTarget(foreign)
	if !ok {
		return `<span class="ref">` + rightArrow + ` ` + escapeText(foreign) + `</span>`
	}
	return fmt.Sprintf(`<a class="ref" href="#%s">%s %s</a>`, anchor(target), rightArrow, escapeText(foreign))
}

// writeFooter closes the document with what produced it.
func writeFooter(out *strings.Builder) {
	out.WriteString(htmlstyle.Footer("Rendered by Ptah from the declared schema. " +
		"This file is self-contained: opening it fetches nothing."))
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
