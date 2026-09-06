// Package htmlstyle is the one appearance every HTML document Ptah writes.
//
// Ptah emits HTML from four places -- the exported schema document, the live
// schema dashboard, the migration safety report and the database test report --
// and each of them used to carry a stylesheet of its own. They agreed about
// nothing: three spellings of "safe", three of "destructive", three type
// scales, and dark mode on one of them. This package is the declaration those
// four now read, so a color has one value and changing it changes every page.
//
// A caller composes Tokens and Base with whatever is genuinely its own, the
// way the dashboard already composes the document's stylesheet. Nothing here
// knows what any of those pages contain.
package htmlstyle

import (
	"strings"

	"ptah.run/internal/buildinfo"
)

// Tokens is the color and metric declaration, in the three blocks a
// self-contained page needs.
//
// The light values sit on bare :root, so every token has a value before any
// theme block runs. The other two blocks redefine those values and introduce
// none, which is what keeps a token from existing in one theme and not the
// other: a color defined only under prefers-color-scheme is missing for a
// reader who chose light explicitly, and the element it paints falls back to
// whatever the browser decides.
func Tokens() string { return rootVariablesCSS }

// Base is the appearance of the parts these documents share: text, links,
// tables, cards, count strips, tags and the footer.
//
// It defines no layout, because the four pages are laid out differently and a
// shared rule that only one of them wants is a rule the other three work
// around.
func Base() string { return baseCSS }

// Mark is the Ptah mark, inlined.
//
// It is inlined rather than linked because these documents fetch nothing, and
// it keeps its own colors in both themes because a mark recolored to match its
// surroundings is a different mark.
func Mark() string { return markSVG }

// Footer closes a document with the note the caller wants and the version of
// the binary that wrote the file.
//
// The version is there because these documents are shared and archived, and
// "which Ptah produced this" is a question their reader cannot otherwise
// answer. The note is expected to be trusted text the caller wrote, not
// anything read out of a schema or a database.
func Footer(note string) string {
	var out strings.Builder
	out.WriteString(`<div class="footer"><span>`)
	out.WriteString(note)
	out.WriteString(`</span><span class="footer-mark">`)
	out.WriteString(markSVG)
	out.WriteString(`ptah `)
	out.WriteString(buildinfo.Resolve().Version)
	out.WriteString(`</span></div>`)
	return out.String()
}

// Head is everything from the doctype to the open body tag, for a page that
// wants the standard shell. The title is written verbatim, so a caller with a
// title from outside the program escapes it first.
func Head(title, extraCSS string) string {
	var out strings.Builder
	out.WriteString("<!doctype html>\n<html lang=\"en\">\n<head>\n")
	out.WriteString(`<meta charset="utf-8">`)
	out.WriteString(`<meta name="viewport" content="width=device-width, initial-scale=1">`)
	out.WriteString(`<title>` + title + `</title>`)
	out.WriteString(`<style>` + rootVariablesCSS + baseCSS + extraCSS + `</style>`)
	out.WriteString("\n</head>\n")
	return out.String()
}

// markSVG is docs/site/src/assets/logo.svg, inlined and stripped of the title
// the pages do not need: each one names Ptah in words beside it.
const markSVG = `<svg viewBox="0 0 64 64" width="14" height="14" aria-hidden="true">` +
	`<rect width="64" height="64" rx="14" fill="#0f172a"/>` +
	`<rect x="23" y="13" width="18" height="11" rx="2" fill="#f59e0b"/>` +
	`<rect x="17" y="27" width="30" height="11" rx="2" fill="#38bdf8"/>` +
	`<rect x="11" y="41" width="42" height="11" rx="2" fill="#38bdf8"/></svg>`
