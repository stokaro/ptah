package featureinventory

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// SiteRoot is the directory the reader-facing pages live in. A `Canonical page`
// cell names a page by the slug it has under this root.
const SiteRoot = "docs/site/src/content/docs"

// Document is one page or repository document an inventory row can point at.
type Document struct {
	// Path is the tracked file, relative to the repository root.
	Path string
	// Body is the whole file.
	Body string
	// Fenced is the concatenated contents of every fenced code block in it,
	// which is where a runnable example has to be. A page with no fenced block
	// carries no example whatever its prose says.
	Fenced string
}

// Documents is every tracked document a row can name, keyed both by site slug
// and by repository path.
//
// Two keys rather than one, because the register points at both kinds on
// purpose: a reader-facing surface names a site slug, and a contributor surface
// names a repository document -- `Example` says `yes: examples/viz/README.md`
// for the visualization artifacts, and `repo doc only: docs/oci_registry.md`
// where nothing on the site covers a surface yet.
type Documents struct {
	bySlug map[string]Document
	byPath map[string]Document
}

// NewDocuments reads every tracked Markdown document.
func NewDocuments(repoRoot string) (*Documents, error) {
	files, err := DocFiles(repoRoot)
	if err != nil {
		return nil, err
	}
	docs := &Documents{bySlug: make(map[string]Document), byPath: make(map[string]Document)}
	for _, rel := range files {
		body, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(rel)))
		if err != nil {
			return nil, fmt.Errorf("featureinventory: reading %s: %w", rel, err)
		}
		doc := Document{Path: rel, Body: string(body), Fenced: fencedText(string(body))}
		docs.byPath[rel] = doc
		slug, ok := slugOf(rel)
		if !ok {
			continue
		}
		docs.bySlug[slug] = doc
		if parent, ok := directorySlug(slug); ok {
			docs.bySlug[parent] = doc
		}
	}
	if len(docs.bySlug) == 0 {
		return nil, fmt.Errorf("featureinventory: no page resolved under %s; refusing to check page claims against an empty site", SiteRoot)
	}
	return docs, nil
}

// Slugs returns every resolvable site slug, sorted.
func (d *Documents) Slugs() []string {
	slugs := make([]string, 0, len(d.bySlug))
	for slug := range d.bySlug {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)
	return slugs
}

// Lookup resolves one reference: a site slug, or a tracked repository path.
func (d *Documents) Lookup(reference string) (Document, bool) {
	if doc, ok := d.bySlug[reference]; ok {
		return doc, true
	}
	doc, ok := d.byPath[reference]
	return doc, ok
}

// slugOf turns a tracked site file into the slug a page reference spells.
func slugOf(rel string) (string, bool) {
	trimmed, ok := strings.CutPrefix(rel, SiteRoot+"/")
	if !ok {
		return "", false
	}
	slug := strings.TrimSuffix(strings.TrimSuffix(trimmed, ".mdx"), ".md")
	if slug == "" {
		return "", false
	}
	return slug, true
}

// directorySlug is the second spelling an `index` page answers to, so a row may
// name `concepts` for `concepts/index.mdx`. The splash page keeps only its
// literal slug: `path.Dir("index")` is `.`, which names no page.
func directorySlug(slug string) (string, bool) {
	if path.Base(slug) != "index" {
		return "", false
	}
	parent := path.Dir(slug)
	if parent == "." || parent == "" {
		return "", false
	}
	return parent, true
}

// fencedText concatenates every fenced code block in a document.
func fencedText(body string) string {
	var out strings.Builder
	fence := ""
	for line := range strings.SplitSeq(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if fence != "" {
			if strings.HasPrefix(trimmed, fence) {
				fence = ""
				continue
			}
			out.WriteString(line)
			out.WriteString("\n")
			continue
		}
		if match := fenceOpen.FindStringSubmatch(line); match != nil {
			fence = match[1]
		}
	}
	return out.String()
}

// The two shapes a page column may take, other than a reference.
const (
	noPageMarker = "**none"
	exampleYes   = "yes"
)

// environmentVariable matches a `PTAH_*` name exactly.
var environmentVariable = regexp.MustCompile(`^PTAH_[A-Z0-9_]+$`)

// CheckInventoryPages is check 6: the page columns resolve, and the page a row
// names carries the surface.
//
// It exists because the register's central promise -- which page owns each
// surface -- was the one part of it no gate read. Checks 1 and 4 key on
// `Feature ID` and `Public surface`, the two document checks read the documents
// rather than the register's claims about them, and check 5 compares a generated
// block with itself. Five rows named a page that does not carry their surface at
// all while every other number in the file reproduced exactly.
//
// Three rules, in increasing strength:
//
//   - every page or file a row names exists. This is the rename: the site is
//     being restructured, a page moves, and a row keeps pointing at a slug that
//     404s;
//   - a row claiming a runnable example names a document with at least one
//     fenced code block. A page with none carries no example whatever its prose
//     says, and this is exact for every row;
//   - where the row's surface names a command path or a `PTAH_*` variable, the
//     named document contains one of the row's tokens -- anywhere for the
//     canonical page, inside a fenced block for the example.
//
// The third rule is scoped, and the scope is its limit rather than an oversight.
// A command path and an environment variable each have exactly one spelling, so
// their absence from a page is a fact. A media type, a workflow file, a release
// archive name and a directive are things a page can own without quoting -- the
// support matrix owns the capability probe without naming
// `.github/workflows/capability-matrix.yml` -- and demanding the quotation
// reported 47 rows whose page was right. Measured over the register: 228 rows
// carry an identifier and are held to the third rule.
func CheckInventoryPages(docs *Documents, inventory *Inventory) []Finding {
	var findings []Finding
	for _, row := range inventory.Rows {
		findings = append(findings, checkCanonicalPage(docs, inventory, row)...)
		findings = append(findings, checkExample(docs, inventory, row)...)
	}
	return sortFindings(findings)
}

// checkCanonicalPage reads one row's `Canonical page` cell.
func checkCanonicalPage(docs *Documents, inventory *Inventory, row Row) []Finding {
	cell := strings.TrimSpace(row.Cells["Canonical page"])
	if strings.HasPrefix(cell, noPageMarker) {
		return nil
	}
	reference, ok := firstReference(cell)
	if !ok {
		return []Finding{{File: inventory.Path, Line: row.Line, Message: fmt.Sprintf(
			"row %q names neither a page nor `**none`` in Canonical page: %q", row.ID, cell)}}
	}
	doc, found := docs.Lookup(reference)
	if !found {
		return []Finding{{File: inventory.Path, Line: row.Line, Message: fmt.Sprintf(
			"row %q names the canonical page `%s`, which resolves to no tracked document under %s", row.ID, reference, SiteRoot)}}
	}
	return missingSurface(inventory, row, reference, doc.Body,
		"row %q names `%s` as the canonical page for %s, and that document does not contain any of them")
}

// checkExample reads one row's `Example` cell.
func checkExample(docs *Documents, inventory *Inventory, row Row) []Finding {
	cell := strings.TrimSpace(row.Cells["Example"])
	if !strings.HasPrefix(cell, exampleYes) {
		return nil
	}
	reference, ok := firstReference(cell)
	if !ok {
		return []Finding{{File: inventory.Path, Line: row.Line, Message: fmt.Sprintf(
			"row %q claims an example and names no document: %q", row.ID, cell)}}
	}
	doc, found := docs.Lookup(reference)
	if !found {
		return []Finding{{File: inventory.Path, Line: row.Line, Message: fmt.Sprintf(
			"row %q claims an example on `%s`, which resolves to no tracked document under %s", row.ID, reference, SiteRoot)}}
	}
	if strings.TrimSpace(doc.Fenced) == "" {
		return []Finding{{File: inventory.Path, Line: row.Line, Message: fmt.Sprintf(
			"row %q claims a runnable example on `%s`, and that document has no fenced code block at all", row.ID, reference)}}
	}
	return missingSurface(inventory, row, reference, doc.Fenced,
		"row %q claims a runnable example on `%s` for %s, and no fenced block there contains any of them")
}

// missingSurface applies the third rule: a row whose surface carries an
// identifier must find one of its tokens in the document it names.
func missingSurface(inventory *Inventory, row Row, reference, text, message string) []Finding {
	tokens := row.identifyingTokens()
	if len(tokens) == 0 {
		return nil
	}
	for _, token := range row.Surface {
		if strings.Contains(text, token.Raw) {
			return nil
		}
	}
	return []Finding{{File: inventory.Path, Line: row.Line,
		Message: fmt.Sprintf(message, row.ID, reference, strings.Join(tokens, ", "))}}
}

// identifyingTokens lists the row's command paths and `PTAH_*` variables, the
// two spellings a document cannot paraphrase.
func (r Row) identifyingTokens() []string {
	var tokens []string
	for _, token := range r.Surface {
		if token.Kind == KindCommand {
			tokens = append(tokens, "`"+token.Raw+"`")
			continue
		}
		name, _, _ := strings.Cut(token.Raw, "=")
		if environmentVariable.MatchString(name) {
			tokens = append(tokens, "`"+name+"`")
		}
	}
	return tokens
}

// firstReference reads the first backticked page slug or repository path out of
// a cell.
func firstReference(cell string) (string, bool) {
	match := backticked.FindStringSubmatch(cell)
	if match == nil {
		return "", false
	}
	return strings.TrimSpace(match[1]), true
}

// PageReferenceCount reports how many rows each rule actually inspected, so a
// run that stopped reading the columns says so instead of reporting the same
// success as a clean register.
func PageReferenceCount(inventory *Inventory) (canonical, examples, identified int) {
	for _, row := range inventory.Rows {
		if strings.HasPrefix(strings.TrimSpace(row.Cells["Canonical page"]), "`") {
			canonical++
		}
		if strings.HasPrefix(strings.TrimSpace(row.Cells["Example"]), exampleYes) {
			examples++
		}
		if len(row.identifyingTokens()) > 0 {
			identified++
		}
	}
	return canonical, examples, identified
}
