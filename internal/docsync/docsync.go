// Package docsync keeps a generated block in a document equal to what a
// generator renders, and rewrites it on request.
//
// Five shell wrappers implemented this separately -- 650 lines carrying the
// same argument parsing, the same awk extractor, the same carrier discovery,
// the same diff rendering, and five byte-identical copies of an embedded Python
// program that replaces the text between two markers. A sixth generated surface
// invited a sixth copy, with its own marker edge cases (stokaro/ptah#2510).
//
// What the wrappers got right is kept, and it is the part worth naming: every
// refusal here is a way the comparison could otherwise succeed while measuring
// nothing. A missing marker yields an empty block on both sides and compares
// equal; a generator that printed nothing compares equal to a document whose
// block was deleted; and a second copy of a block in another file goes stale
// with nothing looking at it.
package docsync

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Target is one generated block, or one whole generated file.
type Target struct {
	// Name is what a diagnostic calls this block.
	Name string
	// Path is the document that carries it, relative to the repository root.
	Path string
	// Begin and End are the marker lines. Both empty means the whole file is
	// generated, which is how a page with no hand-written half is declared.
	Begin, End string
	// Source names what the block is generated FROM, so a stale-content
	// diagnostic sends its reader to the declaration rather than to the page.
	Source string
	// Render writes the current content. It is a function rather than a command
	// line because every generator in this repository is already a library:
	// running them in one process is what stops eleven `go run` starts.
	Render func(io.Writer) error
}

// WholeFile reports whether the target has no markers.
func (t Target) WholeFile() bool { return t.Begin == "" && t.End == "" }

// Result is what one target's check found.
type Result struct {
	Target  Target
	Stale   bool
	Diff    string
	Problem string
}

// OK reports whether the target needs nothing.
func (r Result) OK() bool { return !r.Stale && r.Problem == "" }

// Generate renders one target, refusing empty output.
//
// A generator that prints nothing is the failure this cannot let through: the
// document's block would be rewritten to nothing on a --write, and on a check
// it compares equal to a document somebody had already emptied.
func Generate(target Target) (string, error) {
	var out bytes.Buffer
	if err := target.Render(&out); err != nil {
		return "", fmt.Errorf("%s: %w", target.Name, err)
	}
	if out.Len() == 0 {
		return "", fmt.Errorf(
			"%s: the generator produced nothing; refusing to compare %s against an empty block",
			target.Name, target.Path)
	}
	return out.String(), nil
}

// Extract returns the current content of the target's block.
//
// A document that carries neither marker is a problem rather than an empty
// block, for the reason the package comment gives.
//
// A marker must be ALONE ON ITS LINE. Accepting it as a substring is how a
// broken marker -- text appended to it in a merge, say -- passes the existence
// check and then splits nothing, which left Replace writing a second copy of
// the block at the end of the file instead of refusing.
func Extract(document string, target Target) (string, error) {
	if target.WholeFile() {
		return document, nil
	}
	cut, err := split(document, target)
	if err != nil {
		return "", err
	}
	return cut.Body, nil
}

// parts is a document cut into the three pieces a marker target has.
type parts struct {
	// Head is everything up to and including the line before the begin marker.
	Head string
	// Body is the generated block.
	Body string
	// Tail is everything after the end marker's line.
	Tail string
}

// split cuts a document into its three parts.
func split(document string, target Target) (parts, error) {
	before, rest, ok := strings.Cut(padded(document), "\n"+target.Begin+"\n")
	if !ok {
		return parts{}, fmt.Errorf(
			"%s: %s carries no %s line", target.Name, target.Path, target.Begin)
	}
	block, after, ok := strings.Cut(rest, target.End+"\n")
	if !ok {
		return parts{}, fmt.Errorf(
			"%s: %s carries no %s line", target.Name, target.Path, target.End)
	}
	// padded added a leading newline so a marker on the first line is found by
	// the same search as one anywhere else; strip it back off. The separator
	// also consumed the newline that ENDED the line before the marker, so give
	// it back -- without it a rewrite joins that line to the marker, and the
	// document drifts by one line each time it is written.
	head := strings.TrimPrefix(before, "\n")
	if head != "" {
		head += "\n"
	}
	return parts{Head: head, Body: block, Tail: after}, nil
}

// padded makes a first-line marker reachable by a search that anchors on the
// newline before it, and guarantees the end marker has a newline after it.
func padded(document string) string {
	if !strings.HasSuffix(document, "\n") {
		document += "\n"
	}
	return "\n" + document
}

// Replace returns the document with the target's block set to content.
func Replace(document, content string, target Target) (string, error) {
	if target.WholeFile() {
		return content, nil
	}
	cut, err := split(document, target)
	if err != nil {
		return "", err
	}
	return cut.Head + target.Begin + "\n" + content + target.End + "\n" + cut.Tail, nil
}

// Check reports whether the target's block in root matches what it renders.
func Check(root string, target Target) Result {
	generated, err := Generate(target)
	if err != nil {
		return Result{Target: target, Problem: err.Error()}
	}
	document, err := os.ReadFile(filepath.Join(root, target.Path)) //#nosec G304 -- see Write
	if err != nil {
		return Result{Target: target, Problem: fmt.Sprintf("%s: %v", target.Name, err)}
	}
	current, err := Extract(string(document), target)
	if err != nil {
		return Result{Target: target, Problem: err.Error()}
	}
	if current == generated {
		return Result{Target: target}
	}
	return Result{Target: target, Stale: true, Diff: UnifiedDiff(generated, current)}
}

// Write rewrites the target's block and reports whether the file changed.
func Write(root string, target Target) (bool, error) {
	generated, err := Generate(target)
	if err != nil {
		return false, err
	}
	path := filepath.Join(root, target.Path)
	document, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("%s: %w", target.Name, err)
	}
	updated, err := Replace(string(document), generated, target)
	if err != nil {
		return false, err
	}
	if updated == string(document) {
		return false, nil
	}
	// 0o644: a documentation page is world-readable, and this preserves what
	// the tree already has rather than tightening a tracked file's mode.
	// #nosec G703 G306 -- G703: the path is a declared Target.Path joined to
	// the root git reported, both this repository's own values. G306: a tracked
	// documentation page is world-readable, and tightening its mode here would
	// be a change nobody asked for.
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		return false, fmt.Errorf("%s: %w", target.Name, err)
	}
	return true, nil
}

// UnifiedDiff renders the difference between the generated block and the one in
// the document, line by line and without a dependency.
func UnifiedDiff(generated, current string) string {
	want := strings.Split(strings.TrimSuffix(generated, "\n"), "\n")
	got := strings.Split(strings.TrimSuffix(current, "\n"), "\n")
	var out strings.Builder
	for i := 0; i < len(want) || i < len(got); i++ {
		switch {
		case i >= len(got):
			fmt.Fprintf(&out, "  +%s\n", want[i])
		case i >= len(want):
			fmt.Fprintf(&out, "  -%s\n", got[i])
		case want[i] != got[i]:
			fmt.Fprintf(&out, "  -%s\n  +%s\n", got[i], want[i])
		}
	}
	return out.String()
}
