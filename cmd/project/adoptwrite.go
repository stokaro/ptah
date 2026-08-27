package project

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// normalizeProject rewrites the compat-only spellings a report names.
//
// It splices bytes rather than editing through hclwrite, and the difference is
// visible to the person whose file it is. hclwrite re-aligns the block it
// touches, so setting one attribute re-indents its neighbours: a file written
// as
//
//	url      =    "sqlite://app.db"
//
// comes back as `url = "sqlite://app.db"` even though adoption had nothing to
// say about it. A normalizer that tidies formatting it was not asked to tidy
// makes the diff larger than the change and gives the reader nothing in the
// output to explain it (stokaro/ptah#1215).
//
// Only `migration.dir` is rewritten today, because it is the only construct the
// analysis classifies as compat-only. A second one joins by appearing in the
// report with Current and Native set.
func normalizeProject(path string, report AdoptionReport) ([]string, error) {
	pending := make([]Construct, 0, len(report.Constructs))
	for _, construct := range report.Constructs {
		if construct.Class == classCompatOnly && construct.Current != "" && construct.Native != "" {
			pending = append(pending, construct)
		}
	}
	if len(pending) == 0 {
		return nil, nil
	}

	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	file, diags := hclsyntax.ParseConfig(source, path, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parse %s: %s", path, diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("parse %s: unexpected body type %T", path, file.Body)
	}

	var (
		edits   []edit
		changed []string
	)
	for _, construct := range pending {
		found := migrationDirRanges(body, source, construct.Current)
		if len(found) == 0 {
			// The analysis said this construct is here and the file does not
			// carry it. Refusing beats writing a file that still says the old
			// thing while the command reports success.
			return nil, fmt.Errorf(
				"%s: %s is %s in the analysis but no migration.dir in the file carries it",
				path, construct.Name, construct.Current)
		}
		for _, span := range found {
			edits = append(edits, edit{span: span, replacement: strconv.Quote(construct.Native)})
		}
		changed = append(changed, fmt.Sprintf("%s: %s -> %s", construct.Name, construct.Current, construct.Native))
	}

	// Written through a temporary file in the same directory and renamed, so a
	// failure partway leaves the original rather than half a project file.
	return changed, writeAtomically(path, applyEdits(source, edits))
}

// edit is one byte range and what replaces it.
type edit struct {
	span        hcl.Range
	replacement string
}

// applyEdits splices every edit into source, last first so an earlier offset is
// not moved by a later replacement.
func applyEdits(source []byte, edits []edit) []byte {
	sort.Slice(edits, func(i, j int) bool {
		return edits[i].span.Start.Byte > edits[j].span.Start.Byte
	})
	out := append([]byte(nil), source...)
	for _, e := range edits {
		out = append(out[:e.span.Start.Byte],
			append([]byte(e.replacement), out[e.span.End.Byte:]...)...)
	}
	return out
}

// migrationDirRanges is the source range of every `migration.dir` whose literal
// value is current.
//
// It walks `migration` blocks wherever they are -- at the top level and inside
// each `env` -- because either place is where a project may carry one, and an
// adoption that rewrote only the selected env would leave the others naming a
// reference native Ptah is being told to stop using.
func migrationDirRanges(body *hclsyntax.Body, source []byte, current string) []hcl.Range {
	var found []hcl.Range
	for _, block := range body.Blocks {
		if block.Type != "migration" {
			found = append(found, migrationDirRanges(block.Body, source, current)...)
			continue
		}
		attribute, declared := block.Body.Attributes["dir"]
		if !declared {
			continue
		}
		literal, ok := literalStringValue(attribute.Expr)
		if !ok || literal != current {
			continue
		}
		found = append(found, attribute.Expr.Range())
	}
	return found
}

// literalStringValue reads a quoted literal back out of an expression.
//
// Anything else -- a variable, a function call, an interpolation -- has no
// value until evaluation, so there is no literal to compare against the
// analysis or to replace. Those are left alone rather than rewritten to
// whatever they happened to evaluate to on this run.
func literalStringValue(expr hclsyntax.Expression) (string, bool) {
	template, ok := expr.(*hclsyntax.TemplateExpr)
	if !ok || !template.IsStringLiteral() {
		return "", false
	}
	value, diags := template.Value(nil)
	if diags.HasErrors() || value.IsNull() {
		return "", false
	}
	return value.AsString(), true
}

// writeAtomically replaces path's contents without leaving a partial file
// behind on failure.
func writeAtomically(path string, contents []byte) error {
	dir := filepath.Dir(path)
	temporary, err := os.CreateTemp(dir, filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("create a temporary file beside %s: %w", path, err)
	}
	name := temporary.Name()
	defer os.Remove(name)

	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close %s: %w", name, err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

// writeNormalizationText reports what the rewrite changed.
func writeNormalizationText(out io.Writer, path string, changed []string) {
	fmt.Fprintf(out, "Rewrote %s:\n", path)
	for _, line := range changed {
		fmt.Fprintf(out, "  %s\n", line)
	}
	fmt.Fprintln(out, "\nEvery other construct already meant the same thing natively and is untouched.")
}
