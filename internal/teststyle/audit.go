// Package teststyle audits repository tests against Ptah's test style rules.
package teststyle

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

const whiteBoxJustificationPrefix = "// White-box testing required:"

// Baseline records known test-style violations. Cleanup PRs should reduce this
// file; ordinary feature PRs should keep it unchanged.
type Baseline struct {
	TestConditionals []ConditionalBaseline `json:"test_conditionals"`
	WhiteBoxFiles    []WhiteBoxBaseline    `json:"white_box_files"`
}

// ConditionalBaseline records prohibited conditional statements in one test
// function, grouped by statement kind.
type ConditionalBaseline struct {
	Path     string `json:"path"`
	Function string `json:"function"`
	Kind     string `json:"kind"`
	Count    int    `json:"count"`
}

// WhiteBoxBaseline records a same-package test file that still needs black-box
// conversion or an explicit white-box justification.
type WhiteBoxBaseline struct {
	Path    string `json:"path"`
	Package string `json:"package"`
	Reason  string `json:"reason"`
}

// Scan scans root and returns the current repository test-style baseline.
func Scan(root string) (Baseline, error) {
	if root == "" {
		root = "."
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return Baseline{}, fmt.Errorf("resolve root: %w", err)
	}

	fset := token.NewFileSet()
	conditionals := map[conditionalKey]int{}
	var whiteBoxFiles []WhiteBoxBaseline

	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if skippedDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("relativize %s: %w", path, err)
		}
		relativePath = filepath.ToSlash(relativePath)

		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return fmt.Errorf("parse %s: %w", relativePath, err)
		}
		scanConditionals(relativePath, file, conditionals)
		whiteBoxFiles = append(whiteBoxFiles, scanWhiteBoxFile(path, relativePath, file.Name.Name)...)
		return nil
	}); err != nil {
		return Baseline{}, err
	}

	return Baseline{
		TestConditionals: conditionalBaseline(conditionals),
		WhiteBoxFiles:    sortedWhiteBoxBaseline(whiteBoxFiles),
	}, nil
}

// ReadBaseline reads a baseline JSON file.
func ReadBaseline(path string) (Baseline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Baseline{}, fmt.Errorf("read baseline: %w", err)
	}
	var baseline Baseline
	if err := json.Unmarshal(data, &baseline); err != nil {
		return Baseline{}, fmt.Errorf("parse baseline: %w", err)
	}
	baseline.Normalize()
	return baseline, nil
}

// WriteBaseline writes a deterministic baseline JSON file.
func WriteBaseline(path string, baseline Baseline) error {
	baseline.Normalize()
	data, err := json.MarshalIndent(baseline, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal baseline: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write baseline: %w", err)
	}
	return nil
}

// Normalize sorts baseline entries deterministically.
func (b *Baseline) Normalize() {
	sort.Slice(b.TestConditionals, func(i, j int) bool {
		return compareConditional(b.TestConditionals[i], b.TestConditionals[j]) < 0
	})
	sort.Slice(b.WhiteBoxFiles, func(i, j int) bool {
		return compareWhiteBox(b.WhiteBoxFiles[i], b.WhiteBoxFiles[j]) < 0
	})
}

// Diff returns a human-readable mismatch report. Empty string means equal.
func Diff(want, got Baseline) string {
	want = cloneBaseline(want)
	got = cloneBaseline(got)
	want.Normalize()
	got.Normalize()

	var builder strings.Builder
	writeConditionalDiff(&builder, want.TestConditionals, got.TestConditionals)
	writeWhiteBoxDiff(&builder, want.WhiteBoxFiles, got.WhiteBoxFiles)
	return builder.String()
}

func cloneBaseline(baseline Baseline) Baseline {
	return Baseline{
		TestConditionals: slices.Clone(baseline.TestConditionals),
		WhiteBoxFiles:    slices.Clone(baseline.WhiteBoxFiles),
	}
}

type conditionalKey struct {
	path     string
	function string
	kind     string
}

func skippedDirectory(name string) bool {
	return name == ".git" || name == "vendor" || name == "node_modules"
}

func scanConditionals(path string, file *ast.File, conditionals map[conditionalKey]int) {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isTestFunction(fn.Name.Name) {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch stmt := node.(type) {
			case *ast.IfStmt:
				addConditional(conditionals, path, fn.Name.Name, "if")
			case *ast.SwitchStmt:
				addConditional(conditionals, path, fn.Name.Name, "switch")
			case *ast.TypeSwitchStmt:
				addConditional(conditionals, path, fn.Name.Name, "type switch")
			case *ast.BranchStmt:
				if stmt.Tok == token.GOTO {
					addConditional(conditionals, path, fn.Name.Name, "goto")
				}
			}
			return true
		})
	}
}

func addConditional(conditionals map[conditionalKey]int, path, function, kind string) {
	conditionals[conditionalKey{path: path, function: function, kind: kind}]++
}

func isTestFunction(name string) bool {
	return strings.HasPrefix(name, "Test") ||
		strings.HasPrefix(name, "Fuzz") ||
		strings.HasPrefix(name, "Example")
}

func scanWhiteBoxFile(path string, relativePath string, packageName string) []WhiteBoxBaseline {
	if strings.HasSuffix(packageName, "_test") {
		return nil
	}
	if !strings.HasSuffix(relativePath, "_internal_test.go") {
		return []WhiteBoxBaseline{{
			Path:    relativePath,
			Package: packageName,
			Reason:  "same-package test file is not named *_internal_test.go",
		}}
	}
	if hasWhiteBoxJustification(path) {
		return nil
	}
	return []WhiteBoxBaseline{{
		Path:    relativePath,
		Package: packageName,
		Reason:  "missing white-box justification comment after package clause",
	}}
}

func hasWhiteBoxJustification(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	seenPackage := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !seenPackage {
			seenPackage = strings.HasPrefix(line, "package ")
			continue
		}
		if line == "" {
			continue
		}
		return strings.HasPrefix(line, whiteBoxJustificationPrefix)
	}
	return false
}

func conditionalBaseline(conditionals map[conditionalKey]int) []ConditionalBaseline {
	result := make([]ConditionalBaseline, 0, len(conditionals))
	for key, count := range conditionals {
		result = append(result, ConditionalBaseline{
			Path:     key.path,
			Function: key.function,
			Kind:     key.kind,
			Count:    count,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return compareConditional(result[i], result[j]) < 0
	})
	return result
}

func sortedWhiteBoxBaseline(values []WhiteBoxBaseline) []WhiteBoxBaseline {
	sort.Slice(values, func(i, j int) bool {
		return compareWhiteBox(values[i], values[j]) < 0
	})
	return values
}

func compareConditional(a, b ConditionalBaseline) int {
	return cmp.Or(
		cmp.Compare(a.Path, b.Path),
		cmp.Compare(a.Function, b.Function),
		cmp.Compare(a.Kind, b.Kind),
	)
}

func compareWhiteBox(a, b WhiteBoxBaseline) int {
	return cmp.Or(
		cmp.Compare(a.Path, b.Path),
		cmp.Compare(a.Package, b.Package),
		cmp.Compare(a.Reason, b.Reason),
	)
}

func writeConditionalDiff(builder *strings.Builder, want, got []ConditionalBaseline) {
	wantSet := conditionalSet(want)
	gotSet := conditionalSet(got)
	writeMissing(builder, "test conditional baseline is stale or too high", setDiff(wantSet, gotSet))
	writeMissing(builder, "new test conditional violations", setDiff(gotSet, wantSet))
}

func writeWhiteBoxDiff(builder *strings.Builder, want, got []WhiteBoxBaseline) {
	wantSet := whiteBoxSet(want)
	gotSet := whiteBoxSet(got)
	writeMissing(builder, "white-box baseline is stale or too high", setDiff(wantSet, gotSet))
	writeMissing(builder, "new white-box test violations", setDiff(gotSet, wantSet))
}

func conditionalSet(values []ConditionalBaseline) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[fmt.Sprintf("%s\t%s\t%s\t%d", value.Path, value.Function, value.Kind, value.Count)] = struct{}{}
	}
	return result
}

func whiteBoxSet(values []WhiteBoxBaseline) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[fmt.Sprintf("%s\t%s\t%s", value.Path, value.Package, value.Reason)] = struct{}{}
	}
	return result
}

func setDiff(a, b map[string]struct{}) []string {
	var result []string
	for value := range a {
		if _, ok := b[value]; !ok {
			result = append(result, value)
		}
	}
	slices.Sort(result)
	return result
}

func writeMissing(builder *strings.Builder, title string, values []string) {
	if len(values) == 0 {
		return
	}
	if builder.Len() > 0 {
		builder.WriteByte('\n')
	}
	fmt.Fprintf(builder, "%s:\n", title)
	for _, value := range values {
		fmt.Fprintf(builder, "  %s\n", value)
	}
}

// ErrBaselineMismatch indicates current test-style findings differ from the
// committed baseline.
var ErrBaselineMismatch = errors.New("test style baseline mismatch")
