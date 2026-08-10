// Package testcontour discovers and runs build-tagged live test contours.
package testcontour

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Config describes one live test contour invocation.
type Config struct {
	Package string
	Tag     string
	Tags    []string
	Timeout time.Duration
	Dir     string
	Stdout  io.Writer
	Stderr  io.Writer
}

type packageFiles struct {
	Dir          string
	TestGoFiles  []string
	XTestGoFiles []string
}

type testEvent struct {
	Action string
	Test   string
	Output string
}

type testState struct {
	ran      bool
	terminal bool
}

// Run discovers every test declared by the contour tag and requires all of
// them to finish without a skipped test or subtest.
func Run(ctx context.Context, config Config) error {
	config, err := normalizeConfig(config)
	if err != nil {
		return err
	}

	tests, err := discover(ctx, config)
	if err != nil {
		return err
	}

	return run(ctx, config, tests)
}

func normalizeConfig(config Config) (Config, error) {
	if config.Package == "" {
		return Config{}, errors.New("test contour package is required")
	}
	if !strings.HasPrefix(config.Tag, "ptah_live_") {
		return Config{}, fmt.Errorf("test contour tag %q must start with ptah_live_", config.Tag)
	}
	if !validBuildTag(config.Tag) {
		return Config{}, fmt.Errorf("test contour tag %q is not a valid build tag", config.Tag)
	}
	if config.Timeout <= 0 {
		return Config{}, errors.New("test contour timeout must be positive")
	}
	if config.Stdout == nil {
		config.Stdout = io.Discard
	}
	if config.Stderr == nil {
		config.Stderr = io.Discard
	}
	if config.Dir == "" {
		config.Dir = "."
	}

	for _, tag := range config.Tags {
		if !validBuildTag(tag) {
			return Config{}, fmt.Errorf("additional build tag %q is not valid", tag)
		}
	}

	config.Tags = append(slices.Clone(config.Tags), config.Tag)
	sort.Strings(config.Tags)
	config.Tags = slices.Compact(config.Tags)

	return config, nil
}

func validBuildTag(tag string) bool {
	if tag == "" {
		return false
	}
	for _, character := range tag {
		if character != '_' && character != '.' &&
			(character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func discover(ctx context.Context, config Config) ([]string, error) {
	files, err := listPackageFiles(ctx, config)
	if err != nil {
		return nil, err
	}

	testFiles := append(slices.Clone(files.TestGoFiles), files.XTestGoFiles...)
	tests := make([]string, 0)
	for _, name := range testFiles {
		path := filepath.Join(files.Dir, name)
		declares, err := declaresTag(path, config.Tag)
		if err != nil {
			return nil, err
		}
		if !declares {
			continue
		}

		declared, err := declaredTests(path)
		if err != nil {
			return nil, err
		}
		tests = append(tests, declared...)
	}

	sort.Strings(tests)
	if len(tests) == 0 {
		return nil, fmt.Errorf("test contour %q selects no tests in %s", config.Tag, config.Package)
	}
	if duplicate := firstDuplicate(tests); duplicate != "" {
		return nil, fmt.Errorf("test contour %q selects duplicate test %s", config.Tag, duplicate)
	}

	return tests, nil
}

func listPackageFiles(ctx context.Context, config Config) (packageFiles, error) {
	args := []string{"list", "-json", "-tags=" + strings.Join(config.Tags, ","), config.Package}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = config.Dir
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return packageFiles{}, fmt.Errorf("list test contour package %s: %s", config.Package, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return packageFiles{}, fmt.Errorf("list test contour package %s: %w", config.Package, err)
	}

	var files packageFiles
	if err := json.Unmarshal(output, &files); err != nil {
		return packageFiles{}, fmt.Errorf("decode go list output for %s: %w", config.Package, err)
	}
	if files.Dir == "" {
		return packageFiles{}, fmt.Errorf("go list returned no directory for %s", config.Package)
	}

	return files, nil
}

func declaresTag(path, tag string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("open test contour file %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "//go:build ") {
			expression, err := constraint.Parse(line)
			if err != nil {
				return false, fmt.Errorf("parse build constraint in %s: %w", path, err)
			}
			return referencesPositiveTag(expression, tag, false), nil
		}
		if line != "" && !strings.HasPrefix(line, "//") {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("read build constraint in %s: %w", path, err)
	}

	return false, nil
}

func referencesPositiveTag(expression constraint.Expr, tag string, negated bool) bool {
	switch expression := expression.(type) {
	case *constraint.TagExpr:
		return !negated && expression.Tag == tag
	case *constraint.NotExpr:
		return referencesPositiveTag(expression.X, tag, !negated)
	case *constraint.AndExpr:
		return referencesPositiveTag(expression.X, tag, negated) ||
			referencesPositiveTag(expression.Y, tag, negated)
	case *constraint.OrExpr:
		return referencesPositiveTag(expression.X, tag, negated) ||
			referencesPositiveTag(expression.Y, tag, negated)
	default:
		return false
	}
}

func declaredTests(path string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse test contour file %s: %w", path, err)
	}

	tests := make([]string, 0)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv != nil || !isTestName(function.Name.Name) {
			continue
		}
		tests = append(tests, function.Name.Name)
	}

	return tests, nil
}

func isTestName(name string) bool {
	if name == "TestMain" {
		return false
	}
	suffix, ok := strings.CutPrefix(name, "Test")
	if !ok || suffix == "" {
		return false
	}
	first, _ := utf8.DecodeRuneInString(suffix)
	return !unicode.IsLower(first)
}

func firstDuplicate(values []string) string {
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			return values[index]
		}
	}
	return ""
}

func run(ctx context.Context, config Config, tests []string) error {
	states := make(map[string]testState, len(tests))
	for _, name := range tests {
		states[name] = testState{}
	}

	pattern := exactTestPattern(tests)
	args := []string{
		"test",
		"-json",
		"-count=1",
		"-tags=" + strings.Join(config.Tags, ","),
		"-run=" + pattern,
		"-timeout=" + config.Timeout.String(),
		config.Package,
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = config.Dir
	cmd.Stderr = config.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("capture test contour output: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start test contour %q: %w", config.Tag, err)
	}

	skips := make([]string, 0)
	decodeErr := decodeEvents(stdout, config.Stdout, states, &skips)
	waitErr := cmd.Wait()
	if decodeErr != nil {
		return decodeErr
	}
	if waitErr != nil {
		return fmt.Errorf("test contour %q failed: %w", config.Tag, waitErr)
	}
	if len(skips) != 0 {
		return fmt.Errorf("test contour %q skipped %s", config.Tag, strings.Join(skips, ", "))
	}

	missing := missingResults(states)
	if len(missing) != 0 {
		return fmt.Errorf("test contour %q produced no complete result for %s", config.Tag, strings.Join(missing, ", "))
	}

	return nil
}

func exactTestPattern(tests []string) string {
	quoted := make([]string, len(tests))
	for index, name := range tests {
		quoted[index] = regexp.QuoteMeta(name)
	}
	return "^(?:" + strings.Join(quoted, "|") + ")$"
}

func decodeEvents(reader io.Reader, output io.Writer, states map[string]testState, skips *[]string) error {
	decoder := json.NewDecoder(reader)
	var outputErr error
	for {
		var event testEvent
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				return outputErr
			}
			_, _ = io.Copy(io.Discard, reader)
			return fmt.Errorf("decode test contour output: %w", err)
		}
		if event.Output != "" && outputErr == nil {
			if _, err := io.WriteString(output, event.Output); err != nil {
				outputErr = fmt.Errorf("write test contour output: %w", err)
			}
		}
		recordEvent(event, states, skips)
	}
}

func recordEvent(event testEvent, states map[string]testState, skips *[]string) {
	if event.Action == "skip" {
		name := event.Test
		if name == "" {
			name = "package"
		}
		*skips = append(*skips, name)
	}

	state, selected := states[event.Test]
	if !selected {
		return
	}
	switch event.Action {
	case "run":
		state.ran = true
	case "pass", "fail", "skip":
		state.terminal = true
	}
	states[event.Test] = state
}

func missingResults(states map[string]testState) []string {
	missing := make([]string, 0)
	for name, state := range states {
		if !state.ran || !state.terminal {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	return missing
}
