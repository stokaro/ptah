// Package testcontour runs build-tagged integration packages and rejects
// skipped or incomplete tests.
package testcontour

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Config describes one integration package contour invocation.
type Config struct {
	Package string
	Tags    []string
	Timeout time.Duration
	Race    bool
	Dir     string
	Stdout  io.Writer
	Stderr  io.Writer
}

type testEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

type packageFiles struct {
	ImportPath   string
	Dir          string
	TestGoFiles  []string
	XTestGoFiles []string
}

type testID struct {
	Package string
	Name    string
}

type testState struct {
	ran      bool
	terminal bool
}

type integrationBuildPolicy struct {
	positive bool
	required bool
	excluded bool
}

type tagPolarity uint8

const (
	tagDisabled tagPolarity = iota
	tagEnabled
)

const completeIntegrationPattern = "./integration/..."

// Run executes every test selected by the package pattern and build tags. It
// fails when the contour runs no tests or any test or subtest is skipped.
func Run(ctx context.Context, config Config) error {
	config, err := normalizeConfig(config)
	if err != nil {
		return err
	}
	if slices.Contains(config.Tags, "integration") {
		if err := validateRepositoryIntegrationLayout(ctx, config); err != nil {
			return err
		}
	}
	packages, err := listPackages(ctx, config, config.Package)
	if err != nil {
		return err
	}
	var expected []testID
	if slices.Contains(config.Tags, "integration") {
		expected, err = validateIntegrationPackages(config, packages)
		if err != nil {
			return err
		}
	} else {
		expected, err = declaredPackageTests(packages)
		if err != nil {
			return err
		}
	}
	return run(ctx, config, expected)
}

func normalizeConfig(config Config) (Config, error) {
	if config.Package == "" {
		return Config{}, errors.New("test contour package is required")
	}
	if config.Timeout <= 0 {
		return Config{}, errors.New("test contour timeout must be positive")
	}
	if len(config.Tags) == 0 {
		return Config{}, errors.New("test contour requires at least one build tag")
	}
	for _, tag := range config.Tags {
		if !validBuildTag(tag) {
			return Config{}, fmt.Errorf("build tag %q is not valid", tag)
		}
	}
	if slices.Contains(config.Tags, "integration") && config.Package != completeIntegrationPattern {
		return Config{}, fmt.Errorf(
			"integration contour package must be %s, got %s",
			completeIntegrationPattern,
			config.Package,
		)
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
	config.Tags = slices.Clone(config.Tags)
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

func validateIntegrationPackages(config Config, packages []packageFiles) ([]testID, error) {
	expected := make([]testID, 0)
	for _, files := range packages {
		if len(files.TestGoFiles) != 0 {
			return nil, fmt.Errorf(
				"integration package %s contains white-box test files: %s",
				files.ImportPath,
				strings.Join(files.TestGoFiles, ", "),
			)
		}
		packageTestCount := 0
		for _, name := range files.XTestGoFiles {
			if strings.HasSuffix(name, "_internal_test.go") {
				return nil, fmt.Errorf("integration package %s contains internal test file %s", files.ImportPath, name)
			}
			path := filepath.Join(files.Dir, name)
			policy, err := integrationTagPolicy(path)
			if err != nil {
				return nil, err
			}
			if !policy.required {
				return nil, fmt.Errorf("integration test file %s must require //go:build integration", path)
			}
			declared, err := declaredTests(path, files.ImportPath)
			if err != nil {
				return nil, err
			}
			packageTestCount += len(declared)
			expected = append(expected, declared...)
		}
		if len(files.XTestGoFiles) != 0 && packageTestCount == 0 {
			return nil, fmt.Errorf("integration package %s declares no top-level tests", files.ImportPath)
		}
	}
	if err := validateCompleteIntegrationSelection(config, packages); err != nil {
		return nil, err
	}
	sort.Slice(expected, func(left, right int) bool {
		if expected[left].Package == expected[right].Package {
			return expected[left].Name < expected[right].Name
		}
		return expected[left].Package < expected[right].Package
	})
	return expected, nil
}

func declaredPackageTests(packages []packageFiles) ([]testID, error) {
	expected := make([]testID, 0)
	for _, files := range packages {
		for _, name := range append(slices.Clone(files.TestGoFiles), files.XTestGoFiles...) {
			declared, err := declaredTests(filepath.Join(files.Dir, name), files.ImportPath)
			if err != nil {
				return nil, err
			}
			expected = append(expected, declared...)
		}
	}
	return expected, nil
}

func validateCompleteIntegrationSelection(config Config, packages []packageFiles) error {
	selected := make(map[string]struct{})
	for _, files := range packages {
		for _, name := range files.XTestGoFiles {
			selected[filepath.Clean(filepath.Join(files.Dir, name))] = struct{}{}
		}
	}
	integrationRoot, err := filepath.Abs(filepath.Join(config.Dir, "integration"))
	if err != nil {
		return fmt.Errorf("resolve complete integration root: %w", err)
	}
	return filepath.WalkDir(integrationRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk complete integration contour: %w", walkErr)
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		policy, err := integrationTagPolicy(path)
		if err != nil {
			return err
		}
		if policy.positive && !policy.required {
			return fmt.Errorf("integration test file %s has a build constraint that can select it without integration", path)
		}
		if !policy.required {
			if !policy.excluded {
				return fmt.Errorf(
					"test file %s under integration/ must require //go:build integration or !integration",
					path,
				)
			}
			return nil
		}
		if _, ok := selected[filepath.Clean(path)]; !ok {
			return fmt.Errorf(
				"complete integration contour did not select %s on this platform",
				path,
			)
		}
		return nil
	})
}

func listPackages(ctx context.Context, config Config, pattern string) ([]packageFiles, error) {
	args := []string{"list", "-json", "-tags=" + strings.Join(config.Tags, ","), pattern}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = config.Dir
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("list integration packages %s: %s", pattern, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, fmt.Errorf("list integration packages %s: %w", pattern, err)
	}

	packages := make([]packageFiles, 0)
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var files packageFiles
		if err := decoder.Decode(&files); err != nil {
			if errors.Is(err, io.EOF) {
				return packages, nil
			}
			return nil, fmt.Errorf("decode integration package list for %s: %w", pattern, err)
		}
		packages = append(packages, files)
	}
}

func validateRepositoryIntegrationLayout(ctx context.Context, config Config) error {
	rootCommand := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel")
	rootCommand.Dir = config.Dir
	rootOutput, err := rootCommand.Output()
	if err != nil {
		return fmt.Errorf("resolve repository root for integration layout: %w", err)
	}
	root := strings.TrimSpace(string(rootOutput))

	listCommand := exec.CommandContext(
		ctx,
		"git",
		"-c", "core.quotePath=false",
		"ls-files", "--full-name", "--cached", "--others", "--exclude-standard", "-z", "--", "*_test.go",
	)
	listCommand.Dir = config.Dir
	output, err := listCommand.Output()
	if err != nil {
		return fmt.Errorf("list repository test files for integration layout: %w", err)
	}
	for rawPath := range bytes.SplitSeq(output, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		relativePath := filepath.ToSlash(string(rawPath))
		policy, err := integrationTagPolicy(filepath.Join(root, filepath.FromSlash(relativePath)))
		if err != nil {
			return err
		}
		allowedPath := integrationTestPathAllowed(relativePath)
		if policy.positive && !policy.required {
			return fmt.Errorf("integration test file %s has a build constraint that can select it without integration", relativePath)
		}
		if policy.required && !allowedPath {
			return fmt.Errorf("integration test file %s must live under integration/ or testkit/integration/", relativePath)
		}
		if allowedPath && !policy.required && !policy.excluded {
			return fmt.Errorf(
				"test file %s under an integration tree must require //go:build integration or !integration",
				relativePath,
			)
		}
		if allowedPath {
			if err := validateIntegrationFilePackage(
				filepath.Join(root, filepath.FromSlash(relativePath)),
				relativePath,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func integrationTestPathAllowed(path string) bool {
	return strings.HasPrefix(path, "integration/") || strings.HasPrefix(path, "testkit/integration/")
}

func validateIntegrationFilePackage(path, relativePath string) error {
	if strings.HasSuffix(relativePath, "_internal_test.go") {
		return fmt.Errorf("test file %s under an integration tree must not use the _internal_test.go suffix", relativePath)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.PackageClauseOnly)
	if err != nil {
		return fmt.Errorf("parse integration test package in %s: %w", relativePath, err)
	}
	if !strings.HasSuffix(parsed.Name.Name, "_test") {
		return fmt.Errorf(
			"test file %s under an integration tree uses white-box package %s; package name must end in _test",
			relativePath,
			parsed.Name.Name,
		)
	}
	return nil
}

func integrationTagPolicy(path string) (integrationBuildPolicy, error) {
	expression, err := readBuildConstraint(path)
	if err != nil || expression == nil {
		return integrationBuildPolicy{}, err
	}
	return integrationBuildPolicy{
		positive: referencesTag(expression, "integration", tagEnabled),
		required: !constraintCanMatch(expression, "integration", tagDisabled),
		excluded: !constraintCanMatch(expression, "integration", tagEnabled),
	}, nil
}

func readBuildConstraint(path string) (constraint.Expr, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open integration test file %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if constraint.IsGoBuild(line) {
			expression, err := constraint.Parse(line)
			if err != nil {
				return nil, fmt.Errorf("parse build constraint in %s: %w", path, err)
			}
			return expression, nil
		}
		if line != "" && !strings.HasPrefix(line, "//") {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read build constraint in %s: %w", path, err)
	}
	return nil, nil
}

func referencesTag(expression constraint.Expr, tag string, polarity tagPolarity) bool {
	switch expression := expression.(type) {
	case *constraint.TagExpr:
		return polarity == tagEnabled && expression.Tag == tag
	case *constraint.NotExpr:
		return referencesTag(expression.X, tag, oppositePolarity(polarity))
	case *constraint.AndExpr:
		return referencesTag(expression.X, tag, polarity) ||
			referencesTag(expression.Y, tag, polarity)
	case *constraint.OrExpr:
		return referencesTag(expression.X, tag, polarity) ||
			referencesTag(expression.Y, tag, polarity)
	default:
		return false
	}
}

func oppositePolarity(polarity tagPolarity) tagPolarity {
	if polarity == tagEnabled {
		return tagDisabled
	}
	return tagEnabled
}

func constraintCanMatch(expression constraint.Expr, tag string, value tagPolarity) bool {
	tags := make(map[string]struct{})
	collectConstraintTags(expression, tag, tags)
	variables := make([]string, 0, len(tags))
	for variable := range tags {
		variables = append(variables, variable)
	}
	sort.Strings(variables)
	assignments := map[string]bool{tag: value == tagEnabled}
	return anyConstraintAssignmentMatches(expression, variables, assignments, 0)
}

func collectConstraintTags(expression constraint.Expr, fixed string, tags map[string]struct{}) {
	switch expression := expression.(type) {
	case *constraint.TagExpr:
		if expression.Tag != fixed {
			tags[expression.Tag] = struct{}{}
		}
	case *constraint.NotExpr:
		collectConstraintTags(expression.X, fixed, tags)
	case *constraint.AndExpr:
		collectConstraintTags(expression.X, fixed, tags)
		collectConstraintTags(expression.Y, fixed, tags)
	case *constraint.OrExpr:
		collectConstraintTags(expression.X, fixed, tags)
		collectConstraintTags(expression.Y, fixed, tags)
	}
}

func anyConstraintAssignmentMatches(
	expression constraint.Expr,
	variables []string,
	assignments map[string]bool,
	index int,
) bool {
	if index == len(variables) {
		return expression.Eval(func(tag string) bool {
			return assignments[tag]
		})
	}
	variable := variables[index]
	assignments[variable] = false
	if anyConstraintAssignmentMatches(expression, variables, assignments, index+1) {
		return true
	}
	assignments[variable] = true
	return anyConstraintAssignmentMatches(expression, variables, assignments, index+1)
}

func declaredTests(path, importPath string) ([]testID, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse integration test file %s: %w", path, err)
	}
	declared := make([]testID, 0)
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv != nil || function.Name.Name == "TestMain" || !isTestName(function.Name.Name) {
			continue
		}
		declared = append(declared, testID{Package: importPath, Name: function.Name.Name})
	}
	return declared, nil
}

func isTestName(name string) bool {
	const prefix = "Test"
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		return true
	}
	character, _ := utf8.DecodeRuneInString(name[len(prefix):])
	return !unicode.IsLower(character)
}

func run(ctx context.Context, config Config, expected []testID) error {
	args := []string{
		"test",
		"-json",
		"-count=1",
		"-p=1",
	}
	if config.Race {
		args = append(args, "-race")
	}
	args = append(
		args,
		"-tags="+strings.Join(config.Tags, ","),
		"-timeout="+config.Timeout.String(),
		config.Package,
	)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = config.Dir
	cmd.Stderr = config.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("capture test contour output: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start test contour %s: %w", config.Package, err)
	}

	states := make(map[testID]testState, len(expected))
	for _, id := range expected {
		states[id] = testState{}
	}
	skips := make([]testID, 0)
	decodeErr := decodeEvents(stdout, config.Stdout, states, &skips)
	waitErr := cmd.Wait()
	if decodeErr != nil {
		return decodeErr
	}
	if waitErr != nil {
		return fmt.Errorf("test contour %s failed: %w", config.Package, waitErr)
	}
	if len(states) == 0 {
		return fmt.Errorf("test contour %s ran no tests", config.Package)
	}
	if len(skips) != 0 {
		return fmt.Errorf("test contour %s skipped %s", config.Package, formatTestIDs(skips))
	}
	missing := missingResults(states)
	if len(missing) != 0 {
		return fmt.Errorf("test contour %s produced no complete result for %s", config.Package, formatTestIDs(missing))
	}
	return nil
}

func decodeEvents(
	reader io.Reader,
	output io.Writer,
	states map[testID]testState,
	skips *[]testID,
) error {
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

func recordEvent(event testEvent, states map[testID]testState, skips *[]testID) {
	if event.Test == "" {
		return
	}
	id := testID{Package: event.Package, Name: event.Test}
	state := states[id]
	switch event.Action {
	case "run":
		state.ran = true
	case "pass", "fail", "skip":
		state.terminal = true
	}
	states[id] = state
	if event.Action == "skip" {
		*skips = append(*skips, id)
	}
}

func missingResults(states map[testID]testState) []testID {
	missing := make([]testID, 0)
	for id, state := range states {
		if !state.ran || !state.terminal {
			missing = append(missing, id)
		}
	}
	sort.Slice(missing, func(left, right int) bool {
		if missing[left].Package == missing[right].Package {
			return missing[left].Name < missing[right].Name
		}
		return missing[left].Package < missing[right].Package
	})
	return missing
}

func formatTestIDs(ids []testID) string {
	formatted := make([]string, len(ids))
	for index, id := range ids {
		formatted[index] = id.Package + ":" + id.Name
	}
	return strings.Join(formatted, ", ")
}
