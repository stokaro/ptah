// Package testcontour runs build-tagged integration packages and rejects
// skipped or incomplete tests.
//
// A run covers the whole contour, or one part of it when Config names a shard:
// the runner then lists every test with go test -list, partitions the listing
// with package shard, writes a manifest of the partition and runs only its
// part. The gate that proves the parts add up to the contour is shard.Verify.
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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"ptah.run/internal/testcontour/shard"
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

	// Shards splits the contour into that many parts, and Shard selects the
	// part this run executes, counted from 1. Both zero run the whole contour.
	Shard  int
	Shards int
	// TimingsFile names the timing report that balances the partition of a
	// sharded run; see shard.Plan. It changes which part a test lands in, never
	// whether it runs, so a report that cannot be read is reported on Stderr
	// and the run balances by test count instead. A run of the whole contour
	// has no partition to balance and refuses one.
	TimingsFile string
	// Report, when set, receives the raw go test -json stream.
	Report io.Writer
	// Manifest, when set, receives the sharded run's shard.Manifest as JSON
	// before any test runs. A run of the whole contour writes none.
	Manifest io.Writer
}

type testEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

// maxRunPattern bounds one -run argument. Linux refuses a single argument
// above 128 KiB and Windows a whole command line above 32 KiB, so a part that
// selects many tests of one package runs it more than once instead.
const maxRunPattern = 24 << 10

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
		expected, err = validateIntegrationPackages(ctx, config, packages)
		if err != nil {
			return err
		}
	} else {
		expected, err = declaredPackageTests(packages)
		if err != nil {
			return err
		}
	}
	if config.Shards == 0 {
		return run(ctx, config, config.Package, expected, [][]string{{config.Package}})
	}
	return runShard(ctx, config, expected)
}

// runShard lists the contour, partitions it and runs the part config.Shard
// names. The listing has to agree with the test functions the source declares,
// so a parse of go test -list output that lost a name cannot shrink the
// contour every shard partitions.
func runShard(ctx context.Context, config Config, declared []testID) error {
	listed, err := listTests(ctx, config)
	if err != nil {
		return err
	}
	if err := compareListing(listed, declared); err != nil {
		return err
	}
	partition, err := shard.Plan(listed, readTimings(config), config.Shards)
	if err != nil {
		return fmt.Errorf("partition test contour %s: %w", config.Package, err)
	}
	if config.Manifest != nil {
		manifest := shard.Manifest{Shard: config.Shard, Shards: config.Shards, Partition: partition}
		if err := json.NewEncoder(config.Manifest).Encode(manifest); err != nil {
			return fmt.Errorf("write shard manifest: %w", err)
		}
	}

	part := partition.Parts[config.Shard-1]
	var assigned []testID
	for _, test := range part.Tests() {
		assigned = append(assigned, testID{Package: test.Package, Name: test.Name})
	}
	invocations := part.Invocations(listed, maxRunPattern)
	_, _ = fmt.Fprintf(
		config.Stderr,
		"testcontour: shard %d of %d runs %d of %d tests in %d go test invocations, planned at %s; partition %s, balanced by %s\n",
		config.Shard, config.Shards, len(assigned), len(listed), len(invocations),
		time.Duration(part.WeightMS)*time.Millisecond, partition.Digest, partition.Weighting,
	)
	label := fmt.Sprintf("%s shard %d of %d", config.Package, config.Shard, config.Shards)
	return run(ctx, config, label, assigned, invocations)
}

// readTimings reads the report that balances the partition. Every shard reads
// the same bytes, so every shard that cannot read them falls back to the test
// count the same way, and the fallback is named in the partition's Weighting.
func readTimings(config Config) shard.Timings {
	if config.TimingsFile == "" {
		return shard.Timings{}
	}
	file, err := os.Open(config.TimingsFile)
	if err != nil {
		_, _ = fmt.Fprintf(config.Stderr, "testcontour: no contour timings, balancing by test count: %s\n", err)
		return shard.Timings{Source: "no readable timings"}
	}
	defer file.Close()
	timings, err := shard.ReadTimings(file)
	if err != nil {
		_, _ = fmt.Fprintf(config.Stderr, "testcontour: unreadable contour timings, balancing by test count: %s\n", err)
		return shard.Timings{Source: "unreadable timings"}
	}
	return timings
}

// listTests asks the test binaries which tests they hold. It refuses a
// benchmark, fuzz target or example: the contour runs tests, and a part that
// selects tests by name would leave the others behind.
func listTests(ctx context.Context, config Config) ([]shard.Test, error) {
	args := []string{"test", "-list", ".", "-json"}
	if config.Race {
		args = append(args, "-race")
	}
	args = append(args, "-tags="+strings.Join(config.Tags, ","), config.Package)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = config.Dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list tests of %s: %w: %s", config.Package, err, strings.TrimSpace(stderr.String()))
	}

	var listed []shard.Test
	var refused []string
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var event testEvent
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode test listing of %s: %w", config.Package, err)
		}
		if event.Action != "output" || event.Test != "" || event.Package == "" {
			continue
		}
		name := strings.TrimRight(event.Output, "\r\n")
		switch {
		case isTestName(name) && isIdentifier(name):
			listed = append(listed, shard.Test{Package: event.Package, Name: name})
		case isIdentifier(name) && (isFunctionName(name, "Benchmark") ||
			isFunctionName(name, "Fuzz") || isFunctionName(name, "Example")):
			refused = append(refused, event.Package+":"+name)
		}
	}
	if len(refused) != 0 {
		return nil, fmt.Errorf("test contour %s runs tests only, and lists %s", config.Package, strings.Join(refused, ", "))
	}
	return listed, nil
}

func compareListing(listed []shard.Test, declared []testID) error {
	inListing := make(map[testID]bool, len(listed))
	for _, test := range listed {
		inListing[testID{Package: test.Package, Name: test.Name}] = true
	}
	inSource := make(map[testID]bool, len(declared))
	var unlisted []testID
	for _, id := range declared {
		inSource[id] = true
		if !inListing[id] {
			unlisted = append(unlisted, id)
		}
	}
	var undeclared []testID
	for id := range inListing {
		if !inSource[id] {
			undeclared = append(undeclared, id)
		}
	}
	sortTestIDs(undeclared)
	if len(unlisted) == 0 && len(undeclared) == 0 {
		return nil
	}
	return fmt.Errorf(
		"go test -list and the declared test functions disagree; declared but not listed: [%s]; listed but not declared: [%s]",
		formatTestIDs(unlisted), formatTestIDs(undeclared),
	)
}

func isIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for index, character := range name {
		if character != '_' && !unicode.IsLetter(character) && (index == 0 || !unicode.IsDigit(character)) {
			return false
		}
	}
	return true
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
	if config.Shard < 0 || config.Shards < 0 || (config.Shards == 0) != (config.Shard == 0) ||
		config.Shard > config.Shards {
		return Config{}, fmt.Errorf("shard %d of %d does not name a part of the contour", config.Shard, config.Shards)
	}
	if config.Manifest != nil && config.Shards == 0 {
		return Config{}, errors.New("a shard manifest is written only by a sharded run")
	}
	if config.TimingsFile != "" && config.Shards == 0 {
		return Config{}, errors.New("contour timings balance a sharded run, and a run of the whole contour reads none")
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

func validateIntegrationPackages(ctx context.Context, config Config, packages []packageFiles) ([]testID, error) {
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
	if err := validateCompleteIntegrationSelection(ctx, config, packages); err != nil {
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

// validateCompleteIntegrationSelection checks that go list selected every test
// file under the integration tree. The files come from git, never from a walk
// of the directory, which would also descend into a checkout parked below it.
func validateCompleteIntegrationSelection(ctx context.Context, config Config, packages []packageFiles) error {
	selected := make(map[string]struct{})
	for _, files := range packages {
		for _, name := range files.XTestGoFiles {
			selected[filepath.Clean(filepath.Join(files.Dir, name))] = struct{}{}
		}
	}
	dir, err := filepath.Abs(config.Dir)
	if err != nil {
		return fmt.Errorf("resolve complete integration root: %w", err)
	}
	// Without --full-name the paths are relative to config.Dir, the base go
	// list reports package directories against.
	listCommand := exec.CommandContext(
		ctx,
		"git",
		"-c", "core.quotePath=false",
		"ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "integration/",
	)
	listCommand.Dir = config.Dir
	output, err := listCommand.Output()
	if err != nil {
		return fmt.Errorf("list integration test files: %w", err)
	}
	for rawPath := range bytes.SplitSeq(output, []byte{0}) {
		if !bytes.HasSuffix(rawPath, []byte("_test.go")) {
			continue
		}
		path := filepath.Join(dir, filepath.FromSlash(string(rawPath)))
		policy, err := integrationTagPolicy(path)
		if err != nil {
			return err
		}
		if policy.positive && !policy.required {
			return fmt.Errorf("integration test file %s has a build constraint that can select it without integration", path)
		}
		if !policy.required {
			return fmt.Errorf(
				"test file %s under integration/ must require //go:build integration",
				path,
			)
		}
		if _, ok := selected[filepath.Clean(path)]; !ok {
			return fmt.Errorf(
				"complete integration contour did not select %s on this platform",
				path,
			)
		}
	}
	return nil
}

func listPackages(ctx context.Context, config Config, pattern string) ([]packageFiles, error) {
	args := []string{"list", "-json", "-tags=" + strings.Join(config.Tags, ","), pattern}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = config.Dir
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
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
		"ls-files", "--full-name", "--cached", "--others", "--exclude-standard", "-z", "--", "*_test.go", "go.mod", "*/go.mod",
	)
	listCommand.Dir = config.Dir
	output, err := listCommand.Output()
	if err != nil {
		return fmt.Errorf("list repository test files for integration layout: %w", err)
	}
	// Module boundaries come from the same git inventory as tests. A linked
	// checkout parked below this one contributes neither modules nor tests.
	var modules []string
	for rawPath := range bytes.SplitSeq(output, []byte{0}) {
		path := filepath.ToSlash(string(rawPath))
		if filepath.Base(path) == "go.mod" {
			modules = append(modules, strings.TrimSuffix(path, "go.mod"))
		}
	}
	for rawPath := range bytes.SplitSeq(output, []byte{0}) {
		if len(rawPath) == 0 || !bytes.HasSuffix(rawPath, []byte("_test.go")) {
			continue
		}
		relativePath := filepath.ToSlash(string(rawPath))
		policy, err := integrationTagPolicy(filepath.Join(root, filepath.FromSlash(relativePath)))
		if err != nil {
			return err
		}
		allowedPath := integrationTestPathAllowed(relativePath, modules)
		if policy.positive && !policy.required {
			return fmt.Errorf("integration test file %s has a build constraint that can select it without integration", relativePath)
		}
		if policy.required && !allowedPath {
			return fmt.Errorf("integration test file %s must live under integration/", relativePath)
		}
		if allowedPath && !policy.required {
			return fmt.Errorf(
				"test file %s under an integration tree must require //go:build integration",
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

func integrationTestPathAllowed(path string, modules []string) bool {
	// The nearest module owns a test. A directory named integration elsewhere
	// does not enroll a package in the module's integration contour.
	var owner string
	for _, module := range modules {
		if len(module) > len(owner) && strings.HasPrefix(path, module) {
			owner = module
		}
	}
	return strings.HasPrefix(strings.TrimPrefix(path, owner), "integration/")
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
	return isFunctionName(name, "Test")
}

// isFunctionName reports whether name is one go test registers under prefix:
// the prefix alone, or the prefix followed by anything but a lower-case letter.
func isFunctionName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		return true
	}
	character, _ := utf8.DecodeRuneInString(name[len(prefix):])
	return !unicode.IsLower(character)
}

// run executes each invocation in turn, so a part that needs several go test
// commands still runs all of them, and judges the tests they report against
// expected. label names the contour in errors.
//
// A test the invocations ran beyond expected is refused. expected comes from
// the declared test functions, and go test runs more than those: an example
// with an output comment is a test the declarations do not name. A sharded run
// refuses one earlier, when its listing names anything but a test, so on the
// whole contour this is the only check that sees it.
func run(ctx context.Context, config Config, label string, expected []testID, invocations [][]string) error {
	states := make(map[testID]testState, len(expected))
	assigned := make(map[testID]bool, len(expected))
	for _, id := range expected {
		states[id] = testState{}
		assigned[id] = true
	}
	skips := make([]testID, 0)
	var failures []error
	for _, packages := range invocations {
		if err := invoke(ctx, config, label, packages, states, &skips); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	if len(states) == 0 {
		return fmt.Errorf("test contour %s ran no tests", label)
	}
	if len(skips) != 0 {
		return fmt.Errorf("test contour %s skipped %s", label, formatTestIDs(skips))
	}
	missing := missingResults(states)
	if len(missing) != 0 {
		return fmt.Errorf("test contour %s produced no complete result for %s", label, formatTestIDs(missing))
	}
	var unassigned []testID
	for id := range states {
		if !strings.Contains(id.Name, "/") && !assigned[id] {
			unassigned = append(unassigned, id)
		}
	}
	if len(unassigned) != 0 {
		sortTestIDs(unassigned)
		return fmt.Errorf("test contour %s ran tests it was not given: %s", label, formatTestIDs(unassigned))
	}
	return nil
}

// invoke runs one go test command over the arguments that select its
// packages and tests, copying the event stream to config.Report.
func invoke(
	ctx context.Context,
	config Config,
	label string,
	selection []string,
	states map[testID]testState,
	skips *[]testID,
) error {
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
	)
	args = append(args, selection...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = config.Dir
	cmd.Stderr = config.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("capture test contour output: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start test contour %s: %w", label, err)
	}
	var events io.Reader = stdout
	if config.Report != nil {
		events = io.TeeReader(stdout, config.Report)
	}
	decodeErr := decodeEvents(events, config.Stdout, states, skips)
	waitErr := cmd.Wait()
	if decodeErr != nil {
		return decodeErr
	}
	if waitErr != nil {
		return fmt.Errorf("test contour %s failed: %w", label, waitErr)
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
	sortTestIDs(missing)
	return missing
}

func sortTestIDs(ids []testID) {
	sort.Slice(ids, func(left, right int) bool {
		if ids[left].Package == ids[right].Package {
			return ids[left].Name < ids[right].Name
		}
		return ids[left].Package < ids[right].Package
	})
}

func formatTestIDs(ids []testID) string {
	formatted := make([]string, len(ids))
	for index, id := range ids {
		formatted[index] = id.Package + ":" + id.Name
	}
	return strings.Join(formatted, ", ")
}
