// Command goversionguard audits every actions/setup-go step against the
// repository's canonical Go toolchain version.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	yaml "go.yaml.in/yaml/v3"
	"golang.org/x/mod/modfile"

	"go.5x5.cz/ptah/internal/pathguard"
)

const (
	setupGoRepository        = "actions/setup-go"
	compositeActionPath      = ".github/actions/ptah/action.yml"
	compositeVersionSelector = "${{ inputs.go-version }}"
)

type setupGoPin struct {
	path     string
	job      string
	line     int
	selector string
	value    string
}

type goModulePin struct {
	path    string
	version string
}

type workflowAudit struct {
	pins      []setupGoPin
	jobCounts map[string]map[string]int
}

type localActionManifest struct {
	path        string
	displayPath string
}

func main() {
	root := flag.String("root", ".", "repository root")
	expected := flag.String("version", "", "expected Go version")
	flag.Parse()
	if flag.NArg() != 0 || *expected == "" {
		fmt.Fprintln(os.Stderr, "usage: goversionguard -root <repository> -version <go-version>")
		os.Exit(2)
	}
	modulePins, moduleErr := auditGoModules(*root, *expected)
	for _, pin := range modulePins {
		fmt.Printf("%s: Go %s\n", pin.path, pin.version)
	}
	workflowPins, workflowErr := auditCanonicalWorkflows(*root, *expected)
	actionPins, actionErr := auditCompositeActions(*root, *expected)
	pins := slices.Concat(workflowPins, actionPins)
	for _, pin := range pins {
		if pin.selector == "go-version-file" {
			fmt.Printf("%s:%d actions/setup-go: go-version-file %s\n", pin.path, pin.line, pin.value)
			continue
		}
		fmt.Printf("%s:%d actions/setup-go go-version: Go %s\n", pin.path, pin.line, pin.value)
	}
	if auditErr := errors.Join(moduleErr, workflowErr, actionErr); auditErr != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", auditErr)
		os.Exit(1)
	}
}

func auditGoModules(root, expected string) ([]goModulePin, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path != root && entry.IsDir() && ignoredRepositoryDirectory(root, path, entry.Name()) {
			return filepath.SkipDir
		}
		if entry.IsDir() || entry.Name() != "go.mod" {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list Go modules: %w", err)
	}
	slices.Sort(paths)

	var pins []goModulePin
	var auditErrors []error
	for _, path := range paths {
		relative := relativePath(root, path)
		contents, readErr := os.ReadFile(path)
		if readErr != nil {
			auditErrors = append(auditErrors, fmt.Errorf("read %s: %w", relative, readErr))
			continue
		}
		module, parseErr := modfile.Parse(path, contents, nil)
		if parseErr != nil {
			auditErrors = append(auditErrors, fmt.Errorf("parse %s: %w", relative, parseErr))
			continue
		}
		version := ""
		if module.Go != nil {
			version = module.Go.Version
		}
		pins = append(pins, goModulePin{path: relative, version: version})
		if version != expected {
			auditErrors = append(auditErrors, fmt.Errorf(
				"%s uses Go %s; expected %s from go.mod",
				relative,
				missingLabel(version),
				expected,
			))
		}
	}
	return pins, errors.Join(auditErrors...)
}

func ignoredRepositoryDirectory(root, path, name string) bool {
	if name == ".git" || name == "node_modules" || name == "vendor" {
		return true
	}
	switch relativePath(root, path) {
	case ".claude/worktrees", ".codex/worktrees":
		return true
	case ".codex/worktress": // Legacy local agent directory spelling.
		return true
	default:
		return false
	}
}

func missingLabel(value string) string {
	if value == "" {
		return "<missing>"
	}
	return value
}

func auditWorkflows(root, expected string) ([]setupGoPin, error) {
	audit, err := auditWorkflowInventory(root, expected)
	return audit.pins, err
}

func auditWorkflowInventory(root, expected string) (workflowAudit, error) {
	paths, err := workflowFilePaths(root)
	if err != nil {
		return workflowAudit{}, err
	}

	audit := workflowAudit{jobCounts: make(map[string]map[string]int, len(paths))}
	var auditErrors []error
	for _, path := range paths {
		relative := filepath.ToSlash(relativePath(root, path))
		filePins, jobCounts, fileErr := auditWorkflow(root, path, expected)
		audit.pins = append(audit.pins, filePins...)
		audit.jobCounts[relative] = jobCounts
		if fileErr != nil {
			auditErrors = append(auditErrors, fileErr)
		}
	}
	return audit, errors.Join(auditErrors...)
}

func workflowFilePaths(root string) ([]string, error) {
	workflowsRoot := filepath.Join(root, ".github", "workflows")
	var paths []string
	err := filepath.WalkDir(workflowsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !isWorkflowFile(entry.Name()) {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	slices.Sort(paths)
	return paths, nil
}

func auditCanonicalWorkflows(root, expected string) ([]setupGoPin, error) {
	audit, auditErr := auditWorkflowInventory(root, expected)
	actualCounts := audit.jobCounts

	expectedCounts := expectedWorkflowSetupGoCounts()
	var countErrors []error
	for _, path := range slices.Sorted(maps.Keys(expectedCounts)) {
		actualJobs := actualCounts[path]
		expectedJobs := expectedCounts[path]
		for _, job := range slices.Sorted(maps.Keys(expectedJobs)) {
			actualCount, found := actualJobs[job]
			if !found {
				countErrors = append(countErrors, fmt.Errorf(
					"%s job %s is missing from the workflow; expected %d setup-go steps",
					path,
					job,
					expectedJobs[job],
				))
			} else if actualCount != expectedJobs[job] {
				countErrors = append(countErrors, fmt.Errorf(
					"%s job %s declares %d setup-go steps; expected %d",
					path,
					job,
					actualCount,
					expectedJobs[job],
				))
			}
			delete(actualJobs, job)
		}
		for _, job := range slices.Sorted(maps.Keys(actualJobs)) {
			countErrors = append(countErrors, fmt.Errorf(
				"%s job %s declares %d setup-go steps but is missing from the canonical inventory",
				path,
				job,
				actualJobs[job],
			))
		}
		delete(actualCounts, path)
	}
	for _, path := range slices.Sorted(maps.Keys(actualCounts)) {
		actualJobs := actualCounts[path]
		if len(actualJobs) == 0 {
			countErrors = append(countErrors, fmt.Errorf(
				"%s is missing from the canonical workflow inventory",
				path,
			))
		}
		for _, job := range slices.Sorted(maps.Keys(actualJobs)) {
			countErrors = append(countErrors, fmt.Errorf(
				"%s job %s declares %d setup-go steps but is missing from the canonical inventory",
				path,
				job,
				actualJobs[job],
			))
		}
	}
	return audit.pins, errors.Join(auditErr, errors.Join(countErrors...))
}

func expectedWorkflowSetupGoCounts() map[string]map[string]int {
	return map[string]map[string]int{
		".github/workflows/atlas-oracle.yml": {
			"complete-integration":    0,
			"differential-and-corpus": 1,
		},
		".github/workflows/capability-matrix-nightly.yml": {
			"cells":  1,
			"report": 1,
			"suite":  1,
		},
		".github/workflows/capability-matrix.yml": {
			"cells":         1,
			"documentation": 1,
			"presets":       1,
			"probe":         1,
			"report":        1,
		},
		".github/workflows/docs.yml": {
			"build":   0,
			"changes": 0,
			"deploy":  0,
			"style":   0,
		},
		".github/workflows/export-acceptance.yml": {
			"export-acceptance": 1,
		},
		".github/workflows/go-fuzz.yml": {
			"fuzz": 1,
		},
		".github/workflows/go-integration-tests.yml": {
			"integration-tests": 1,
		},
		".github/workflows/go-lint.yml": {
			"go-lint": 1,
		},
		".github/workflows/go-security.yml": {
			"coverage":    1,
			"gosec":       1,
			"govulncheck": 1,
		},
		".github/workflows/go-unit-tests.yml": {
			"unit-tests":          1,
			"windows-publication": 1,
		},
		".github/workflows/go-version-consistency.yml": {
			"check-go-version": 1,
		},
		".github/workflows/install-smoke.yml": {
			"documented-install": 1,
		},
		".github/workflows/ptah-action-smoke.yml": {
			"smoke": 1,
		},
		".github/workflows/release.yml": {
			"goreleaser-check": 1,
			"release":          1,
		},
		".github/workflows/testkit-sync.yml": {
			"sync": 1,
		},
		".github/workflows/testkit.yml": {
			"containers":    1,
			"published-pin": 1,
			"sqlite":        1,
		},
	}
}

func isWorkflowFile(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".yml" || ext == ".yaml"
}

func auditWorkflow(root, path, expected string) ([]setupGoPin, map[string]int, error) {
	relative := relativePath(root, path)
	document, err := parseYAMLFile(relative, path)
	if err != nil {
		return nil, nil, err
	}
	if err := validateMappingKeys(relative, document); err != nil {
		return nil, nil, err
	}

	var pins []setupGoPin
	jobCounts := make(map[string]int)
	var auditErrors []error
	for _, job := range workflowJobs(document) {
		jobCounts[job.name] = 0
		for _, step := range job.steps {
			pin, setupStep, stepErr := auditStep(relative, step, expected)
			if setupStep {
				jobCounts[job.name]++
			}
			if stepErr != nil {
				auditErrors = append(auditErrors, stepErr)
				continue
			}
			if setupStep {
				pin.job = job.name
				pins = append(pins, pin)
			}
		}
	}
	return pins, jobCounts, errors.Join(auditErrors...)
}

func auditCompositeAction(root, expected string) ([]setupGoPin, error) {
	path := filepath.Join(root, filepath.FromSlash(compositeActionPath))
	manifest, err := containedActionManifest(root, path, compositeActionPath)
	if err != nil {
		return nil, err
	}
	document, err := parseYAMLFile(manifest.displayPath, manifest.path)
	if err != nil {
		return nil, err
	}
	if err := validateMappingKeys(compositeActionPath, document); err != nil {
		return nil, err
	}
	rootMapping := documentMapping(document)
	if rootMapping == nil {
		return nil, fmt.Errorf("%s:1: action must be a YAML mapping", compositeActionPath)
	}

	var auditErrors []error
	if err := auditCompositeVersionInput(rootMapping, expected); err != nil {
		auditErrors = append(auditErrors, err)
	}
	steps, stepsErr := compositeSteps(rootMapping)
	if stepsErr != nil {
		auditErrors = append(auditErrors, stepsErr)
		return nil, errors.Join(auditErrors...)
	}

	var pins []setupGoPin
	setupCount := 0
	for _, step := range steps {
		inspected, setupStep, stepErr := inspectSetupGoStep(compositeActionPath, step)
		if stepErr != nil {
			auditErrors = append(auditErrors, stepErr)
			continue
		}
		if !setupStep {
			continue
		}
		setupCount++
		if inspected.pin.selector != "go-version" || inspected.pin.value != compositeVersionSelector {
			auditErrors = append(auditErrors, stepError(
				compositeActionPath,
				inspected.value,
				"must consume %s through go-version",
				compositeVersionSelector,
			))
			continue
		}
		pins = append(pins, inspected.pin)
	}
	if setupCount != 1 {
		auditErrors = append(auditErrors, fmt.Errorf(
			"%s: declares %d setup-go steps; expected exactly one",
			compositeActionPath,
			setupCount,
		))
	}
	return pins, errors.Join(auditErrors...)
}

func auditCompositeActions(root, expected string) ([]setupGoPin, error) {
	canonicalPins, canonicalErr := auditCompositeAction(root, expected)
	additionalPins, additionalErr := auditAdditionalCompositeActions(root, expected)
	return slices.Concat(canonicalPins, additionalPins), errors.Join(canonicalErr, additionalErr)
}

func auditAdditionalCompositeActions(root, expected string) ([]setupGoPin, error) {
	manifests, discoveryErr := discoverRepositoryActionManifests(root)
	workflowReferences, workflowErr := workflowLocalActionReferences(root)
	referencedManifests, referenceErr := resolveLocalActionReferences(root, workflowReferences)
	queue := slices.Concat(manifests, referencedManifests)

	seen := make(map[string]struct{}, len(queue))
	var pins []setupGoPin
	var auditErrors []error
	for len(queue) != 0 {
		manifest := queue[0]
		queue = queue[1:]
		if _, found := seen[manifest.path]; found {
			continue
		}
		seen[manifest.path] = struct{}{}

		steps, references, actionErr := inspectLocalActionManifest(manifest)
		if actionErr != nil {
			auditErrors = append(auditErrors, actionErr)
		} else if manifest.displayPath != compositeActionPath {
			actionPins, pinErr := auditActionSetupGo(manifest.displayPath, steps, expected)
			pins = append(pins, actionPins...)
			if pinErr != nil {
				auditErrors = append(auditErrors, pinErr)
			}
		}
		nestedManifests, nestedErr := resolveLocalActionReferences(root, references)
		queue = append(queue, nestedManifests...)
		if nestedErr != nil {
			auditErrors = append(auditErrors, nestedErr)
		}
	}
	return pins, errors.Join(discoveryErr, workflowErr, referenceErr, errors.Join(auditErrors...))
}

func discoverRepositoryActionManifests(root string) ([]localActionManifest, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path != root && entry.IsDir() && ignoredRepositoryDirectory(root, path, entry.Name()) {
			return filepath.SkipDir
		}
		if entry.IsDir() || !isActionManifest(entry.Name()) {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list local actions: %w", err)
	}
	slices.Sort(paths)

	manifests := make([]localActionManifest, 0, len(paths))
	var containmentErrors []error
	for _, path := range paths {
		displayPath := filepath.ToSlash(relativePath(root, path))
		manifest, containmentErr := containedActionManifest(root, path, displayPath)
		if containmentErr != nil {
			containmentErrors = append(containmentErrors, containmentErr)
			continue
		}
		manifests = append(manifests, manifest)
	}
	return manifests, errors.Join(containmentErrors...)
}

func isActionManifest(name string) bool {
	return name == "action.yml" || name == "action.yaml"
}

func containedActionManifest(root, path, displayPath string) (localActionManifest, error) {
	repositoryRoot, err := pathguard.ResolveWithinRoot(root, "")
	if err != nil {
		return localActionManifest{}, fmt.Errorf("resolve repository root for %s: %w", displayPath, err)
	}
	resolvedPath, err := pathguard.ResolveWithinRoot(path, repositoryRoot)
	if err != nil {
		return localActionManifest{}, fmt.Errorf(
			"resolve action manifest %s within repository root: %w",
			displayPath,
			err,
		)
	}
	return localActionManifest{
		path:        resolvedPath,
		displayPath: filepath.ToSlash(displayPath),
	}, nil
}

func inspectLocalActionManifest(manifest localActionManifest) ([]*yaml.Node, []string, error) {
	document, err := parseYAMLFile(manifest.displayPath, manifest.path)
	if err != nil {
		return nil, nil, err
	}
	if err := validateMappingKeys(manifest.displayPath, document); err != nil {
		return nil, nil, err
	}
	rootMapping := documentMapping(document)
	if rootMapping == nil {
		return nil, nil, fmt.Errorf("%s:1: action must be a YAML mapping", manifest.displayPath)
	}
	steps, composite, err := localCompositeSteps(manifest.displayPath, rootMapping)
	if err != nil || !composite {
		return nil, nil, err
	}

	var references []string
	for _, step := range steps {
		if reference, local := localActionReference(step); local {
			references = append(references, reference)
		}
	}
	return steps, references, nil
}

func auditActionSetupGo(path string, steps []*yaml.Node, expected string) ([]setupGoPin, error) {
	var pins []setupGoPin
	var auditErrors []error
	for _, step := range steps {
		pin, setupStep, err := auditStep(path, step, expected)
		if err != nil {
			auditErrors = append(auditErrors, err)
		} else if setupStep {
			pins = append(pins, pin)
		}
	}
	return pins, errors.Join(auditErrors...)
}

func workflowLocalActionReferences(root string) ([]string, error) {
	paths, err := workflowFilePaths(root)
	if err != nil {
		// auditCanonicalWorkflows owns the repository-level requirement that the
		// workflow directory exists. Keep this helper usable for isolated action
		// audits while preserving that fail-closed check in main.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var references []string
	var auditErrors []error
	for _, path := range paths {
		displayPath := filepath.ToSlash(relativePath(root, path))
		document, parseErr := parseYAMLFile(displayPath, path)
		if parseErr != nil {
			auditErrors = append(auditErrors, parseErr)
			continue
		}
		if validationErr := validateMappingKeys(displayPath, document); validationErr != nil {
			auditErrors = append(auditErrors, validationErr)
			continue
		}
		for _, job := range workflowJobs(document) {
			for _, step := range job.steps {
				if reference, local := localActionReference(step); local {
					references = append(references, reference)
				}
			}
		}
	}
	return references, errors.Join(auditErrors...)
}

func localActionReference(step *yaml.Node) (string, bool) {
	if step.Kind != yaml.MappingNode {
		return "", false
	}
	uses := mappingValue(step, "uses")
	if uses == nil || uses.Kind != yaml.ScalarNode || uses.Tag != "!!str" {
		return "", false
	}
	_, local := localActionRepositoryPath(uses.Value)
	return uses.Value, local
}

func localActionRepositoryPath(reference string) (string, bool) {
	if path, found := strings.CutPrefix(reference, "./"); found {
		return path, true
	}
	if path, found := strings.CutPrefix(reference, "$/"); found {
		return path, true
	}
	return "", false
}

func resolveLocalActionReferences(root string, references []string) ([]localActionManifest, error) {
	manifests := make([]localActionManifest, 0, len(references))
	var resolutionErrors []error
	for _, reference := range references {
		manifest, err := resolveLocalActionReference(root, reference)
		if err != nil {
			resolutionErrors = append(resolutionErrors, err)
			continue
		}
		manifests = append(manifests, manifest)
	}
	return manifests, errors.Join(resolutionErrors...)
}

func resolveLocalActionReference(root, reference string) (localActionManifest, error) {
	repositoryPath, local := localActionRepositoryPath(reference)
	if !local {
		return localActionManifest{}, fmt.Errorf("action reference %q is not repository-local", reference)
	}
	if strings.HasPrefix(reference, "$/") && strings.Contains(repositoryPath, "@") {
		return localActionManifest{}, fmt.Errorf("local action reference %q must not include a ref suffix", reference)
	}
	repositoryRoot, err := pathguard.ResolveWithinRoot(root, "")
	if err != nil {
		return localActionManifest{}, fmt.Errorf("resolve repository root for local action %q: %w", reference, err)
	}
	directory := filepath.Clean(filepath.Join(repositoryRoot, filepath.FromSlash(repositoryPath)))
	relativeDirectory, err := filepath.Rel(repositoryRoot, directory)
	if err != nil {
		return localActionManifest{}, fmt.Errorf("resolve local action %q: %w", reference, err)
	}
	if relativeDirectory == ".." || strings.HasPrefix(relativeDirectory, ".."+string(filepath.Separator)) ||
		filepath.IsAbs(relativeDirectory) {
		return localActionManifest{}, fmt.Errorf("local action reference %q escapes the repository root", reference)
	}
	resolvedDirectory, err := pathguard.ResolveWithinRoot(directory, repositoryRoot)
	if err != nil {
		return localActionManifest{}, fmt.Errorf(
			"resolve local action reference %q within repository root: %w",
			reference,
			err,
		)
	}
	directory = resolvedDirectory

	var matches []localActionManifest
	for _, name := range []string{"action.yml", "action.yaml"} {
		candidate := filepath.Join(directory, name)
		displayPath := filepath.ToSlash(filepath.Clean(filepath.Join(repositoryPath, name)))
		manifest, containmentErr := containedActionManifest(repositoryRoot, candidate, displayPath)
		if containmentErr != nil {
			return localActionManifest{}, containmentErr
		}
		info, statErr := os.Stat(manifest.path)
		if statErr == nil && !info.IsDir() {
			matches = append(matches, manifest)
			continue
		}
		if statErr != nil && !os.IsNotExist(statErr) {
			return localActionManifest{}, fmt.Errorf("inspect local action %q: %w", reference, statErr)
		}
	}
	if len(matches) != 1 {
		return localActionManifest{}, fmt.Errorf(
			"local action reference %q resolves to %d action manifests; expected exactly one",
			reference,
			len(matches),
		)
	}
	return matches[0], nil
}

func localCompositeSteps(path string, rootMapping *yaml.Node) ([]*yaml.Node, bool, error) {
	runs := mappingValue(rootMapping, "runs")
	if runs == nil || runs.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("%s: runs must be a mapping", path)
	}
	using := mappingValue(runs, "using")
	if using == nil || using.Kind != yaml.ScalarNode || using.Tag != "!!str" {
		return nil, false, fmt.Errorf("%s: runs.using must be an untagged string", path)
	}
	if using.Value != "composite" {
		return nil, false, nil
	}
	steps := mappingValue(runs, "steps")
	if steps == nil || steps.Kind != yaml.SequenceNode {
		return nil, true, fmt.Errorf("%s: composite action steps must be a sequence", path)
	}
	return steps.Content, true, nil
}

func auditCompositeVersionInput(rootMapping *yaml.Node, expected string) error {
	inputs := mappingValue(rootMapping, "inputs")
	goVersion := mappingValue(inputs, "go-version")
	defaultValue := mappingValue(goVersion, "default")
	if defaultValue == nil {
		return fmt.Errorf("%s: go-version input must declare a default", compositeActionPath)
	}
	if defaultValue.Kind != yaml.ScalarNode || defaultValue.Tag != "!!str" {
		return fmt.Errorf(
			"%s:%d: go-version default must be an untagged string",
			compositeActionPath,
			defaultValue.Line,
		)
	}
	if defaultValue.Value != expected {
		return fmt.Errorf(
			"%s:%d: go-version default uses Go %s; expected %s from go.mod",
			compositeActionPath,
			defaultValue.Line,
			defaultValue.Value,
			expected,
		)
	}
	return nil
}

func compositeSteps(rootMapping *yaml.Node) ([]*yaml.Node, error) {
	runs := mappingValue(rootMapping, "runs")
	if runs == nil || runs.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s: runs must be a mapping", compositeActionPath)
	}
	using := mappingValue(runs, "using")
	if using == nil || using.Kind != yaml.ScalarNode || using.Tag != "!!str" || using.Value != "composite" {
		return nil, fmt.Errorf("%s: runs.using must be composite", compositeActionPath)
	}
	steps := mappingValue(runs, "steps")
	if steps == nil || steps.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("%s: composite action steps must be a sequence", compositeActionPath)
	}
	return steps.Content, nil
}

func parseYAMLFile(displayPath, path string) (*yaml.Node, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", displayPath, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(contents, &document); err != nil {
		return nil, fmt.Errorf("%s: parse YAML: %w", displayPath, err)
	}
	return &document, nil
}

func documentMapping(document *yaml.Node) *yaml.Node {
	if document == nil || document.Kind != yaml.DocumentNode || len(document.Content) == 0 {
		return nil
	}
	if document.Content[0].Kind != yaml.MappingNode {
		return nil
	}
	return document.Content[0]
}

func validateMappingKeys(path string, node *yaml.Node) error {
	var validationErrors []error
	if node.Kind == yaml.AliasNode {
		validationErrors = append(validationErrors, fmt.Errorf(
			"%s:%d: YAML aliases are not supported by the Go version guard",
			path,
			node.Line,
		))
	}
	if strings.HasPrefix(node.Tag, "!") && !strings.HasPrefix(node.Tag, "!!") {
		validationErrors = append(validationErrors, fmt.Errorf(
			"%s:%d: custom YAML tag %q is not supported by the Go version guard",
			path,
			node.Line,
			node.Tag,
		))
	}
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]*yaml.Node, len(node.Content)/2)
		for index := 0; index+1 < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode {
				validationErrors = append(validationErrors, fmt.Errorf(
					"%s:%d: non-scalar YAML mapping keys are not supported by the Go version guard",
					path,
					key.Line,
				))
				continue
			}
			if key.Value == "<<" || key.Tag == "!!merge" {
				validationErrors = append(validationErrors, fmt.Errorf(
					"%s:%d: YAML merge keys are not supported by the Go version guard",
					path,
					key.Line,
				))
			}
			identity := key.Tag + "\x00" + key.Value
			if previous := seen[identity]; previous != nil {
				validationErrors = append(validationErrors, fmt.Errorf(
					"%s:%d: duplicate mapping key %q (first declared on line %d)",
					path,
					key.Line,
					key.Value,
					previous.Line,
				))
			} else {
				seen[identity] = key
			}
		}
	}
	for _, child := range node.Content {
		if err := validateMappingKeys(path, child); err != nil {
			validationErrors = append(validationErrors, err)
		}
	}
	return errors.Join(validationErrors...)
}

type workflowJob struct {
	name  string
	steps []*yaml.Node
}

func workflowJobs(document *yaml.Node) []workflowJob {
	if document.Kind != yaml.DocumentNode || len(document.Content) == 0 {
		return nil
	}
	jobs := mappingValue(document.Content[0], "jobs")
	if jobs == nil || jobs.Kind != yaml.MappingNode {
		return nil
	}
	var workflowJobs []workflowJob
	for index := 0; index+1 < len(jobs.Content); index += 2 {
		jobKey := jobs.Content[index]
		job := jobs.Content[index+1]
		jobSteps := mappingValue(job, "steps")
		var steps []*yaml.Node
		if jobSteps != nil && jobSteps.Kind == yaml.SequenceNode {
			steps = jobSteps.Content
		}
		workflowJobs = append(workflowJobs, workflowJob{name: jobKey.Value, steps: steps})
	}
	return workflowJobs
}

func auditStep(path string, step *yaml.Node, expected string) (setupGoPin, bool, error) {
	inspected, setupStep, err := inspectSetupGoStep(path, step)
	if err != nil || !setupStep {
		return setupGoPin{}, setupStep, err
	}
	selector := inspected.selector
	if strings.Contains(selector.value.Value, "${{") {
		return setupGoPin{}, true, stepError(path, selector.value, "dynamic version selectors are not supported")
	}
	if selector.key.Value == "go-version" && selector.value.Value != expected {
		return setupGoPin{}, true, stepError(
			path,
			selector.value,
			"uses Go %s; expected %s from go.mod",
			selector.value.Value,
			expected,
		)
	}
	if selector.key.Value == "go-version-file" && selector.value.Value != "go.mod" {
		return setupGoPin{}, true, stepError(
			path,
			selector.value,
			"uses go-version-file %s; expected go.mod",
			selector.value.Value,
		)
	}
	return inspected.pin, true, nil
}

type inspectedSetupGo struct {
	pin      setupGoPin
	selector selectorEntry
	value    *yaml.Node
}

func inspectSetupGoStep(path string, step *yaml.Node) (inspectedSetupGo, bool, error) {
	if step.Kind != yaml.MappingNode {
		return inspectedSetupGo{}, false, nil
	}
	uses := mappingValue(step, "uses")
	if uses == nil {
		return inspectedSetupGo{}, false, nil
	}
	if uses.Kind != yaml.ScalarNode || uses.Tag != "!!str" {
		return inspectedSetupGo{}, false, stepError(path, uses, "action reference must be an untagged string")
	}
	if !isSetupGoAction(uses.Value) {
		return inspectedSetupGo{}, false, nil
	}
	if !validSetupGoAction(uses.Value) {
		return inspectedSetupGo{}, false, stepError(path, uses, "invalid setup-go action reference %q", uses.Value)
	}

	with := mappingValue(step, "with")
	if with == nil {
		return inspectedSetupGo{}, true, stepError(path, uses, "declares 0 version selectors; expected exactly one")
	}
	if with.Kind != yaml.MappingNode || with.Tag != "!!map" {
		return inspectedSetupGo{}, true, stepError(path, with, "setup-go with must be an untagged mapping")
	}

	selectors := selectorEntries(with)
	if len(selectors) != 1 {
		return inspectedSetupGo{}, true, stepError(
			path,
			uses,
			"declares %d version selectors; expected exactly one",
			len(selectors),
		)
	}
	selector := selectors[0]
	if selector.key.Tag != "!!str" {
		return inspectedSetupGo{}, true, stepError(path, selector.key, "version selector key must be an untagged string")
	}
	if selector.value.Kind != yaml.ScalarNode || selector.value.Tag != "!!str" {
		return inspectedSetupGo{}, true, stepError(path, selector.value, "version selector must be an untagged string")
	}
	return inspectedSetupGo{
		pin: setupGoPin{
			path:     path,
			line:     uses.Line,
			selector: selector.key.Value,
			value:    selector.value.Value,
		},
		selector: selector,
		value:    selector.value,
	}, true, nil
}

func validSetupGoAction(value string) bool {
	repository, suffix, found := strings.Cut(value, "@")
	return found && strings.EqualFold(repository, setupGoRepository) && suffix != "" &&
		!strings.ContainsAny(suffix, " \t\r\n") && !strings.Contains(suffix, "${{")
}

func isSetupGoAction(value string) bool {
	repository, _, found := strings.Cut(value, "@")
	return found && strings.EqualFold(repository, setupGoRepository)
}

type selectorEntry struct {
	key   *yaml.Node
	value *yaml.Node
}

func selectorEntries(mapping *yaml.Node) []selectorEntry {
	var entries []selectorEntry
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		key := mapping.Content[index]
		if key.Kind != yaml.ScalarNode || (key.Value != "go-version" && key.Value != "go-version-file") {
			continue
		}
		entries = append(entries, selectorEntry{key: key, value: mapping.Content[index+1]})
	}
	return entries
}

func mappingValue(mapping *yaml.Node, name string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		key := mapping.Content[index]
		if key.Kind == yaml.ScalarNode && key.Value == name {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func stepError(path string, node *yaml.Node, format string, args ...any) error {
	return fmt.Errorf("%s:%d actions/setup-go: %s", path, node.Line, fmt.Sprintf(format, args...))
}

func relativePath(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relative)
}
