package gotoolchain

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// minimumSteps is the anti-vacuity floor.
//
// Every count here comes from a search, and a search that matches nothing
// reports zero violations -- indistinguishable from a clean tree right up until
// it is not. This repository has more than twenty setup-go steps; if the
// enumeration collapses, that is a broken check rather than a passing one.
const minimumSteps = 20

// Run applies every rule to the tree at root.
func Run(root string) (Report, error) {
	module, err := ReadModule(filepath.Join(root, "go.mod"))
	if err != nil {
		return Report{}, err
	}
	report := Report{Toolchain: module.Toolchain, Findings: CheckModule(module)}
	if len(report.Findings) > 0 {
		return report, nil
	}

	paths, err := manifestPaths(root)
	if err != nil {
		return Report{}, err
	}
	manifests := make(map[string]Manifest)
	forwarded := make(map[string][]string)
	mentions := 0
	for _, path := range paths {
		manifest, err := ParseManifest(root, path)
		if err != nil {
			return Report{}, err
		}
		manifests[path] = manifest
		mentions += manifest.Mentions
		report.Steps += len(manifest.Steps)
		report.Findings = append(report.Findings, CheckSteps(manifest, forwarded)...)
	}
	// Forwarding counts STEPS, not references: one step forwards both keys, and
	// a count of references would read as two exempted steps.
	report.Forwarding = forwardingSteps(manifests)
	report.Deriving = report.Steps
	report.Findings = append(report.Findings, CheckForwardedDefaults(manifests, forwarded)...)

	// The census. Widening a pattern only ever covers the spellings someone
	// thought of; this pairs the structural match with a count that needs no
	// shape, so a reference the walk does not reach reddens the gate rather
	// than shrinking the sample.
	if mentions != report.Steps {
		report.Findings = append(report.Findings, Finding{File: ".github", Line: 1, Message: fmt.Sprintf(
			"%d lines under .github mention %s but %d were enumerated as steps. A reference the parser does not reach leaves its version key unjudged",
			mentions, setupGo, report.Steps)})
	}

	more, err := checkOtherDeclarations(root, module)
	if err != nil {
		return Report{}, err
	}
	report.Findings = append(report.Findings, more...)

	if report.Steps < minimumSteps {
		report.Findings = append(report.Findings, Finding{File: ".github", Line: 1, Message: fmt.Sprintf(
			"found %d setup-go steps; the scan matched almost nothing and would have passed vacuously", report.Steps)})
	}
	return report, nil
}

// forwardingSteps counts the setup-go steps that hand over an action input.
func forwardingSteps(manifests map[string]Manifest) int {
	steps := 0
	for _, manifest := range manifests {
		for _, step := range manifest.Steps {
			_, version := forwardedName(step.Version)
			_, file := forwardedName(step.VersionFile)
			if version || file {
				steps++
			}
		}
	}
	return steps
}

// manifestPaths lists every workflow and action manifest under .github.
func manifestPaths(root string) ([]string, error) {
	var paths []string
	base := filepath.Join(root, ".github")
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if ext := filepath.Ext(path); ext != ".yml" && ext != ".yaml" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(relative))
		return nil
	})
	return paths, err
}

// goVersionValue matches a version-shaped scalar, for the two declarations that
// must not restate one.
var goVersionValue = regexp.MustCompile(`^\d+\.\d+`)

// checkOtherDeclarations covers the three rules that are not about setup-go:
// golangci-lint's run.go, the secondary module's floor, and the one
// user-facing Go minimum.
func checkOtherDeclarations(root string, module Module) ([]Finding, error) {
	var findings []Finding

	// D3: golangci-lint restates no Go version. `run.go` is documented to
	// default to the go directive in go.mod, so setting it duplicates a fact --
	// and this is the copy that drifted.
	config, err := ParseManifestFile(filepath.Join(root, ".golangci.yml"))
	switch {
	case err == nil:
		if value, line := config.runGo(); goVersionValue.MatchString(value) {
			findings = append(findings, Finding{File: ".golangci.yml", Line: line, Message: fmt.Sprintf(
				"golangci-lint declares run.go (%s). Delete it; its documented default is already the go directive from go.mod.", value)})
		}
	case !os.IsNotExist(err):
		return nil, err
	}

	// D4: the root's floor never rises above the secondary module's. testkit is
	// the main module of its own build and requires the root, and Go requires a
	// main module's `go` directive to be at least its dependencies' -- so
	// testkit sitting LOWER than the root is what breaks, at the next release.
	secondary := filepath.Join(root, "testkit", "go.mod")
	if _, err := os.Stat(secondary); err == nil {
		other, err := ReadModule(secondary)
		if err != nil {
			return nil, err
		}
		if other.Go != module.Go && higher(module.Go, other.Go) == module.Go {
			findings = append(findings, Finding{File: "testkit/go.mod", Line: other.GoLine, Message: fmt.Sprintf(
				"testkit declares go %s but the root module it requires declares go %s. testkit is the main module of its own build, and a main module's go directive may not be below its dependencies' -- raise this floor with the root's.",
				other.Go, module.Go)})
		}
	}

	// D5: the one user-facing Go minimum tracks the root `go` directive. It
	// must track the FLOOR, not the toolchain: the toolchain is what CI builds
	// with, and quoting it to a reader overstates what they need installed.
	testing, err := os.ReadFile(filepath.Join(root, "TESTING.md")) //#nosec G304 -- a fixed path in this repository
	switch {
	case err == nil:
		want := "**Go " + module.Go + "+**"
		if !strings.Contains(string(testing), want) {
			findings = append(findings, Finding{File: "TESTING.md", Line: statedMinimumLine(string(testing)), Message: fmt.Sprintf(
				"the stated Go prerequisite does not match the go directive in go.mod. Write %s; the floor comes from the go directive, never from the toolchain directive.", want)})
		}
	case !os.IsNotExist(err):
		return nil, err
	}
	return findings, nil
}

// statedMinimum finds the line a stated Go prerequisite sits on.
var statedMinimum = regexp.MustCompile(`\*\*Go \d`)

func statedMinimumLine(body string) int {
	for i, line := range strings.Split(body, "\n") {
		if statedMinimum.MatchString(line) {
			return i + 1
		}
	}
	return 1
}

// higher returns the greater of two dotted versions.
func higher(a, b string) string {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		x, y := atoi(as[i]), atoi(bs[i])
		if x != y {
			if x > y {
				return a
			}
			return b
		}
	}
	if len(as) > len(bs) {
		return a
	}
	return b
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return n
		}
		n = n*10 + int(r-'0')
	}
	return n
}
