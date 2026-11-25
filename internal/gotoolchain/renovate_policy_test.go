package gotoolchain_test

import (
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// renovateConfig is the dependency-automation policy, read from the repository
// root because that is where Renovate reads it.
const renovateConfig = "../../renovate.json"

// The two go.mod depTypes this package's subject is split across.
//
// They are separable only here: both directives carry the dependency name `go`,
// and Renovate tells them apart by depType alone. The titles it writes are the
// evidence for which is which -- stokaro/ptah#2732 is "update go toolchain
// directive to v1.27.1" and stokaro/ptah#223 is "update dependency go to
// v1.26.5".
const (
	goDirectiveDepType = "golang"
	toolchainDepType   = "toolchain"
)

// indirectDepType is the go.mod depType Renovate gives a requirement no file
// here imports.
const indirectDepType = "indirect"

// renovateRule is one packageRules entry, read for what it does to go.mod.
type renovateRule struct {
	MatchManagers     []string `json:"matchManagers"`
	MatchDepTypes     []string `json:"matchDepTypes"`
	MatchUpdateTypes  []string `json:"matchUpdateTypes"`
	MatchPackageNames []string `json:"matchPackageNames"`
	AllowedVersions   string   `json:"allowedVersions"`
	Enabled           *bool    `json:"enabled"`
}

// TestRenovate_LeavesTheCompatibilityFloorToAHuman keeps a bot off the released
// contract.
//
// go.mod carries two Go versions with different lifecycles. `toolchain` is what
// CI builds with and moves on every patch release. The `go` directive is the
// published compatibility floor: the module is a released import path, so
// raising it forces every consumer onto a newer language version, and AGENTS.md
// says it moves on a human decision.
//
// Renovate moved it once, in stokaro/ptah#223, under a title that reads like any
// other bump. This asserts the policy now refuses that update.
func TestRenovate_LeavesTheCompatibilityFloorToAHuman(t *testing.T) {
	c := qt.New(t)

	disabling := rulesDisabling(c, goDirectiveDepType)

	c.Assert(len(disabling) > 0, qt.IsTrue,
		qt.Commentf("%s must disable the gomod %q depType, or a bot can propose raising the published "+
			"compatibility floor as an ordinary dependency bump", renovateConfig, goDirectiveDepType))
}

// TestRenovate_StillMovesTheToolchain is the other half.
//
// A rule that disabled both directives would satisfy the test above while
// freezing the version CI builds with, which is the one that must keep moving:
// it is how a standard-library advisory is cleared without touching the floor.
func TestRenovate_StillMovesTheToolchain(t *testing.T) {
	c := qt.New(t)

	disabling := rulesDisabling(c, toolchainDepType)

	c.Assert(disabling, qt.HasLen, 0,
		qt.Commentf("%s disables the gomod %q depType, so the version CI builds with stops being "+
			"proposed and a standard-library advisory has to be cleared by hand",
			renovateConfig, toolchainDepType))
}

// TestRenovate_LeavesAnIndirectMajorToTheImporter keeps a bot off a module path
// it does not choose.
//
// A major Go upgrade is a path change: foo becomes foo/v2. For an indirect
// requirement the path is decided by whatever imports it, so the rewrite
// produces a go.mod the import tree does not support, and postUpdateOptions
// cannot repair it -- gomodTidy needs a file that parses, and the rewrite is
// what stops it parsing. stokaro/ptah#3522 replaced github.com/yuin/goldmark-emoji
// with a second copy of github.com/yuin/goldmark-emoji/v2 and failed 32 checks
// on the duplicate require.
func TestRenovate_LeavesAnIndirectMajorToTheImporter(t *testing.T) {
	c := qt.New(t)

	disabling := rulesDisablingUpdate(c, indirectDepType, "major")

	c.Assert(len(disabling) > 0, qt.IsTrue,
		qt.Commentf("%s must disable major updates for the gomod %q depType, or a bot can rewrite "+
			"a module path chosen by an importer and hand go.mod a graph that does not parse",
			renovateConfig, indirectDepType))
}

// TestRenovate_StillMovesAnIndirectPatch is the other half.
//
// A rule disabling every indirect update would satisfy the test above while
// freezing the requirements a security advisory usually lands in, which is the
// half that has to keep moving.
func TestRenovate_StillMovesAnIndirectPatch(t *testing.T) {
	c := qt.New(t)

	disabling := rulesDisablingUpdate(c, indirectDepType, "patch")

	c.Assert(disabling, qt.HasLen, 0,
		qt.Commentf("%s disables patch updates for the gomod %q depType, so an advisory in a "+
			"transitive requirement has to be cleared by hand", renovateConfig, indirectDepType))
}

// TestRenovate_ProposesNoDevelopmentBuild keeps go.mod off a module's
// development branch.
//
// A Go module version ending in -dev is a build of that module's development
// branch. stokaro/ptah#3399 pinned google.golang.org/grpc to a v1.85.0-dev
// pseudo-version on purpose, to clear an advisory no release had fixed yet.
// Renovate reads an unstable current version as consent to more unstable ones:
// stokaro/ptah#3595 moved it to v1.86.0-dev, and every module that requires
// ptah.run inherited an unreleased grpc. The rows are the two shapes: the -dev
// tag itself, and the pseudo-version of a commit after it.
func TestRenovate_ProposesNoDevelopmentBuild(t *testing.T) {
	for _, version := range []string{
		"v1.86.0-dev",
		"v1.85.0-dev.0.20260924231546-c0ef4986f448",
	} {
		t.Run(version, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(goVersionRefused(c, version), qt.IsTrue,
				qt.Commentf("%s lets Renovate propose the development build %s for every Go module",
					renovateConfig, version))
		})
	}
}

// TestRenovate_StillProposesReleases is the other half.
//
// A rule refusing every version with a suffix would satisfy the test above
// while freezing the modules that publish no tags and move by pseudo-version,
// such as golang.org/x/exp, and the release that supersedes a deliberate pin.
func TestRenovate_StillProposesReleases(t *testing.T) {
	for _, version := range []string{
		"v1.85.0",
		"v0.0.0-20261124000051-d6646c509b6d",
	} {
		t.Run(version, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(goVersionRefused(c, version), qt.IsFalse,
				qt.Commentf("%s refuses %s for every Go module, so Renovate stops proposing it",
					renovateConfig, version))
		})
	}
}

// goVersionRefused reports whether a gomod rule that applies to every module
// excludes the version through a negated allowedVersions pattern, the form
// Renovate spells "!/pattern/".
func goVersionRefused(c *qt.C, version string) bool {
	c.Helper()

	for _, rule := range packageRules(c) {
		pattern, negated := strings.CutPrefix(rule.AllowedVersions, "!/")
		pattern, closed := strings.CutSuffix(pattern, "/")
		everyModule := slices.Contains(rule.MatchManagers, "gomod") && len(rule.MatchPackageNames) == 0
		if negated && closed && everyModule && regexp.MustCompile(pattern).MatchString(version) {
			return true
		}
	}
	return false
}

// packageRules reads the policy's package rules.
func packageRules(c *qt.C) []renovateRule {
	c.Helper()

	body, err := os.ReadFile(renovateConfig)
	c.Assert(err, qt.IsNil)

	var config struct {
		PackageRules []renovateRule `json:"packageRules"`
	}
	c.Assert(json.Unmarshal(body, &config), qt.IsNil)
	c.Assert(len(config.PackageRules) > 0, qt.IsTrue,
		qt.Commentf("%s carries no package rules, so every assertion here would be vacuous", renovateConfig))
	return config.PackageRules
}

// rulesDisabling returns the gomod rules that turn updates off for one depType,
// whatever the update is.
func rulesDisabling(c *qt.C, depType string) []renovateRule {
	c.Helper()

	rules := packageRules(c)
	matching := make([]renovateRule, 0, len(rules))
	for _, rule := range rules {
		disabled := rule.Enabled != nil && !*rule.Enabled
		if disabled && slices.Contains(rule.MatchManagers, "gomod") && slices.Contains(rule.MatchDepTypes, depType) {
			matching = append(matching, rule)
		}
	}
	return matching
}

// rulesDisablingUpdate narrows that to one kind of update.
//
// A rule naming no matchUpdateTypes disables every kind, so it counts for each
// one asked about: read the other way, a blanket rule would answer "major is
// disabled" while quietly disabling patch too, and the pair of tests above
// exists to separate those.
func rulesDisablingUpdate(c *qt.C, depType, updateType string) []renovateRule {
	c.Helper()

	disabling := rulesDisabling(c, depType)
	matching := make([]renovateRule, 0, len(disabling))
	for _, rule := range disabling {
		if len(rule.MatchUpdateTypes) == 0 || slices.Contains(rule.MatchUpdateTypes, updateType) {
			matching = append(matching, rule)
		}
	}
	return matching
}
