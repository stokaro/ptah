package gotoolchain_test

import (
	"encoding/json"
	"os"
	"slices"
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

// renovateRule is one packageRules entry, read for what it does to go.mod.
type renovateRule struct {
	MatchManagers []string `json:"matchManagers"`
	MatchDepTypes []string `json:"matchDepTypes"`
	Enabled       *bool    `json:"enabled"`
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

// rulesDisabling returns the gomod rules that turn updates off for one depType.
func rulesDisabling(c *qt.C, depType string) []renovateRule {
	c.Helper()

	body, err := os.ReadFile(renovateConfig)
	c.Assert(err, qt.IsNil)

	var config struct {
		PackageRules []renovateRule `json:"packageRules"`
	}
	c.Assert(json.Unmarshal(body, &config), qt.IsNil)
	c.Assert(len(config.PackageRules) > 0, qt.IsTrue,
		qt.Commentf("%s carries no package rules, so every assertion here would be vacuous", renovateConfig))

	matching := make([]renovateRule, 0, len(config.PackageRules))
	for _, rule := range config.PackageRules {
		disabled := rule.Enabled != nil && !*rule.Enabled
		if disabled && slices.Contains(rule.MatchManagers, "gomod") && slices.Contains(rule.MatchDepTypes, depType) {
			matching = append(matching, rule)
		}
	}
	return matching
}
