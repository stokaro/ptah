package atlas_test

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/atlas"
)

// These tests bind the published Atlas-compatibility pages to the flag surface
// `ptah-compat migrate apply` actually registers (stokaro/ptah#1354).
//
// The defect they close was a sentence that survived the code: the coverage
// pages said the pinned community binary registers no `migrate apply
// --to-version` or `--lock-name` and therefore "Ptah rejects them as unknown",
// while both flags — and `--skip-lock` with them — had been implemented. A
// reader following that sentence would reach for a workaround the tool does not
// need, and the documentation evidence contradicted the executable conformance
// evidence.
//
// The flag set is read from the command rather than listed here on purpose. A
// list would have to be edited by the same person who forgot the page, so it
// would go stale in exactly the situation the tests exist for.

// migrateApplyDocPage names a published page these tests govern.
type migrateApplyDocPage struct {
	name string
	path []string
}

// migrateApplyInventoryDocPages lists the pages that enumerate the apply flag
// surface, and so must name every flag the verb registers.
//
// `reference/atlas-commands.md` is deliberately absent: it documents the
// compatibility flags that need prose, not the whole inventory, and it names
// neither `--allow-dirty` nor `--baseline` nor `--exec-order` today. Adding it
// would be a documentation change wearing a test's clothes.
func migrateApplyInventoryDocPages() []migrateApplyDocPage {
	return []migrateApplyDocPage{
		{name: "atlas_migrate-commands", path: []string{"atlas", "migrate-commands.md"}},
		{name: "atlas_comparison", path: []string{"atlas", "comparison.md"}},
		{name: "atlas_docs-coverage", path: []string{"atlas", "docs-coverage.md"}},
	}
}

// migrateApplyClaimDocPages lists every page that discusses which apply flags
// Ptah accepts. It is wider than the inventory set because a stale rejection
// claim is harmful wherever it is written, not only on a page that also carries
// the full list.
func migrateApplyClaimDocPages() []migrateApplyDocPage {
	return []migrateApplyDocPage{
		{name: "atlas_migrate-commands", path: []string{"atlas", "migrate-commands.md"}},
		{name: "atlas_comparison", path: []string{"atlas", "comparison.md"}},
		{name: "atlas_docs-coverage", path: []string{"atlas", "docs-coverage.md"}},
		{name: "atlas_feature-matrix", path: []string{"atlas", "feature-matrix.md"}},
		{name: "atlas_overview", path: []string{"atlas", "overview.md"}},
		{name: "reference_atlas-commands", path: []string{"reference", "atlas-commands.md"}},
	}
}

// ptahRejectsClaim matches a sentence that attributes a flag rejection to Ptah
// rather than to the binary Ptah is measured against. The bounded gap keeps the
// subject and the verb in the same clause, so a page that names Ptah early and
// describes some unrelated refusal later is not caught by proximity alone.
var ptahRejectsClaim = regexp.MustCompile(`(?i)\bptah\b[^.]{0,80}\brejects?\b`)

// docFlagToken matches a long flag as the documentation spells one.
var docFlagToken = regexp.MustCompile(`--[a-zA-Z][a-zA-Z0-9-]*`)

// compatMigrateApplyLocalFlags returns the long flags `ptah-compat migrate
// apply` registers on itself, sorted, without `--help`.
//
// Local flags only: `--config`, `--env`, and `--var` are inherited from the
// root command and belong to the project-configuration surface, so a page about
// `migrate apply` is not required to name them.
func compatMigrateApplyLocalFlags(tb testing.TB) []string {
	c := qt.New(tb)
	c.Helper()
	root := atlas.NewCompatCommand("atlas")
	applyCmd, _, err := root.Find([]string{"migrate", "apply"})
	c.Assert(err, qt.IsNil)
	c.Assert(applyCmd.CommandPath(), qt.Equals, "atlas migrate apply")

	var flags []string
	applyCmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		flags = append(flags, "--"+f.Name)
	})
	flags = slices.DeleteFunc(flags, func(flag string) bool { return flag == "--help" })
	slices.Sort(flags)
	return flags
}

// unnamedApplyFlags returns the registered flags a page never mentions.
func unnamedApplyFlags(page string, flags []string) []string {
	missing := make([]string, 0)
	for _, flag := range flags {
		missing = appendUnnamedApplyFlag(missing, page, flag)
	}
	return missing
}

// appendUnnamedApplyFlag keeps one flag when the page does not spell it. It is
// a helper rather than a filter inside the loop so the test body stays free of
// control flow.
func appendUnnamedApplyFlag(missing []string, page, flag string) []string {
	if strings.Contains(page, flag) {
		return missing
	}
	return append(missing, flag)
}

// staleApplyRejectionClaims returns the sentences of a page that attribute a
// rejection to Ptah while naming only flags `migrate apply` registers.
//
// A sentence that also names a flag the verb does not register is left alone:
// that is the shape of an accurate sentence such as "the pinned community
// binary does not register `migrate apply --dir-format`, and Ptah rejects it
// there", where the rejected flag is the unregistered one and the registered
// flags around it are the supported list it is contrasted with.
func staleApplyRejectionClaims(page string, flags []string) []string {
	found := make([]string, 0)
	for sentence := range strings.SplitSeq(asOneLine(page), ". ") {
		found = appendStaleApplyRejectionClaim(found, sentence, flags)
	}
	return found
}

// appendStaleApplyRejectionClaim keeps one sentence when it makes the stale
// claim. It is a helper rather than a filter inside the loop so the test body
// stays free of control flow.
func appendStaleApplyRejectionClaim(found []string, sentence string, flags []string) []string {
	if !ptahRejectsClaim.MatchString(sentence) {
		return found
	}
	named := docFlagToken.FindAllString(sentence, -1)
	registered := 0
	for _, flag := range named {
		registered += countRegisteredFlag(flag, flags)
	}
	if registered == 0 || registered != len(named) {
		return found
	}
	return append(found, sentence)
}

// countRegisteredFlag reports 1 when flag is one the verb registers.
func countRegisteredFlag(flag string, flags []string) int {
	if slices.Contains(flags, flag) {
		return 1
	}
	return 0
}

// TestCompatMigrateApplyFlagDocs_NameEveryRegisteredFlag pins that the pages
// enumerating the apply surface still enumerate all of it. Without this a flag
// can be implemented and left undocumented, which is how `--to-version`,
// `--lock-name`, and `--skip-lock` came to be missing from a list that a
// rejection sentence then contradicted.
func TestCompatMigrateApplyFlagDocs_NameEveryRegisteredFlag(t *testing.T) {
	c := qt.New(t)
	flags := compatMigrateApplyLocalFlags(c.TB)

	// Control: a derivation that silently returned nothing would make every row
	// below pass while proving nothing. These three are the flags stokaro/ptah#1354
	// is about, so their presence also pins that the verb still implements them.
	c.Assert(flags, qt.Contains, "--to-version")
	c.Assert(flags, qt.Contains, "--lock-name")
	c.Assert(flags, qt.Contains, "--skip-lock")

	for _, tt := range migrateApplyInventoryDocPages() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			page := readCompatDocPage(c.TB, tt.path)
			missing := unnamedApplyFlags(page, flags)
			c.Assert(missing, qt.HasLen, 0, qt.Commentf("flags the page never names: %q", missing))
		})
	}
}

// TestCompatMigrateApplyFlagDocs_NeverCallAnImplementedFlagRejected pins the
// defect itself: no governed page may tell a reader that Ptah turns away a flag
// `migrate apply` registers.
func TestCompatMigrateApplyFlagDocs_NeverCallAnImplementedFlagRejected(t *testing.T) {
	c := qt.New(t)
	flags := compatMigrateApplyLocalFlags(c.TB)
	c.Assert(flags, qt.Contains, "--to-version")

	for _, tt := range migrateApplyClaimDocPages() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			page := readCompatDocPage(c.TB, tt.path)
			claims := staleApplyRejectionClaims(page, flags)
			c.Assert(claims, qt.HasLen, 0, qt.Commentf("stale rejection claims: %q", claims))
		})
	}
}
