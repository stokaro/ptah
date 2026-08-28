package featureinventory_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/featureinventory"
)

// shippedRoots builds one root per shipped command tree.
//
// The strict root is a third construction rather than an environment variable
// set around the second, because the policy is resolved once at the ptah-compat
// process boundary and passed in. Constructing it directly is what the binary
// does, and it keeps the census free of mutable process state.
func shippedRoots() map[featureinventory.Tree]*cobra.Command {
	return map[featureinventory.Tree]*cobra.Command{
		featureinventory.TreeNative:       root.NewRootCommand(),
		featureinventory.TreeCompat:       atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.Full()),
		featureinventory.TreeCompatStrict: atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.StrictCE()),
	}
}

// shippedCensus is the census every gate below measures against.
func shippedCensus(c *qt.C) *featureinventory.Census {
	c.Helper()
	census, err := featureinventory.NewCensus(shippedRoots())
	c.Assert(err, qt.IsNil)
	return census
}

// repoRoot resolves the checkout under test.
func repoRoot(c *qt.C) string {
	c.Helper()
	dir, err := featureinventory.RepoRoot()
	c.Assert(err, qt.IsNil)
	return dir
}

// shippedInventory loads the committed inventory document.
func shippedInventory(c *qt.C) *featureinventory.Inventory {
	c.Helper()
	inventory, err := featureinventory.LoadInventory(repoRoot(c))
	c.Assert(err, qt.IsNil)
	return inventory
}

// documentedReferences scans every tracked document for command references.
func documentedReferences(c *qt.C) []featureinventory.Reference {
	c.Helper()
	dir := repoRoot(c)
	programs, err := featureinventory.Programs(dir)
	c.Assert(err, qt.IsNil)
	launchers := featureinventory.Launchers(programs)
	c.Assert(len(launchers) > 0, qt.IsTrue)

	docs, err := featureinventory.DocFiles(dir)
	c.Assert(err, qt.IsNil)

	var references []featureinventory.Reference
	for _, doc := range docs {
		found, err := featureinventory.ScanDocument(dir, doc, launchers)
		c.Assert(err, qt.IsNil)
		references = append(references, found...)
	}
	// The control every documentation gate in this repository needs and several
	// have gone without: a scanner that stopped finding invocations would
	// compare an empty set against the trees and report the same success it
	// reports on a clean tree.
	c.Assert(len(references) > 100, qt.IsTrue,
		qt.Commentf("the document scan found %d command references; a scan that found almost none would pass every check below without reading anything", len(references)))
	return references
}

// messages renders findings for an assertion's failure output.
func messages(findings []featureinventory.Finding) []string {
	out := make([]string, 0, len(findings))
	for _, finding := range findings {
		out = append(out, finding.String())
	}
	return out
}

// commandReferenceBody reads the committed generated command reference.
func commandReferenceBody(c *qt.C) string {
	c.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(c), filepath.FromSlash(featureinventory.CommandReferencePath)))
	c.Assert(err, qt.IsNil)
	return string(body)
}

// fixtureRoots builds a small tree the self-tests drive.
//
// A fixture rather than the shipped trees, because a self-test has to be able to
// plant the defect it claims to detect. Planting one in the real tree would mean
// editing the command layer from a test.
//
// Every command carries the argument validator its real counterpart carries, and
// that is load-bearing rather than tidy. A cobra command with no validator
// accepts any positional word, so a fixture built from bare commands asserts
// that `ptah schema render ./models` is a legal line -- which the shipped binary
// refuses at exit 2 with `unexpected positional arguments ["./models"]`. The
// fixture was making a false claim about the product, and a self-test written
// against it was measuring nothing.
//
// The tree carries one of each answer a validator gives:
//
//   - `schema` and `schema render` refuse every positional word, which is what
//     cmdutil.NoPositionalArgs does on 21 real commands;
//   - `oci tag` requires two, so a probe offering one word would call a correct
//     line stale;
//   - `sql` is a namespace whose last verb has been removed. It is a leaf and it
//     still refuses `lint`, which is the removal shape check 2 exists for and
//     the one a Leaf-based reading cannot see.
func fixtureRoots() map[featureinventory.Tree]*cobra.Command {
	native := &cobra.Command{Use: "ptah", Short: "fixture root"}
	group := &cobra.Command{Use: "schema", Short: "fixture group", Args: cobra.NoArgs}
	render := &cobra.Command{Use: "render", Short: "fixture verb", Args: cobra.NoArgs, Run: func(*cobra.Command, []string) {}}
	render.Flags().String("root-dir", "", "fixture flag")
	render.Flags().String("hidden-flag", "", "fixture hidden flag")
	_ = render.Flags().MarkHidden("hidden-flag")
	group.AddCommand(render)
	native.AddCommand(group)

	oci := &cobra.Command{Use: "oci", Short: "fixture group", Args: cobra.NoArgs}
	tag := &cobra.Command{Use: "tag <oci-reference> <tag>", Short: "fixture verb taking two arguments",
		Args: cobra.MinimumNArgs(2), Run: func(*cobra.Command, []string) {}}
	oci.AddCommand(tag)
	native.AddCommand(oci)

	// The namespace whose last verb is gone: no subcommands, and still refusing
	// every word the verb used to answer to.
	native.AddCommand(&cobra.Command{Use: "sql", Short: "fixture emptied group", Args: cobra.NoArgs})

	compat := &cobra.Command{Use: "ptah-compat", Short: "fixture compat root"}
	migrate := &cobra.Command{Use: "migrate", Short: "fixture compat group", Args: cobra.NoArgs}
	apply := &cobra.Command{Use: "apply", Short: "fixture compat verb", Args: cobra.MaximumNArgs(1)}
	migrate.AddCommand(apply)
	compat.AddCommand(migrate)

	// The strict fixture carries one of each answer: `migrate apply` is gone,
	// `migrate gated` is registered and hidden. A fixture with only the first
	// would let a self-test's mutation of the gated column match nothing and
	// report success -- which it did, once, before this command was added.
	compatGated := &cobra.Command{Use: "gated", Short: "fixture gated verb", Run: func(*cobra.Command, []string) {}}
	migrate.AddCommand(compatGated)

	strict := &cobra.Command{Use: "ptah-compat", Short: "fixture compat root"}
	strictMigrate := &cobra.Command{Use: "migrate", Short: "fixture compat group", Args: cobra.NoArgs}
	strictGated := &cobra.Command{Use: "gated", Short: "fixture gated verb", Hidden: true, Run: func(*cobra.Command, []string) {}}
	strictMigrate.AddCommand(strictGated)
	strict.AddCommand(strictMigrate)

	return map[featureinventory.Tree]*cobra.Command{
		featureinventory.TreeNative:       native,
		featureinventory.TreeCompat:       compat,
		featureinventory.TreeCompatStrict: strict,
	}
}

// fixtureCensus is the census the self-tests plant defects against.
func fixtureCensus(c *qt.C) *featureinventory.Census {
	c.Helper()
	census, err := featureinventory.NewCensus(fixtureRoots())
	c.Assert(err, qt.IsNil)
	return census
}

// writeInventory writes an inventory document into a throwaway directory and
// returns the parsed result.
//
// The self-tests need a document they can break, and they drive the same
// LoadInventory the gate drives rather than building an Inventory value
// directly: a fixture that skipped the parser would leave the parser's own
// bounds -- the numbered-section bound, the required columns, the duplicate-ID
// rule -- asserted by nothing.
func writeInventory(c *qt.C, rows string) *featureinventory.Inventory {
	c.Helper()
	dir := c.TempDir()
	writeDocument(c, dir, fixtureHeader+rows)
	inventory, err := featureinventory.LoadInventory(dir)
	c.Assert(err, qt.IsNil)
	return inventory
}

// fixtureHeader carries a table BEFORE the bounding heading, so every self-test
// below also asserts that the header tables are not read as data. The real
// document opens with six of them, and the first has a column named
// `Feature ID`.
var fixtureHeader = "# Fixture inventory\n\n## Columns\n\n| Column | Meaning |\n| --- | --- |\n| Feature ID | not a row |\n\n" +
	featureinventory.InventoryHeading + "\n\n### 1. Fixture section\n\n| " + joinColumns() + " |\n| " + delimiterRow() + "\n"

// inventoryRow renders one table row with the required number of columns.
func inventoryRow(id, surface string) string {
	cells := make([]string, 0, len(featureinventory.RequiredColumns))
	cells = append(cells, id, surface)
	for range len(featureinventory.RequiredColumns) - 2 {
		cells = append(cells, "fixture")
	}
	return "| " + strings.Join(cells, " | ") + " |\n"
}

// completionShellRows claims the four shells cobra's completion group carries.
//
// They are written out here rather than looped, so a fixture claiming "every
// path" says which paths it means. The five of them are exactly what the Phase 1
// audit's walk could not see.
func completionShellRows() string {
	return inventoryRow("completion-bash", "`ptah completion bash`") +
		inventoryRow("completion-fish", "`ptah completion fish`") +
		inventoryRow("completion-powershell", "`ptah completion powershell`") +
		inventoryRow("completion-zsh", "`ptah completion zsh`") +
		inventoryRow("compat-completion-bash", "`ptah-compat completion bash`") +
		inventoryRow("compat-completion-fish", "`ptah-compat completion fish`") +
		inventoryRow("compat-completion-powershell", "`ptah-compat completion powershell`") +
		inventoryRow("compat-completion-zsh", "`ptah-compat completion zsh`")
}

// joinColumns renders the required column headings for a fixture table.
func joinColumns() string { return strings.Join(featureinventory.RequiredColumns, " | ") }

// delimiterRow renders the separator beneath them.
func delimiterRow() string {
	return strings.TrimSuffix(strings.Repeat("--- | ", len(featureinventory.RequiredColumns)), " ")
}

// writeDocument writes a body to the inventory path inside dir.
func writeDocument(c *qt.C, dir, body string) {
	c.Helper()
	c.Assert(os.MkdirAll(filepath.Join(dir, "docs"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, filepath.FromSlash(featureinventory.InventoryPath)), []byte(body), 0o600), qt.IsNil)
}

// shippedDocuments reads every tracked document of this checkout.
func shippedDocuments(c *qt.C) *featureinventory.Documents {
	c.Helper()
	docs, err := featureinventory.NewDocuments(repoRoot(c))
	c.Assert(err, qt.IsNil)
	return docs
}

// sitePath renders the tracked file one site slug resolves from.
func sitePath(slug string) string { return featureinventory.SiteRoot + "/" + slug + ".md" }

// inventoryPageRow renders one row with its page columns set and every other
// column filled, so a page self-test states only what it is about.
func inventoryPageRow(id, surface, canonical, example string) string {
	set := map[string]string{
		"Feature ID":     id,
		"Public surface": surface,
		"Canonical page": canonical,
		"Example":        example,
	}
	cells := make([]string, 0, len(featureinventory.RequiredColumns))
	for _, column := range featureinventory.RequiredColumns {
		cells = append(cells, cellOr(set, column))
	}
	return "| " + strings.Join(cells, " | ") + " |\n"
}

// cellOr keeps the conditional out of the row builder.
func cellOr(set map[string]string, column string) string {
	if value, ok := set[column]; ok {
		return value
	}
	return "fixture"
}

// writePagesFixture writes an inventory and a small site into a throwaway
// repository, and returns both halves the page check compares.
//
// A real git repository, because the document reader lists tracked files rather
// than walking the filesystem -- this checkout's parent holds other worktrees of
// the same repository, and a walk descends into them. A fixture that skipped
// `git add` would leave the reader with nothing to read and every claim
// unresolvable.
func writePagesFixture(c *qt.C, rows string, pages map[string]string) (*featureinventory.Documents, *featureinventory.Inventory) {
	c.Helper()
	dir := c.TempDir()
	writeDocument(c, dir, fixtureHeader+rows)
	// One page nothing claims, so a fixture whose only document is a repository
	// file still has a site for the reader to resolve against.
	writeTrackedFile(c, dir, sitePath("index"), "# Fixture site\n")
	for name, body := range pages {
		writeTrackedFile(c, dir, name, body)
	}
	initFixtureRepository(c, dir)

	docs, err := featureinventory.NewDocuments(dir)
	c.Assert(err, qt.IsNil)
	inventory, err := featureinventory.LoadInventory(dir)
	c.Assert(err, qt.IsNil)
	return docs, inventory
}

// writeTrackedFile writes one file, creating its directories.
func writeTrackedFile(c *qt.C, dir, name, body string) {
	c.Helper()
	full := filepath.Join(dir, filepath.FromSlash(name))
	c.Assert(os.MkdirAll(filepath.Dir(full), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(full, []byte(body), 0o600), qt.IsNil)
}

// initFixtureRepository makes a throwaway checkout whose index lists every file
// written into it.
func initFixtureRepository(c *qt.C, dir string) {
	c.Helper()
	initialize, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("git init: %s", initialize))

	added, err := exec.Command("git", "-C", dir, "add", "-A").CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("git add: %s", added))
}

// shippedSurfaces reads every non-command surface from this checkout.
func shippedSurfaces(c *qt.C) *featureinventory.Surfaces {
	c.Helper()
	surfaces, err := featureinventory.NewSurfaces(repoRoot(c))
	c.Assert(err, qt.IsNil)
	return surfaces
}

// programDirs lists the discovered program directories.
func programDirs(surfaces *featureinventory.Surfaces) []string {
	dirs := make([]string, 0, len(surfaces.Programs))
	for _, program := range surfaces.Programs {
		dirs = append(dirs, program.Dir)
	}
	return dirs
}

// formatValues returns one declared format set's values.
func formatValues(surfaces *featureinventory.Surfaces, name string) []string {
	for _, list := range surfaces.Formats {
		if list.Name == name {
			return list.Values
		}
	}
	return nil
}

// fixtureSurfaces is a three-member surface set the self-tests plant defects
// against.
func fixtureSurfaces() *featureinventory.Surfaces {
	return &featureinventory.Surfaces{
		Packages: []string{"go.5x5.cz/ptah/core/ast"},
		Programs: []featureinventory.Program{{Dir: "cmd/ptah", Drives: featureinventory.TreeNative}},
		Formats: []featureinventory.FormatList{
			{Name: "demo", Source: "a fixture", Values: []string{"one", "two"}},
		},
	}
}

// writeLedger writes a public-API ledger into a throwaway directory.
func writeLedger(c *qt.C, dir, body string) {
	c.Helper()
	c.Assert(os.MkdirAll(filepath.Join(dir, "docs"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "docs", "public_api.md"), []byte(body), 0o600), qt.IsNil)
}

// errorText renders an error for a regexp assertion, answering "" for no error.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// orEmpty anchors a wanted pattern, answering the empty-string pattern when a
// case expects no error at all.
func orEmpty(pattern string) string {
	if pattern == "" {
		return "^$"
	}
	return pattern
}

// writeTargetEnv names the file `scripts/check-command-reference.sh --write`
// rewrites. Unset, the write path runs against a throwaway copy instead, so it
// is exercised by an ordinary test run and not only by the repair.
const writeTargetEnv = "FEATURE_INVENTORY_COMMAND_REFERENCE_OUT"

// writeTarget resolves that file, copying the committed document into a
// temporary directory when nothing is named.
func writeTarget(c *qt.C) string {
	c.Helper()
	if named := os.Getenv(writeTargetEnv); named != "" {
		return named
	}
	target := filepath.Join(c.TempDir(), "command-reference.md")
	c.Assert(os.WriteFile(target, []byte(commandReferenceBody(c)), 0o600), qt.IsNil)
	return target
}
