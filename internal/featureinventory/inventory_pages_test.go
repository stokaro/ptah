package featureinventory_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/featureinventory"
)

// TestInventoryPages_EveryPageClaimResolvesAndCarriesItsSurface is the page
// check. The gate is scripts/check-inventory-pages.sh.
//
// It is the sixth analyzer in this package and the one the other five left a
// hole for: checks 1 and 4 key on `Feature ID` and `Public surface`, the two
// document checks read the documents rather than the register's claims about
// them, and check 5 compares a generated block with itself. Nothing read
// `Canonical page` or `Example`, which is the register's central promise. Five
// rows named a page that did not carry their surface at all while every other
// number in the file reproduced exactly.
func TestInventoryPages_EveryPageClaimResolvesAndCarriesItsSurface(t *testing.T) {
	c := qt.New(t)
	docs := shippedDocuments(c)
	inventory := shippedInventory(c)

	findings := featureinventory.CheckInventoryPages(docs, inventory)
	c.Assert(messages(findings), qt.HasLen, 0)
}

// TestInventoryPages_ReadsEnoughRowsAndPagesToBeChecking is the control.
//
// Each of the three rules can fall silent on its own: a slug reader that
// resolved nothing would report every page missing rather than nothing, but a
// column reader that matched no cell, or an identifier rule that found no
// identifiers, would report exactly what a clean register reports. The counts
// are asserted so that going quiet is a failure.
func TestInventoryPages_ReadsEnoughRowsAndPagesToBeChecking(t *testing.T) {
	c := qt.New(t)
	docs := shippedDocuments(c)
	inventory := shippedInventory(c)

	canonical, examples, identified := featureinventory.PageReferenceCount(inventory)
	c.Assert(canonical > 200, qt.IsTrue, qt.Commentf("%d rows name a canonical page", canonical))
	c.Assert(examples > 100, qt.IsTrue, qt.Commentf("%d rows claim an example", examples))
	c.Assert(identified > 150, qt.IsTrue, qt.Commentf("%d rows carry a command path or a PTAH_* variable", identified))
	c.Assert(len(docs.Slugs()) > 50, qt.IsTrue, qt.Commentf("%d site slugs resolved", len(docs.Slugs())))
}

// TestInventoryPagesSelftest_NoticesAPageClaimThatIsNotTrue plants each defect.
//
// One row per rule, and the fourth is the one the reviewer of this work found
// five times: a page that exists, is named as canonical, and does not mention
// the surface anywhere.
func TestInventoryPagesSelftest_NoticesAPageClaimThatIsNotTrue(t *testing.T) {
	tests := []struct {
		name  string
		rows  string
		pages map[string]string
		want  string
	}{
		{
			name:  "a canonical page that does not resolve",
			rows:  inventoryPageRow("cli-x", "`ptah schema render`", "`start/retired`", "none"),
			pages: map[string]string{sitePath("start/install"): "# Install\n\n`ptah schema render`\n"},
			want:  "resolves to no tracked document",
		},
		{
			name:  "an example on a page that does not resolve",
			rows:  inventoryPageRow("cli-x", "`ptah schema render`", "`start/install`", "yes: `start/retired`"),
			pages: map[string]string{sitePath("start/install"): "# Install\n\n`ptah schema render`\n"},
			want:  "resolves to no tracked document",
		},
		{
			name:  "an example on a page with no fenced block at all",
			rows:  inventoryPageRow("cli-x", "`ptah schema render`", "`start/install`", "yes: `start/install`"),
			pages: map[string]string{sitePath("start/install"): "# Install\n\nProse naming `ptah schema render` and nothing runnable.\n"},
			want:  "has no fenced code block at all",
		},
		{
			name:  "a canonical page that never names the command",
			rows:  inventoryPageRow("cli-x", "`ptah schema render`", "`start/install`", "none"),
			pages: map[string]string{sitePath("start/install"): "# Install\n\nProse about something else entirely.\n"},
			want:  "does not contain any of them",
		},
		{
			name:  "a canonical page that never names the variable",
			rows:  inventoryPageRow("cfg-x", "`PTAH_OCI_CONFIG`", "`start/install`", "none"),
			pages: map[string]string{sitePath("start/install"): "# Install\n\nProse about something else entirely.\n"},
			want:  "does not contain any of them",
		},
		{
			name: "an example whose fenced blocks never run the command",
			rows: inventoryPageRow("cli-x", "`ptah schema render`", "`start/install`", "yes: `start/install`"),
			pages: map[string]string{
				sitePath("start/install"): "# Install\n\n`ptah schema render` in prose.\n\n```bash\ngo install go.5x5.cz/ptah/cmd/ptah@latest\n```\n",
			},
			want: "no fenced block there contains any of them",
		},
		{
			name:  "a canonical cell that names neither a page nor none",
			rows:  inventoryPageRow("cli-x", "`ptah schema render`", "the site, somewhere", "none"),
			pages: map[string]string{sitePath("start/install"): "# Install\n"},
			want:  "names neither a page nor",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			docs, inventory := writePagesFixture(c, test.rows, test.pages)
			findings := featureinventory.CheckInventoryPages(docs, inventory)
			c.Assert(messages(findings), qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Contains, test.want)
		})
	}
}

// TestInventoryPagesSelftest_LeavesTheClaimsItMustNotRead is the other half.
//
// Every row here was a shape the register actually uses, and reporting any of
// them would make the gate wrong rather than strict. The last two are the third
// rule's stated limit: a media type and a workflow file are things a page owns
// without quoting, so a row whose surface carries no command path and no
// `PTAH_*` name is held to the first two rules only.
func TestInventoryPagesSelftest_LeavesTheClaimsItMustNotRead(t *testing.T) {
	tests := []struct {
		name  string
		rows  string
		pages map[string]string
	}{
		{
			name:  "a row that says no page owns it",
			rows:  inventoryPageRow("cli-x", "`ptah schema render`", "**none — no tracked page names it**", "none"),
			pages: map[string]string{sitePath("start/install"): "# Install\n"},
		},
		{
			name: "a canonical page carrying the command in prose",
			rows: inventoryPageRow("cli-x", "`ptah schema render`", "`start/install`", "none"),
			pages: map[string]string{
				sitePath("start/install"): "# Install\n\nRun `ptah schema render` to see the desired schema.\n",
			},
		},
		{
			name: "a page named by its directory rather than its index file",
			rows: inventoryPageRow("cli-x", "`ptah schema render`", "`concepts`", "none"),
			pages: map[string]string{
				sitePath("concepts/index"): "# Concepts\n\n`ptah schema render` is the entry point.\n",
			},
		},
		{
			name: "an example on a repository document rather than a site page",
			rows: inventoryPageRow("bin-x", "`./cmd/integration-test`", "**none**", "yes: `integration/README.md`"),
			pages: map[string]string{
				"integration/README.md": "# Suite\n\n```bash\ngo run ./cmd/integration-test list\n```\n",
			},
		},
		{
			name: "a canonical page for a media type it explains without quoting",
			rows: inventoryPageRow("oci-x", "`application/vnd.stokaro.ptah.schema.v1`", "`operate/oci-registry`", "none"),
			pages: map[string]string{
				sitePath("operate/oci-registry"): "# Registry\n\nPtah publishes one artifact per schema.\n",
			},
		},
		{
			name: "a canonical page for a workflow file it explains without quoting",
			rows: inventoryPageRow("ci-x", "`.github/workflows/install-smoke.yml`", "`start/install`", "none"),
			pages: map[string]string{
				sitePath("start/install"): "# Install\n\nEvery release is installed and run before it is announced.\n",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			docs, inventory := writePagesFixture(c, test.rows, test.pages)
			findings := featureinventory.CheckInventoryPages(docs, inventory)
			c.Assert(messages(findings), qt.HasLen, 0)
		})
	}
}

// TestInventoryPagesSelftest_RefusesASiteItCannotRead is the control on the
// document reader itself.
//
// A reader that resolved no page would report every claim missing, which is
// loud. A reader pointed at a tree with no site at all would report nothing to
// resolve against, which is not, so it refuses instead.
func TestInventoryPagesSelftest_RefusesASiteItCannotRead(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeTrackedFile(c, dir, "README.md", "# Nothing but this\n")
	initFixtureRepository(c, dir)

	docs, err := featureinventory.NewDocuments(dir)
	c.Assert(err, qt.ErrorMatches, ".*no page resolved under docs/site/src/content/docs.*")
	c.Assert(docs, qt.IsNil)
}
