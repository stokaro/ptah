package featureinventory_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/featureinventory"
)

// TestInventorySurfaces_EverySurfaceIsClaimedAndEveryClaimExists is check 4.
//
// The gate is scripts/check-inventory-surfaces.sh. It covers the three surfaces
// that are not commands: the public Go packages, the runnable programs, and
// every value of every enumerated format set.
func TestInventorySurfaces_EverySurfaceIsClaimedAndEveryClaimExists(t *testing.T) {
	c := qt.New(t)
	surfaces := shippedSurfaces(c)
	inventory := shippedInventory(c)

	findings := featureinventory.CheckInventorySurfaces(surfaces, inventory)
	c.Assert(messages(findings), qt.HasLen, 0)
}

// TestInventorySurfaces_ReadsEverySurfaceItClaimsTo is the control.
//
// Each of the three surfaces is asserted non-empty and one member of each is
// named, because a discovery that returned nothing would find every inventory
// row correct and every gap absent.
func TestInventorySurfaces_ReadsEverySurfaceItClaimsTo(t *testing.T) {
	c := qt.New(t)
	surfaces := shippedSurfaces(c)

	// Floors, not exact counts. This test is the anti-vacuity control: a
	// discovery that returned nothing would find every inventory row correct
	// and every gap absent. Completeness in the other direction -- a surface
	// nothing claims -- belongs to
	// TestInventorySurfaces_EverySurfaceIsClaimedAndEveryClaimExists, which
	// derives both sides. Pinning an exact number here reddens this control
	// every time the tree gains a package or a program, which is a claim that
	// was true when it was written rather than a property worth holding.
	c.Assert(len(surfaces.Packages) >= 35, qt.IsTrue)
	c.Assert(surfaces.Packages, qt.Contains, "go.5x5.cz/ptah/core/ast")

	c.Assert(len(surfaces.Programs) >= 24, qt.IsTrue)
	c.Assert(programDirs(surfaces), qt.Contains, "cmd/ptah")
	c.Assert(programDirs(surfaces), qt.Contains, "cmd/ptah-ls")
	// The finding this discovery exists for. `cmd/main.go` is three lines and a
	// call to root.Execute, so `./cmd` is a fourth complete copy of the native
	// CLI; it is in no release, no gate and no previous inventory, and
	// scripts/check-public-api.sh skips `cmd` and `cmd/*` outright.
	c.Assert(programDirs(surfaces), qt.Contains, "cmd")

	c.Assert(surfaces.Formats, qt.HasLen, 4)
	for _, list := range surfaces.Formats {
		t.Run(list.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(len(list.Values) > 1, qt.IsTrue)
			c.Assert(list.Source, qt.Not(qt.Equals), "")
		})
	}
}

// TestInventorySurfaces_TheFormatListsAreTheOnesTheCodeDecidesWith pins the
// values themselves.
//
// A list read from a flag's help string would agree with a help string that had
// drifted from the switch beside it, which is the defect stokaro/ptah#2065
// repaired: `ptah schema export --to` advertised four of its seven targets, so
// markdown, html and dbml were reachable and unadvertised.
func TestInventorySurfaces_TheFormatListsAreTheOnesTheCodeDecidesWith(t *testing.T) {
	tests := []struct {
		name string
		list string
		want []string
	}{
		{
			name: "schema file extensions",
			list: "schema-file-ext",
			want: []string{".dbml", ".hcl", ".sql", ".yaml", ".yml"},
		},
		{
			name: "schema export targets",
			list: "export-to",
			want: []string{"dbml", "graphql", "hcl", "html", "markdown", "openapi-v3", "protobuf"},
		},
		{
			name: "visualization formats",
			list: "viz-format",
			want: []string{"dot", "mermaid"},
		},
		{
			// Ten, not the nine AGENTS.md names: core/platform declares Oracle
			// too, and `ptah schema render --dialect oracle` exits 0.
			name: "canonical dialects",
			list: "dialect",
			want: []string{"clickhouse", "cockroachdb", "mariadb", "mysql", "oracle", "postgres", "spanner", "sqlite", "sqlserver", "yugabytedb"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			surfaces := shippedSurfaces(c)
			c.Assert(formatValues(surfaces, test.list), qt.DeepEquals, test.want)
		})
	}
}

// TestInventorySurfacesSelftest_NoticesASurfaceWithNoRow plants each half of the
// defect, one surface at a time.
func TestInventorySurfacesSelftest_NoticesASurfaceWithNoRow(t *testing.T) {
	tests := []struct {
		name string
		rows string
		want string
	}{
		{
			name: "a public package nobody claimed",
			rows: inventoryRow("program", "`./cmd/ptah`") + inventoryRow("format", "`format:demo/one` `format:demo/two`"),
			want: "docs/public_api.md lists `go.5x5.cz/ptah/core/ast` and no inventory row claims it",
		},
		{
			name: "a runnable program nobody claimed",
			rows: inventoryRow("pkg", "`go.5x5.cz/ptah/core/ast`") + inventoryRow("format", "`format:demo/one` `format:demo/two`"),
			want: "`cmd/ptah` is an installable `main` package and no inventory row claims it",
		},
		{
			name: "a format value nobody claimed",
			rows: inventoryRow("pkg", "`go.5x5.cz/ptah/core/ast`") + inventoryRow("program", "`./cmd/ptah`") + inventoryRow("format", "`format:demo/one`"),
			want: "`format:demo/two` is a value the code accepts",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			findings := featureinventory.CheckInventorySurfaces(fixtureSurfaces(), writeInventory(c, test.rows))
			c.Assert(messages(findings), qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Contains, test.want)
		})
	}
}

// TestInventorySurfacesSelftest_NoticesAClaimThatNamesNothing is the other
// direction, for each of the three kinds.
func TestInventorySurfacesSelftest_NoticesAClaimThatNamesNothing(t *testing.T) {
	base := inventoryRow("pkg", "`go.5x5.cz/ptah/core/ast`") +
		inventoryRow("program", "`./cmd/ptah`") +
		inventoryRow("format", "`format:demo/one` `format:demo/two`")

	tests := []struct {
		name string
		rows string
		want string
	}{
		{
			name: "a package the ledger does not list",
			rows: base + inventoryRow("gone", "`go.5x5.cz/ptah/core/retired`"),
			want: "which docs/public_api.md does not list",
		},
		{
			name: "a program that is not a main package",
			rows: base + inventoryRow("gone", "`./cmd/ptah-retired`"),
			want: "which is not an installable `main` package under cmd/",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			findings := featureinventory.CheckInventorySurfaces(fixtureSurfaces(), writeInventory(c, test.rows))
			c.Assert(messages(findings), qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Contains, test.want)
		})
	}
}

// TestInventorySurfacesSelftest_RefusesADiscoverySetItCouldNotRead asserts that
// each discovery fails loudly rather than returning an empty set.
//
// An empty ledger, an empty program list or an unresolvable format declaration
// all produce the same thing if they are tolerated: a check with nothing to
// compare, reporting success.
func TestInventorySurfacesSelftest_RefusesADiscoverySetItCouldNotRead(t *testing.T) {
	t.Run("a ledger with no list items", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		writeLedger(c, dir, "# Public API\n\nProse mentioning `go.5x5.cz/ptah/core/ast` and listing nothing.\n")
		packages, err := featureinventory.LedgerPackages(dir)
		c.Assert(err, qt.ErrorMatches, ".*listed no packages.*")
		c.Assert(packages, qt.IsNil)
	})

	t.Run("a tree with no main package", func(t *testing.T) {
		c := qt.New(t)
		programs, err := featureinventory.Programs(c.TempDir())
		c.Assert(err, qt.IsNotNil)
		c.Assert(programs, qt.IsNil)
	})
}
