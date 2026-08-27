package featureinventory_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/featureinventory"
)

// TestInventoryCommands_EveryPathIsClaimedAndEveryClaimExists is check 1.
//
// It runs in both directions from one comparison, and the gate is
// scripts/check-inventory-commands.sh.
func TestInventoryCommands_EveryPathIsClaimedAndEveryClaimExists(t *testing.T) {
	c := qt.New(t)
	census := shippedCensus(c)
	inventory := shippedInventory(c)

	// The control. A parse that stopped finding rows would compare the trees
	// against nothing, and "nothing is unclaimed" is what a clean run prints.
	c.Assert(len(inventory.Rows) > 150, qt.IsTrue,
		qt.Commentf("the inventory parsed to %d rows; a table this gate could not read would report every command claimed", len(inventory.Rows)))

	findings := featureinventory.CheckInventoryCommands(census, inventory)
	c.Assert(messages(findings), qt.HasLen, 0)
}

// TestInventoryCommandsSelftest_NoticesACommandWithNoRow plants the defect the
// gate exists to catch.
func TestInventoryCommandsSelftest_NoticesACommandWithNoRow(t *testing.T) {
	c := qt.New(t)
	census := fixtureCensus(c)

	// Every fixture path but `ptah schema render`.
	inventory := writeInventory(c,
		inventoryRow("root", "`ptah`")+
			inventoryRow("group", "`ptah schema`")+
			inventoryRow("compat-root", "`ptah-compat`")+
			inventoryRow("compat-group", "`ptah-compat migrate`")+
			inventoryRow("compat-verb", "`ptah-compat migrate apply`")+
			inventoryRow("compat-gated", "`ptah-compat migrate gated`")+
			inventoryRow("help", "`ptah help`")+
			inventoryRow("compat-help", "`ptah-compat help`")+
			inventoryRow("completion", "`ptah completion`")+
			inventoryRow("compat-completion", "`ptah-compat completion`")+
			completionShellRows())

	findings := featureinventory.CheckInventoryCommands(census, inventory)
	c.Assert(messages(findings), qt.HasLen, 1)
	c.Assert(findings[0].Message, qt.Contains, "`ptah schema render`")
	c.Assert(findings[0].Message, qt.Contains, "no inventory row claims it")
}

// TestInventoryCommandsSelftest_NoticesARowNamingNoCommand is the other
// direction.
//
// Both directions are asserted because a check written one way round reports
// success on half the ways the file can be wrong, and the two failures look
// nothing alike: one is a feature nobody documented, the other an entry that has
// outlived its subject.
func TestInventoryCommandsSelftest_NoticesARowNamingNoCommand(t *testing.T) {
	c := qt.New(t)
	census := fixtureCensus(c)

	inventory := writeInventory(c,
		inventoryRow("root", "`ptah`")+
			inventoryRow("group", "`ptah schema`")+
			inventoryRow("verb", "`ptah schema render`")+
			inventoryRow("retired", "`ptah schema generate`")+
			inventoryRow("compat-root", "`ptah-compat`")+
			inventoryRow("compat-group", "`ptah-compat migrate`")+
			inventoryRow("compat-verb", "`ptah-compat migrate apply`")+
			inventoryRow("compat-gated", "`ptah-compat migrate gated`")+
			inventoryRow("help", "`ptah help`")+
			inventoryRow("compat-help", "`ptah-compat help`")+
			inventoryRow("completion", "`ptah completion`")+
			inventoryRow("compat-completion", "`ptah-compat completion`")+
			completionShellRows())

	findings := featureinventory.CheckInventoryCommands(census, inventory)
	c.Assert(messages(findings), qt.HasLen, 1)
	c.Assert(findings[0].Message, qt.Contains, "`ptah schema generate`")
	c.Assert(findings[0].Message, qt.Contains, "does not register")
}

// TestInventoryCommandsSelftest_RefusesADocumentItCannotRead asserts the
// parser's own bounds.
//
// Each of these would otherwise produce an inventory of zero rows or of rows
// read out of the wrong table, and a comparison against zero rows reports the
// same success as a clean tree.
func TestInventoryCommandsSelftest_RefusesADocumentItCannotRead(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "no bounding heading",
			body:    "# Fixture\n\n### 1. Rows\n\n| " + joinColumns() + " |\n| " + delimiterRow() + "\n" + inventoryRow("x", "`ptah`"),
			wantErr: ".*yielded no rows under \"## The inventory\".*",
		},
		{
			name:    "the heading with no table under it",
			body:    "# Fixture\n\n## The inventory\n\nProse and nothing else.\n",
			wantErr: ".*yielded no rows.*",
		},
		{
			name:    "a table with a header and no rows",
			body:    "# Fixture\n\n## The inventory\n\n| " + joinColumns() + " |\n| " + delimiterRow() + "\n",
			wantErr: ".*yielded no rows.*",
		},
		{
			name:    "a required column dropped",
			body:    "# Fixture\n\n## The inventory\n\n| Feature ID | Public surface | User goal |\n| --- | --- | --- |\n| x | `ptah` | g |\n",
			wantErr: ".*missing the column\\(s\\) Evidence.*",
		},
		{
			name:    "one identifier naming two features",
			body:    fixtureHeader + inventoryRow("same", "`ptah`") + inventoryRow("same", "`ptah schema`"),
			wantErr: ".*is already used at line.*",
		},
		{
			name: "the region closed by the next heading of the same level",
			body: fixtureHeader + inventoryRow("kept", "`ptah`") +
				"\n## Appendix\n\n| " + joinColumns() + " |\n| " + delimiterRow() + "\n" + inventoryRow("kept", "`ptah schema`"),
			// The duplicate below the region is not read at all, so the
			// duplicate-ID rule cannot fire. A parser that ran past the region
			// would report it, which is how this fixture tells the two apart.
			wantErr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeDocument(c, dir, test.body)
			_, err := featureinventory.LoadInventory(dir)
			c.Assert(errorText(err), qt.Matches, orEmpty(test.wantErr))
		})
	}
}

// TestInventoryCommandsSelftest_ReadsNoTableAboveTheHeading is the other half of
// the bound.
//
// The real document opens with six tables before the region, and the first of
// them has a column literally named `Feature ID`. A scan that read them would
// find rows named "Column", "Token" and "Value" claiming nothing, and would then
// report every command in both trees as unclaimed -- a gate too noisy to keep,
// produced by reading four lines too far up.
func TestInventoryCommandsSelftest_ReadsNoTableAboveTheHeading(t *testing.T) {
	c := qt.New(t)
	inventory := writeInventory(c, inventoryRow("only-row", "`ptah`"))
	c.Assert(inventory.Rows, qt.HasLen, 1)
	c.Assert(inventory.Rows[0].ID, qt.Equals, "only-row")
}
