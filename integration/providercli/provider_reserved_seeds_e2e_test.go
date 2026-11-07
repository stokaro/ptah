//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// The fixture's data program keeps its seed ledger in the SQLite table
// fixture_seed_ledger. With FIXTURE_RESERVED_SEEDS=true it reports the ledger
// as reserved storage of purpose SEEDS; otherwise it leaves the ledger out of
// its reads unsaid.

// reservedSeedsCLI is the CLI over the data program, reserving its ledger
// where reserved says so, after a seed run created the ledger.
func reservedSeedsCLI(c *qt.C, reserved string) dataCLI {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "data")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	spec := providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture",
		Environment: []string{"FIXTURE_RESERVED_SEEDS=" + reserved}}
	f := newDataCLI(c, spec)
	f.createRegions(c)
	f.seed(c, "001_regions.dev.sql", "INSERT INTO \"regions\" (\"code\", \"name\") VALUES ('NO', 'Norway');\n")
	diagnostics, code := f.runSeeds(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	return f
}

// ledgerDeclaration declares regions as the program holds it, and a table of
// the ledger's name.
const ledgerDeclaration = "tables:\n  regions:\n    columns:\n      code: {type: TEXT, primary: true, not_null: true}\n" +
	"      name: {type: TEXT, not_null: true}\n      rank: {type: INTEGER}\n" +
	"  fixture_seed_ledger:\n    columns:\n      path: {type: TEXT, primary: true, not_null: true}\n"

// A reserved ledger is the provider's storage: a declaration that takes its
// name is refused before anything runs, and the ledger keeps its entry.
func TestProviderReservedSeedLedger_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := reservedSeedsCLI(c, "true")
	c.Assert(os.WriteFile(f.schemaPath, []byte(ledgerDeclaration), 0o600), qt.IsNil)

	_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "is reserved for seeds storage by provider \"example.org/fixture\"")
	diagnostics, code = f.runSeeds(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(diagnostics+f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "fixture_seed_ledger")
}

// Without the reservation the host cannot tell the ledger from a table the
// read left out, and plans the declared table as a creation.
func TestProviderUnreservedSeedLedger(t *testing.T) {
	c := qt.New(t)
	f := reservedSeedsCLI(c, "false")
	c.Assert(os.WriteFile(f.schemaPath, []byte(ledgerDeclaration), 0o600), qt.IsNil)

	planned := f.plan(c)

	c.Assert(planned, qt.Contains, `"outcome": "changes"`)
	c.Assert(planned, qt.Contains, `CREATE TABLE \"fixture_seed_ledger\"`)
	c.Assert(planned, qt.Not(qt.Contains), `DROP TABLE`)
}
