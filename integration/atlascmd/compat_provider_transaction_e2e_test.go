//go:build integration

package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// compatTransactionFixture serves a provider that offers no sessions.transactions
// on a database whose items table lacks the column the desired schema adds, and
// returns the addresses of the desired schema and of a plan saved from it.
func compatTransactionFixture(c *qt.C, binaries compatProviderBinaries) (f compatProviderFixture, desired, plan string) {
	c.Helper()
	f = newCompatProviderFixture(c, binaries)
	createFixtureTable(c, filepath.Join(f.dir, "database.db"), `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY)`)
	source := filepath.Join(f.dir, "desired.yaml")
	c.Assert(os.WriteFile(source, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true, primary: true}\n      added: {type: REAL}\n"), 0o600), qt.IsNil)
	saved := filepath.Join(f.dir, "change.plan.json")
	desired = "file://" + filepath.ToSlash(source)
	_, diagnostics, code := f.run(f.withManifest(), "schema", "plan", "--from", f.address, "--to", desired, "--output", saved)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	return f, desired, "file://" + filepath.ToSlash(saved)
}

// ptah-compat reaches the same refusal as the native command through the same
// preflight, and it comes before a dry run's exit: a dry run that exits 0 on a
// plan the real run refuses is a false green. `--tx-mode none`, which the refusal
// names, applies the plan.
func TestCompatProviderRefusesATransactionItsProviderCannotOpen(t *testing.T) {
	parent := qt.New(t)
	binaries := compatProviderBinaries{
		compat:   buildSchemaInspectBinary(parent, "ptah-compat", "ptah.run/cmd/ptah-compat"),
		provider: testutils.BuildFixtureProvider(parent, "provider"),
	}
	tests := []struct {
		name string
		args func(desired, plan string) []string
	}{
		{name: "a computed plan", args: func(desired, _ string) []string { return []string{"--to", desired, "--auto-approve"} }},
		{name: "a computed plan, dry run", args: func(desired, _ string) []string { return []string{"--to", desired, "--dry-run"} }},
		{name: "a saved plan", args: func(_, plan string) []string { return []string{"--plan", plan, "--auto-approve"} }},
		{name: "a saved plan, dry run", args: func(_, plan string) []string { return []string{"--plan", plan, "--dry-run"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f, desired, plan := compatTransactionFixture(c, binaries)
			args := append([]string{"schema", "apply", "--url", f.address}, test.args(desired, plan)...)

			_, diagnostics, code := f.run(f.withManifest(), args...)

			c.Assert(code, qt.Equals, 1, qt.Commentf("%s", diagnostics))
			c.Assert(diagnostics, qt.Contains, `transaction mode "file" needs a transaction this target cannot open`)
			c.Assert(diagnostics, qt.Contains, "rerun with --tx-mode none")
			assertCompatNativeColumn(c, f, 0)
			f.assertStopped(c)
		})
	}
	t.Run("the mode the refusal names", func(t *testing.T) {
		c := qt.New(t)
		f, desired, _ := compatTransactionFixture(c, binaries)

		out, diagnostics, code := f.run(f.withManifest(), "schema", "apply", "--url", f.address, "--to", desired, "--tx-mode", "none", "--auto-approve")

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
		c.Assert(out, qt.Contains, "Schema apply completed successfully.")
		assertCompatNativeColumn(c, f, 1)
		f.assertStopped(c)
	})
}
