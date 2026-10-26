//go:build integration

package atlas_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

func TestCompatProviderExecutionInputs(t *testing.T) {
	for _, test := range []struct {
		name string
		seed func(*qt.C, compatProviderFixture)
	}{
		{"SQL", func(*qt.C, compatProviderFixture) {}},
		{"native", func(c *qt.C, f compatProviderFixture) {
			createFixtureTable(c, filepath.Join(f.dir, "database.db"), `CREATE TABLE "needs_input" ("id" INTEGER NOT NULL)`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newCompatProviderFixture(c, compatProviderBinaries{
				compat:      buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat"),
				provider:    testutils.BuildFixtureProvider(c, "operation-models"),
				environment: []string{"FIXTURE_RENDER_MODE=native-cli", "FIXTURE_INPUT_MODE=stable"},
			})
			test.seed(c, f)
			source := filepath.Join(f.dir, "desired.yaml")
			c.Assert(os.WriteFile(source, []byte("tables:\n  needs_input:\n    columns:\n      id: {type: INTEGER, not_null: true}\n      added: {type: REAL}\n"), 0o600), qt.IsNil)
			to := "file://" + filepath.ToSlash(source)
			const input = "PTAH_FIXTURE_EXECUTION_INPUT"
			const value = "fixture-private-value"
			computed := []string{"schema", "apply", "--url", f.address, "--to", to, "--tx-mode", "none"}
			out, diagnostics, code := f.run(f.withManifest(), append(computed, "--dry-run")...)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(out, qt.Contains, "Execution input: environment "+input)
			_, diagnostics, code = f.run(f.withManifest(), append(computed, "--edit", "--dry-run")...)
			c.Assert(code, qt.Equals, 1)
			c.Assert(diagnostics, qt.Contains, "approved execution inputs cannot be edited as SQL")
			plan := filepath.Join(f.dir, "inputs.plan.json")
			out, diagnostics, code = f.run(f.withManifest(), "schema", "plan", "--from", f.address, "--to", to, "--output", plan)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(out, qt.Contains, "Execution input: environment "+input)
			document, err := os.ReadFile(plan)
			c.Assert(err, qt.IsNil)
			c.Assert(string(document), qt.Contains, input)
			c.Assert(string(document), qt.Not(qt.Contains), value)
			saved := []string{"schema", "apply", "--url", f.address, "--plan", "file://" + filepath.ToSlash(plan), "--tx-mode", "none"}
			_, diagnostics, code = f.run(f.withManifest(), append(saved, "--auto-approve")...)
			c.Assert(code, qt.Equals, 1)
			c.Assert(diagnostics, qt.Contains, "environment variable")
			assertInputFixtureColumn(c, f, 0)
			out, diagnostics, code = f.run(append(f.withManifest(), input+"="+value), append(saved, "--auto-approve")...)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(out, qt.Contains, "Execution input: environment "+input)
			c.Assert(out+diagnostics, qt.Not(qt.Contains), value)
			assertInputFixtureColumn(c, f, 1)
			f.assertStopped(c)
		})
	}
}

func assertInputFixtureColumn(c *qt.C, f compatProviderFixture, want int) {
	c.Helper()
	db, err := sql.Open("sqlite", filepath.Join(f.dir, "database.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var count int
	c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM pragma_table_info('needs_input') WHERE name = 'added'`).Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, want)
}
