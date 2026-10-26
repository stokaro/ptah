//go:build integration

package atlas_test

import (
	"bytes"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func compatNativeFixture(c *qt.C) (compatProviderFixture, string) {
	c.Helper()
	return compatOperationsFixture(c, "native-cli", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "added" REAL)`)
}

func assertCompatNativeColumn(c *qt.C, f compatProviderFixture, want int) {
	c.Helper()
	db, err := sql.Open("sqlite", filepath.Join(f.dir, "database.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(db.Close(), qt.IsNil) })
	var count int
	c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM pragma_table_info('items') WHERE name='added'`).Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, want)
}

func TestCompatProviderNativeApply(t *testing.T) {
	c := qt.New(t)
	f, desired := compatNativeFixture(c)
	args := []string{"schema", "apply", "--url", f.address, "--to", desired, "--tx-mode", "none"}
	out, diagnostics, code := f.run(f.withManifest(), append(args, "--dry-run")...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "Native operation:")
	c.Assert(out, qt.Contains, "items")
	assertCompatNativeColumn(c, f, 0)
	out, diagnostics, code = f.run(f.withManifest(), append(args, "--auto-approve")...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	assertCompatNativeColumn(c, f, 1)
	f.assertStopped(c)
}

func TestCompatProviderNativeApplyCancellation(t *testing.T) {
	c := qt.New(t)
	f, desired := compatNativeFixture(c)
	command := exec.CommandContext(c.Context(), f.binaries.compat, "schema", "apply", "--url", f.address, "--to", desired, "--tx-mode", "none") // #nosec G204 -- the test runs its own compiled CLI
	command.Dir = f.dir
	command.Env = append(environmentWithoutPtahVariables(), f.withManifest()...)
	command.Stdin = strings.NewReader("n\n")
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	c.Assert(command.Run(), qt.IsNil, qt.Commentf("%s", output.String()))
	c.Assert(output.String(), qt.Contains, "Native operation:")
	assertCompatNativeColumn(c, f, 0)
	f.assertStopped(c)
}

func TestCompatProviderNativeApplyRefusesLossyOptions(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"edit", []string{"--edit"}, "a native operation cannot be edited as SQL"},
		{"format", []string{"--format", "{{json .}}"}, "cannot represent native operations"},
		{"transaction", []string{"--tx-mode", "file"}, "requires execution outside a transaction"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f, desired := compatNativeFixture(c)
			args := append([]string{"schema", "apply", "--url", f.address, "--to", desired, "--tx-mode", "none", "--dry-run"}, test.args...)
			_, diagnostics, code := f.run(f.withManifest(), args...)
			c.Assert(code, qt.Equals, 1)
			c.Assert(diagnostics, qt.Contains, test.want)
			assertCompatNativeColumn(c, f, 0)
			f.assertStopped(c)
		})
	}
}

func TestCompatProviderSavedNativeApply(t *testing.T) {
	c := qt.New(t)
	f, _ := compatNativeFixture(c)
	source := filepath.Join(f.dir, "desired.yaml")
	c.Assert(os.WriteFile(source, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true, primary: true}\n      added: {type: REAL}\n"), 0o600), qt.IsNil)
	plan := filepath.Join(f.dir, "native.plan.json")
	out, diagnostics, code := f.run(f.withManifest(), "schema", "plan", "--from", f.address, "--to", "file://"+filepath.ToSlash(source), "--output", plan)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "Native operation:")
	document, err := os.ReadFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(string(document), qt.Contains, `"native"`)
	args := []string{"schema", "apply", "--url", f.address, "--plan", "file://" + filepath.ToSlash(plan), "--tx-mode", "none"}
	_, diagnostics, code = f.run(f.withManifest(), append(args, "--tx-mode", "file", "--dry-run")...)
	c.Assert(code, qt.Equals, 1)
	c.Assert(diagnostics, qt.Contains, "requires execution outside a transaction")
	assertCompatNativeColumn(c, f, 0)
	out, diagnostics, code = f.run(f.withManifest(), append(args, "--dry-run")...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "Native operation:")
	assertCompatNativeColumn(c, f, 0)
	out, diagnostics, code = f.run(f.withManifest(), append(args, "--auto-approve")...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	assertCompatNativeColumn(c, f, 1)
	f.assertStopped(c)
}
