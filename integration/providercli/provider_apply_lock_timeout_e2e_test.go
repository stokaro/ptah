//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/testutils"
)

// lockTimeoutFixture is the CLI over an empty database of the fixture provider
// that serves sessions.locks: its lock is real across processes, held by a
// write transaction on a sidecar SQLite file of its own.
func lockTimeoutFixture(c *qt.C) providerCLI {
	c.Helper()
	f := newProviderCLI(c, fixtureReleaseSpec(c, "locks", testutils.FixtureRelease{}))
	// The fixture resolves the database path before it names the lock's
	// sidecar file, so the file has to exist; an empty file is an empty
	// SQLite database.
	c.Assert(os.WriteFile(f.databasePath, nil, 0o600), qt.IsNil)
	return f
}

// holdApplyLock takes the fixture's schema apply lock from outside the
// command, as another apply holding it would, and keeps it until the test
// ends. The fixture's lock for name is an uncommitted write on the sidecar
// file `<database>.lock-<hex sha256(name)>`, which a second writer cannot
// begin.
func holdApplyLock(c *qt.C, databasePath string) {
	c.Helper()
	dir, err := filepath.EvalSymlinks(filepath.Dir(databasePath))
	c.Assert(err, qt.IsNil)
	path := filepath.Join(dir, filepath.Base(databasePath))
	sidecar := &url.URL{Scheme: "file", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/") +
		fmt.Sprintf(".lock-%x", sha256.Sum256([]byte(atlasschema.ApplyLockName))),
		RawQuery: url.Values{"_txlock": {"immediate"}, "_pragma": {"busy_timeout(0)"}}.Encode()}
	db, err := sql.Open("sqlite", sidecar.String())
	c.Assert(err, qt.IsNil)
	db.SetMaxOpenConns(1)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	tx, err := db.BeginTx(c.Context(), nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(tx.Rollback(), qt.IsNil) })
	_, err = tx.ExecContext(c.Context(), "CREATE TABLE ownership (held INTEGER NOT NULL)")
	c.Assert(err, qt.IsNil)
	_, err = tx.ExecContext(c.Context(), "INSERT INTO ownership VALUES (1)")
	c.Assert(err, qt.IsNil)
}

// A provider that takes the schema apply lock bounds the wait for it with
// --lock-timeout, typed or set through PTAH_LOCK_TIMEOUT, and with no other
// holder the apply takes the lock, runs and gives it back. The host refused a
// typed timeout here as aimed at a dialect without a lock, and noted the
// variable as ignored while it bounded the wait.
func TestProviderSchemaApplyLockTimeout_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		args func(providerCLI, ...string) []string
	}{
		{name: "typed, a computed plan", args: computedArgs},
		{name: "typed, a saved plan", args: savedArgs},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := lockTimeoutFixture(c)
			f.plan(c)

			out, diagnostics, code := f.run(c, c.Context(), "apply", test.args(f, "--lock-timeout", "5s")...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(applyReport(c, out).Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
			c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.DeepEquals, []string{"items", "second"})
		})
	}
	t.Run("from PTAH_LOCK_TIMEOUT", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv("PTAH_LOCK_TIMEOUT", "5s")
		f := lockTimeoutFixture(c)

		out, diagnostics, code := f.run(c, c.Context(), "apply", computedArgs(f)...)

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
		c.Assert(diagnostics, qt.Not(qt.Contains), "PTAH_LOCK_TIMEOUT is ignored")
		c.Assert(applyReport(c, out).Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
		c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.DeepEquals, []string{"items", "second"})
	})
	// A dry run takes no provider lock, so it previews while another session
	// holds it; the timeout bounds the real run the preview stands for.
	t.Run("a dry run while another session holds the lock", func(t *testing.T) {
		c := qt.New(t)
		f := lockTimeoutFixture(c)
		holdApplyLock(c, f.databasePath)

		out, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--dry-run", "--lock-timeout", "300ms")

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
		c.Assert(out, qt.Contains, "Planned schema changes:")
		c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.HasLen, 0)
	})
}

// While another session holds the provider's lock, the apply waits out
// --lock-timeout and stops with the lock-timeout refusal, before it plans and
// with nothing created, whichever spelling set the timeout.
func TestProviderSchemaApplyLockTimeout_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		args func(providerCLI, ...string) []string
	}{
		{name: "typed, a computed plan", args: computedArgs},
		{name: "typed, a saved plan", args: savedArgs},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := lockTimeoutFixture(c)
			f.plan(c)
			holdApplyLock(c, f.databasePath)

			out, diagnostics, code := f.run(c, c.Context(), "apply", test.args(f, "--lock-timeout", "300ms")...)

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
			report := applyReport(c, out)
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
			c.Assert(refusalCode(report), qt.Equals, atlasschema.RefusalLockTimeout)
			c.Assert(report.Error.Message, qt.Equals,
				`acquire schema apply lock: timed out acquiring advisory lock "ptah_schema_apply" on fixture after 300ms`)
			c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.HasLen, 0)
		})
	}
	t.Run("from PTAH_LOCK_TIMEOUT", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv("PTAH_LOCK_TIMEOUT", "300ms")
		f := lockTimeoutFixture(c)
		holdApplyLock(c, f.databasePath)

		out, diagnostics, code := f.run(c, c.Context(), "apply", computedArgs(f)...)

		c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
		c.Assert(diagnostics, qt.Not(qt.Contains), "PTAH_LOCK_TIMEOUT is ignored")
		c.Assert(refusalCode(applyReport(c, out)), qt.Equals, atlasschema.RefusalLockTimeout)
		c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.HasLen, 0)
	})
}
