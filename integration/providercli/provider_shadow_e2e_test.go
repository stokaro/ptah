//go:build integration

package providercli_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The shadow replays run on a database the provider provisions or names: the
// provider's inspection reports its revision storage as reserved and its
// cleanup removes it, so a replay reads back the schema the migrations built
// and nothing of the migrator's own. The expected state comes from the
// command's output and from reading the database files back.

// A checkpoint replays the directory on the provider's shadow database and
// declares the schema the migrations built, without the revision table the
// replay wrote. A realm the provider creates in the operator's database holds
// the replay, and the operator's database keeps what it held.
func TestProviderCheckpointsAReplayOnItsShadowDatabase(t *testing.T) {
	for _, row := range []struct {
		name  string
		realm bool
	}{
		{"a provisioned database", false},
		{"a realm of the operator's database", true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			f := newDevCLI(c)
			operator := filepath.Join(f.dir, "operator.db")
			createTable(c, operator, "kept")
			shadow := map[bool]string{false: "docker://fixture/1", true: fixtureAddress("localhost", operator)}[row.realm]

			out, diagnostics, code := executeProviderCLI(c.Context(), []string{
				"migrations", "checkpoint", "--provider-config", f.manifestPath, "--shadow-db", shadow,
				"--migrations-dir", hashedMigrations(c), "--dry-run",
			})

			f.assertStopped(c)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(out, qt.Contains, "\nCREATE TABLE \"records\" (\"id\" INTEGER);\n")
			c.Assert(out, qt.Contains, "\nDROP TABLE \"records\";\n")
			c.Assert(out, qt.Not(qt.Contains), "schema_migrations")
			c.Assert(tables(c, operator), qt.DeepEquals, []string{"kept"})
		})
	}
}

// A rollback is verified, and planned from the schema difference, on the
// provider's shadow database before the target is touched. The two databases
// are told apart by what their sessions select, since the host reads no
// provider address.
func TestProviderVerifiesARollbackOnItsShadowDatabase(t *testing.T) {
	for _, row := range []struct {
		name string
		args []string
		want string
	}{
		{"down migrations", nil, "Rollback plan verified on shadow database"},
		{"a planned rollback", []string{"--plan"}, `DROP TABLE "records"`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			f := newDevCLI(c)
			dir := hashedMigrations(c)
			runProviderMigration(c, f.providerCLI, "up", "--migrations-dir", dir)

			out, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f.providerCLI, "down",
				append([]string{"--migrations-dir", dir, "--shadow-db", "docker://fixture/1", "--dry-run"}, row.args...)...))

			f.assertStopped(c)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
			c.Assert(out, qt.Contains, row.want)
			c.Assert(tables(c, f.databasePath), qt.Contains, "records")
		})
	}
}

// A shadow database that is the target is refused before anything runs on
// it, whichever rollback the command verifies.
func TestProviderVerifiesARollbackOnItsShadowDatabase_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name string
		args []string
		want string
	}{
		{"down migrations", nil, "rollback verification failed: shadow database must be distinct from target database"},
		{"a planned rollback", []string{"--plan"}, "dynamic rollback planning failed: dev database must be distinct from target database"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			f := newDevCLI(c)
			dir := hashedMigrations(c)
			runProviderMigration(c, f.providerCLI, "up", "--migrations-dir", dir)

			out, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f.providerCLI, "down",
				append([]string{"--migrations-dir", dir, "--shadow-db", f.address, "--dry-run"}, row.args...)...))

			f.assertStopped(c)
			c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
			c.Assert(diagnostics, qt.Contains, row.want)
			c.Assert(tables(c, f.databasePath), qt.Contains, "records")
		})
	}
}
