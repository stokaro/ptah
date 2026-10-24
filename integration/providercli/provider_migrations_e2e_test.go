//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrator"
)

func migrationProviderSpec(c *qt.C) providerhost.Spec {
	c.Helper()
	return migrationProviderCommandSpec(c, "metadata")
}

func migrationProviderCommandSpec(c *qt.C, name string) providerhost.Spec {
	c.Helper()
	return fixtureReleaseSpec(c, name, testutils.FixtureRelease{})
}

// fixtureReleaseSpec builds one fixture command as release selects and trusts
// the digest of the executable it produced.
func fixtureReleaseSpec(c *qt.C, name string, release testutils.FixtureRelease) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProviderRelease(c, name, release)
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
}

func migrationDirectory(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_records.up.sql"), []byte(`CREATE TABLE "records" ("id" INTEGER);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_records.down.sql"), []byte(`DROP TABLE "records";`), 0o600), qt.IsNil)
	return dir
}

func migrationArguments(f providerCLI, verb string, args ...string) []string {
	return append([]string{"migrations", verb, "--provider-config", f.manifestPath, "--db-url", f.address}, args...)
}
func runProviderMigration(c *qt.C, f providerCLI, verb string, args ...string) string {
	c.Helper()
	out, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f, verb, args...))
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s: %s\n%s", verb, diagnostics, out))
	f.assertStopped(c)
	return out
}

func TestProviderMigrationsNativeLifecycle(t *testing.T) {
	c := qt.New(t)
	providerMigrationsLifecycle(t, migrationProviderSpec(c))
}

func TestProviderMigrationsWithoutCommonTables(t *testing.T) {
	c := qt.New(t)
	providerMigrationsLifecycle(t, migrationProviderCommandSpec(c, "native-migrations"))
}

func providerMigrationsLifecycle(t *testing.T, spec providerhost.Spec) {
	t.Helper()
	for _, mode := range []string{"file", "none", "all"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, spec)
			dir := migrationDirectory(c)
			out := runProviderMigration(c, f, "up", "--migrations-dir", dir, "--tx-mode", mode, "--migration-lock-timeout", "1s", "--json")
			var result migrator.RunResult
			c.Assert(json.Unmarshal([]byte(out), &result), qt.IsNil)
			c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
			c.Assert(result.Applied, qt.DeepEquals, []int64{1})
			c.Assert(result.Status.CurrentVersion, qt.Equals, int64(1))
			status := runProviderMigration(c, f, "status", "--migrations-dir", dir, "--json")
			c.Assert(json.Valid([]byte(status)), qt.IsTrue)
			log := runProviderMigration(c, f, "log", "--json")
			c.Assert(json.Valid([]byte(log)), qt.IsTrue)
			c.Assert(log, qt.Contains, `"outcome": "applied"`)
			runProviderMigration(c, f, "tag", "empty", "--version", "0")
			runProviderMigration(c, f, "tag", "release")
			c.Assert(runProviderMigration(c, f, "tag"), qt.Contains, "release")
			runProviderMigration(c, f, "down", "--migrations-dir", dir, "--to-tag", "empty", "--migration-lock-timeout", "1s", "--confirm")
			c.Assert(runProviderMigration(c, f, "log", "--json"), qt.Contains, `"outcome": "rolled_back"`)
			runProviderMigration(c, f, "tag", "release", "--delete")
			c.Assert(runProviderMigration(c, f, "tag"), qt.Not(qt.Contains), "release")
			sql, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(f.databasePath))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(sql.Close(), qt.IsNil) })
			c.Assert(providerTablePresent(c, sql, "records"), qt.IsFalse)
		})
	}
}

func TestProviderMigrationsMetadataCommands(t *testing.T) {
	c := qt.New(t)
	spec := migrationProviderSpec(c)
	f := newProviderCLI(c, spec)
	dir := migrationDirectory(c)
	runProviderMigration(c, f, "baseline", "--migrations-dir", dir, "--force")
	runProviderMigration(c, f, "set", "--migrations-dir", dir, "--version", "0")
	runProviderMigration(c, f, "repair", "--migrations-dir", dir, "--version", "1", "--force")
	out := runProviderMigration(c, f, "up", "--migrations-dir", dir, "--json")
	var result migrator.RunResult
	c.Assert(json.Unmarshal([]byte(out), &result), qt.IsNil)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeUpToDate)
	c.Assert(result.Status.CurrentVersion, qt.Equals, int64(1))
	sql, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(f.databasePath))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sql.Close(), qt.IsNil) })
	c.Assert(providerTablePresent(c, sql, "records"), qt.IsFalse)
}

func TestProviderMigrationsRefuseUntrustedExecutableBeforeConnection(t *testing.T) {
	c := qt.New(t)
	spec := migrationProviderSpec(c)
	spec.SHA256 = strings.Repeat("0", 64)
	for _, verb := range []string{"up", "down", "status", "log", "tag", "baseline", "repair", "set"} {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, spec)
			_, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f, verb))
			c.Assert(code, qt.Equals, 2)
			c.Assert(diagnostics, qt.Contains, "checksum")
			_, err := os.Stat(f.pidPath)
			c.Assert(os.IsNotExist(err), qt.IsTrue)
			_, err = os.Stat(f.databasePath)
			c.Assert(os.IsNotExist(err), qt.IsTrue)
		})
	}
}

func TestProviderMigrationsKeepDataSafetyGate(t *testing.T) {
	c := qt.New(t)
	spec := migrationProviderSpec(c)
	f := newProviderCLI(c, spec)
	dir := migrationDirectory(c)
	sql, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(f.databasePath))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sql.Close(), qt.IsNil) })
	c.Assert(sql.SchemaWriter().ExecuteSQL(c.Context(), `CREATE TABLE "records" ("id" INTEGER);`), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_records.up.sql"), []byte(`DROP TABLE "records";`), 0o600), qt.IsNil)
	out, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f, "up", "--migrations-dir", dir, "--json"))
	c.Assert(code, qt.Equals, 2)
	f.assertStopped(c)
	c.Assert(diagnostics, qt.Contains, "DS101")
	var result migrator.RunResult
	c.Assert(json.Unmarshal([]byte(out), &result), qt.IsNil)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(result.Status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(providerTablePresent(c, sql, "records"), qt.IsTrue)
	runProviderMigration(c, f, "up", "--migrations-dir", dir, "--allow-destructive")
	c.Assert(providerTablePresent(c, sql, "records"), qt.IsFalse)
}

func TestProviderMigrationsReadWithoutCreatingMetadata(t *testing.T) {
	c := qt.New(t)
	spec := migrationProviderSpec(c)
	f := newProviderCLI(c, spec)
	dir := migrationDirectory(c)
	var status migrator.MigrationStatus
	c.Assert(json.Unmarshal([]byte(runProviderMigration(c, f, "status", "--migrations-dir", dir, "--json")), &status), qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{1})
	c.Assert(strings.TrimSpace(runProviderMigration(c, f, "log", "--json")), qt.Equals, "[]")
	runProviderMigration(c, f, "tag")
	sql, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(f.databasePath))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sql.Close(), qt.IsNil) })
	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), sql, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 0)
}
