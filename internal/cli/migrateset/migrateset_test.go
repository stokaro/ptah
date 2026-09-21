package migrateset_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/migrateset"
	"ptah.run/internal/cli/migrateup"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// writePtahMigrations writes a two-migration ptah-format directory.
func writePtahMigrations(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":    "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql":  "DROP TABLE users;\n",
		"0000000002_orders.up.sql":   "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000002_orders.down.sql": "DROP TABLE orders;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// writeAtlasMigrations writes a two-migration atlas-format directory, hashed.
//
// The atlas.sum is not decoration: `ptah-compat migrate set` verifies it before
// writing revision rows (#974), so an unhashed fixture would be refused the way
// the community binary refuses one. Hashing the fixture is the fix; weakening
// the gate to accept it would re-open the bug.
func writeAtlasMigrations(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"2_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	sum, err := atlascompat.ComputeSum(os.DirFS(dir), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
	return dir
}

func runSet(args ...string) (string, error) {
	cmd := migrateset.NewMigrateSetCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// queryVersions returns the version column values of the revision table.
func queryVersions(c *qt.C, dbPath, table string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), "SELECT version FROM "+table+" ORDER BY CAST(version AS INTEGER)")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var versions []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func TestMigrationsSetMovesBoundaryBothDirections(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	// Upward: mark both migrations applied without executing SQL.
	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "2")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Current version is 2 (2 set):")
	c.Assert(out, qt.Contains, "+ 1 (Users)")
	c.Assert(out, qt.Contains, "+ 2 (Orders)")
	c.Assert(queryVersions(c, dbPath, "schema_migrations"), qt.DeepEquals, []string{"1", "2"})

	// Downward: remove the revision row above the target version, still
	// without executing SQL.
	out, err = runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Current version is 1 (1 removed):")
	c.Assert(out, qt.Contains, "- 2 (Orders)")
	c.Assert(queryVersions(c, dbPath, "schema_migrations"), qt.DeepEquals, []string{"1"})
}

func TestMigrationsSetIsIdempotent(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Revision state already at version 1; no changes to be made.")
}

func TestMigrationsSetRejectsUnknownVersion(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "9")
	c.Assert(err, qt.ErrorMatches, `migration with version "9" not found`, qt.Commentf("%s", out))
}

func TestMigrationsSetRequiresVersion(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)
	c.Assert(err, qt.ErrorMatches, "--version is required", qt.Commentf("%s", out))
}

// assertNoRevisionTable asserts the revision table was never created.
func assertNoRevisionTable(c *qt.C, dbPath string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestMigrationsSetDryRunChangesNothing(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "2", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Dry run: would set the revision boundary to version 2.")
	assertNoRevisionTable(c, dbPath)
}

// TestMigrationsSetAtlasFormatMatchesAtlasMigrateSet proves the native verb
// with --revision-format atlas and its Atlas twin leave identical revision
// state behind on identical databases: both wrap atlasmigrate.Set.
func TestMigrationsSetAtlasFormatMatchesAtlasMigrateSet(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrations(t)
	nativeDB := filepath.Join(t.TempDir(), "native.db")
	atlasDB := filepath.Join(t.TempDir(), "atlas.db")

	nativeOut, err := runSet(
		"--db-url", "sqlite://"+nativeDB,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--version", "2",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", nativeOut))

	atlasCmd := atlas.NewCompatCommand("atlas")
	var atlasOut bytes.Buffer
	atlasCmd.SetOut(&atlasOut)
	atlasCmd.SetErr(&atlasOut)
	atlasCmd.SetArgs([]string{"migrate", "set", "2",
		"--url", "sqlite://" + atlasDB,
		"--dir", "file://" + migrationsDir,
	})
	c.Assert(atlasCmd.Execute(), qt.IsNil, qt.Commentf("%s", atlasOut.String()))

	// Same summary output and identical revision rows in both databases.
	c.Assert(nativeOut, qt.Equals, atlasOut.String())
	c.Assert(
		queryVersions(c, nativeDB, "atlas_schema_revisions"),
		qt.DeepEquals,
		queryVersions(c, atlasDB, "atlas_schema_revisions"),
	)
}

// Version 0 names the state where no migration is applied. It is the only
// value that ends a rollback finished by hand when the migration it reverted
// was the oldest one in the directory: --force would record that migration
// applied, which is the opposite outcome (stokaro/ptah#3455).
func TestMigrationsSetToZeroClearsEveryRevision(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "2")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "0")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "No migration is recorded as applied (2 removed):")
	c.Assert(out, qt.Contains, "- 1 (Users)")
	c.Assert(out, qt.Contains, "- 2 (Orders)")
	// The summary never names a version, because the directory has no file for
	// version 0 and a reader would go looking for one.
	c.Assert(out, qt.Not(qt.Contains), "Current version is 0")
	c.Assert(queryVersions(c, dbPath, "schema_migrations"), qt.HasLen, 0)
}

// The Atlas revision table is the format where the delete keeps the new head
// row by name, and version 0 leaves no head to keep.
func TestMigrationsSetToZeroClearsEveryAtlasRevision(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	args := []string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
	}

	out, err := runSet(append(args, "--version", "2")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"1", "2"})

	out, err = runSet(append(args, "--version", "0")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "No migration is recorded as applied (2 removed):")
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.HasLen, 0)
}

func TestMigrationsSetToZeroIsIdempotent(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "0")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "No migration is recorded as applied; no changes to be made.")

	out, err = runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "0")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "No migration is recorded as applied; no changes to be made.")
	c.Assert(queryVersions(c, dbPath, "schema_migrations"), qt.HasLen, 0)
}

func TestMigrationsSetToZeroDryRunChangesNothing(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--version", "0",
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains,
		"Dry run: would remove every revision, leaving no migration recorded as applied.")
	assertNoRevisionTable(c, dbPath)
}

// Zero is the floor, not the removal of the floor: a negative version names no
// state at all and still stops before the database is touched.
func TestMigrationsSetRejectsNegativeVersion(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version=-1")

	c.Assert(err, qt.ErrorMatches, "--version must not be negative", qt.Commentf("%s", out))
	assertNoRevisionTable(c, dbPath)
}

// writeAtlasSum rewrites atlas.sum for whatever the directory now holds. A
// `migrate set` verifies it before writing revision rows, so a fixture that
// changes has to be rehashed rather than have the gate relaxed.
func writeAtlasSum(c *qt.C, dir string) {
	c.Helper()
	sum, err := atlascompat.ComputeSum(os.DirFS(dir), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
}

// writeAtlasRepeatableMigrations writes a directory whose last entry is a
// numbered repeatable, whose revision identity is the token `3R`.
func writeAtlasRepeatableMigrations(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"2_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"3R_view.sql":  "CREATE VIEW v AS SELECT 1;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	writeAtlasSum(c, dir)
	return dir
}

// A repeatable identity has no number, so the delete's numeric predicate
// cannot reach it, while the summary resolves `3R` to version 3 and reports it
// removed. Where the directory still holds the file the version map carries
// the number and both agree; where the file is gone there is no map entry, and
// the row survived a removal the command said it had made.
func TestMigrationsSetRemovesAnUnownedRepeatableRevision(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasRepeatableMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	args := []string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
	}

	out, err := runSet(append(args, "--version", "3")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"1", "2", "3R"})

	// The repeatable leaves the directory; its revision row stays behind.
	c.Assert(os.Remove(filepath.Join(migrationsDir, "3R_view.sql")), qt.IsNil)
	writeAtlasSum(c, migrationsDir)

	out, err = runSet(append(args, "--version", "0")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// Named by the identity the revision table holds, which is what the
	// operator would look the row up by.
	c.Assert(out, qt.Contains, "- 3R (view)")
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.HasLen, 0)
}

// The same disagreement above zero: a set to 1 reports the repeatable removed
// and has to remove it.
func TestMigrationsSetRemovesAnUnownedRepeatableRevisionAboveZero(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasRepeatableMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	args := []string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
	}

	out, err := runSet(append(args, "--version", "3")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(os.Remove(filepath.Join(migrationsDir, "3R_view.sql")), qt.IsNil)
	writeAtlasSum(c, migrationsDir)

	out, err = runSet(append(args, "--version", "1")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "- 3R (view)")
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"1"})
}

// The control the fix must not break: a repeatable the directory still owns is
// removed by the numeric predicate through its version map, and the Atlas
// metadata row is not reported removed and is not touched.
func TestMigrationsSetRemovesAnOwnedRepeatableRevision(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasRepeatableMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	args := []string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
	}

	out, err := runSet(append(args, "--version", "3")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runSet(append(args, "--version", "1")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"1"})
}

// An always-repeatable `R__` file has no order: its identity resolves to
// version 0, so above no boundary and below none. Version 0 is not an ordering
// question -- it names the state where no migration is applied -- so the row
// goes with the rest, and the summary sentence is true of the table it leaves.
func TestMigrationsSetToZeroRemovesABareRepeatableRevision(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasBareRepeatableMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	applyAtlasMigrations(c, dbPath, migrationsDir)
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"R", "1"})

	// The repeatable leaves the directory, so nothing maps its identity to a
	// number and the numeric predicate cannot reach the row.
	c.Assert(os.Remove(filepath.Join(migrationsDir, "R__view.sql")), qt.IsNil)
	writeAtlasSum(c, migrationsDir)

	out, err := runSet(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--version", "0",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "No migration is recorded as applied")
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.HasLen, 0)
}

// The control for the rule above: version 1 is an ordering question, and a
// bare repeatable orders to zero, so it stays. What a set should do with it
// there is stokaro/ptah#3464.
func TestMigrationsSetAboveZeroKeepsABareRepeatableRevision(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasBareRepeatableMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	applyAtlasMigrations(c, dbPath, migrationsDir)
	c.Assert(os.Remove(filepath.Join(migrationsDir, "R__view.sql")), qt.IsNil)
	writeAtlasSum(c, migrationsDir)

	out, err := runSet(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--version", "1",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"R", "1"})
}

func writeAtlasBareRepeatableMigrations(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_users.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"R__view.sql": "CREATE VIEW v AS SELECT 1;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	writeAtlasSum(c, dir)
	return dir
}

// applyAtlasMigrations runs the directory for real, which is the only way an
// `R` row reaches the table: `set` writes none for a repeatable.
func applyAtlasMigrations(c *qt.C, dbPath, dir string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	m, err := migrator.NewFSMigrator(conn, os.DirFS(dir),
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	c.Assert(m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).MigrateUp(context.Background()), qt.IsNil)
}

// writeAtlasAlwaysRepeatableMigrations writes a directory whose repeatable
// carries no number at all, which is the identity `R` the revision table
// stores for `R__name.sql`.
func writeAtlasAlwaysRepeatableMigrations(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"2_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"R__view.sql":  "DROP VIEW IF EXISTS v;\nCREATE VIEW v AS SELECT 1;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	writeAtlasSum(c, dir)
	return dir
}

// An always-repeatable orders after every version, so every boundary is below
// it and a set removes it. The summary names it `R`, which is the identity the
// revision table holds and the only string that finds the row.
//
// Measured on the pinned community binary, which removes the row and reports
// `- R (_view)`: the rows and the summary are parity, not a Ptah choice
// (stokaro/ptah#3464).
func TestMigrationsSetRemovesAnAlwaysRepeatableRevision(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasAlwaysRepeatableMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	args := []string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
	}

	// The row comes from an apply, which is the only way it is written: a set
	// records no `R` row at any version, and neither does the community
	// binary, so seeding it with a set would measure nothing.
	out, err := runUp(args...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"R", "1", "2"})

	out, err = runSet(append(args, "--version", "1")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "- R (view)")
	c.Assert(out, qt.Not(qt.Contains), "- 0 (view)")
	c.Assert(queryVersions(c, dbPath, "atlas_schema_revisions"), qt.DeepEquals, []string{"1"})
}

// The removals are listed in ascending order, which is what tells a reader how
// far down the boundary moved.
//
// A row the directory no longer owns is collected on its own path and appended
// ahead of the rest, so without sorting a repeatable whose file has left the
// directory is listed before the versions below it.
func TestMigrationsSetListsRemovalsInAscendingOrder(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasRepeatableMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")
	args := []string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
	}
	out, err := runSet(append(args, "--version", "3")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(os.Remove(filepath.Join(migrationsDir, "3R_view.sql")), qt.IsNil)
	writeAtlasSum(c, migrationsDir)

	out, err = runSet(append(args, "--version", "0")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "  - 1 (users)\n  - 2 (orders)\n  - 3R (view)\n")
}

func runUp(args ...string) (string, error) {
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}
