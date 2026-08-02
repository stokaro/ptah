package atlas_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"
)

// These tests cover the refusal that stops a database migrated by a pre-#982
// Ptah build from re-running migrations it has already applied.
//
// #982 converged the Flyway importer on Atlas CE's file selection, and doing so
// forced a different projection onto the int64 Atlas version — which is the key
// `atlas_schema_revisions` stores. To this build, a row written by Ptah v0.1.0
// through v0.1.2 matches no file, so every migration reads as pending.
//
// A legacy database is simulated by applying with THIS build and then rewriting
// the recorded versions back to the old encoding, which is exactly the inverse
// of the recovery the refusal prints. That keeps the fixture honest without
// needing an old binary on PATH: the versions written here are the ones the old
// base-100 encoding produced (V1 -> 1*100^2 = 10000, V2 -> 20000).
const (
	legacyFlywayV1 = "10000"
	legacyFlywayV2 = "20000"
)

// rewriteRevisionVersion moves one recorded revision to another version.
func rewriteRevisionVersion(c *qt.C, dbPath, from, to string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	result, err := db.Exec(`UPDATE atlas_schema_revisions SET version = ? WHERE version = ?`, to, from)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	// A fixture that rewrote nothing would leave the database on the new
	// encoding and make every assertion below vacuous.
	c.Assert(affected, qt.Equals, int64(1))
}

func revisionVersions(c *qt.C, dbPath string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	rows, err := db.Query(`SELECT version FROM atlas_schema_revisions ORDER BY version`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var out []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		out = append(out, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return out
}

func countRows(c *qt.C, dbPath, table string) int {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var n int
	c.Assert(db.QueryRow(`SELECT count(*) FROM `+table).Scan(&n), qt.IsNil)
	return n
}

// legacyFlywayFixture builds a hashed Flyway directory, applies it, and then
// rewrites both revisions back to the pre-#982 encoding.
func legacyFlywayFixture(c *qt.C) (dir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	dbPath = filepath.Join(root, "legacy.db")
	// An idempotent DDL and a seed: re-running this pair does NOT fail, it
	// silently inserts a second row. That is the shape the refusal exists for —
	// a CREATE TABLE would at least error.
	writeAtlasApplyProjectMigration(c, dir, "V1__init.sql", "CREATE TABLE IF NOT EXISTS seeded (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V2__seed.sql", "INSERT INTO seeded (id) VALUES (1);")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 1)

	rewriteRevisionVersion(c, dbPath, "4611686018427469511", legacyFlywayV1)
	rewriteRevisionVersion(c, dbPath, "4611686018427510315", legacyFlywayV2)
	return dir, dbPath
}

func TestCompatMigrateApply_LegacyFlywayRevisionsRefused(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := legacyFlywayFixture(c)

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "stokaro/ptah#982")
	// The mapping is printed per file, so the operator can see which migration
	// each row belongs to rather than only that something is wrong.
	c.Assert(message, qt.Contains, "10000")
	c.Assert(message, qt.Contains, "4611686018427469511")
	c.Assert(message, qt.Contains, "V1__init.sql")
	c.Assert(message, qt.Contains, "V2__seed.sql")
	// Nothing ran: the seed did not insert a second row, and the recorded
	// versions are untouched, so there is no dirty revision to clear either.
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 1)
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{legacyFlywayV1, legacyFlywayV2})
}

// TestCompatMigrateApply_LegacyFlywayRefusalPrintsWorkingRecovery runs the SQL
// the refusal prints, verbatim, and then applies again.
//
// The statements are extracted from the message rather than restated, so a
// change to what is printed and a change to what works cannot drift apart. An
// error message offering a repair nobody executed is the failure mode here.
func TestCompatMigrateApply_LegacyFlywayRefusalPrintsWorkingRecovery(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := legacyFlywayFixture(c)

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNotNil)

	statements := extractUpdateStatements(errorText(err) + stderr)
	c.Assert(statements, qt.HasLen, 2)
	execRecoverySQL(c, dbPath, statements)

	stdout, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute.")
	// The seed did not run a second time.
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 1)

	// And the directory keeps working: a migration added afterwards applies.
	writeAtlasApplyProjectMigration(c, dir, "V3__more.sql", "INSERT INTO seeded (id) VALUES (3);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err = runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 2)
}

// extractUpdateStatements pulls the recovery SQL out of the refusal. The
// message reaches a caller both as the returned error and on stderr, so
// identical statements are collapsed.
func extractUpdateStatements(message string) []string {
	var out []string
	seen := make(map[string]bool)
	for line := range strings.SplitSeq(message, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "UPDATE ") || !strings.HasSuffix(trimmed, ";") || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	return out
}

func execRecoverySQL(c *qt.C, dbPath string, statements []string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	for _, statement := range statements {
		_, err := db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// TestCompatMigrateApply_LegacyFlywayPartialRecoveryReportsOnlyWhatIsLeft covers
// an operator who ran some of the printed statements and stopped.
//
// It is the input that separates the detector's real rule from the plausible
// simpler one. "A legacy version is recorded" alone would report V1 again after
// its row had already been migrated forward; the rule is "a legacy version is
// recorded AND the version that file converts to today is not", so only the
// statement still outstanding is printed.
func TestCompatMigrateApply_LegacyFlywayPartialRecoveryReportsOnlyWhatIsLeft(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := legacyFlywayFixture(c)
	// Run only the first of the two statements.
	rewriteRevisionVersion(c, dbPath, legacyFlywayV1, "4611686018427469511")

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "1 already-applied migration(s)")
	c.Assert(message, qt.Contains, "V2__seed.sql")
	c.Assert(message, qt.Not(qt.Contains), "V1__init.sql")
	c.Assert(extractUpdateStatements(message), qt.HasLen, 1)
}

// TestCompatMigrateApply_LegacyFlywayDetectorDoesNotOverRefuse covers the
// databases that must keep working. The detector keys on a legacy version being
// recorded while the version that file converts to today is NOT, so a database
// this build wrote, and the ordinary Flyway workflow of adding a baseline that
// retires applied migrations, both pass through it.
func TestCompatMigrateApply_LegacyFlywayDetectorDoesNotOverRefuse(t *testing.T) {
	c := qt.New(t)

	c.Run("a database migrated only by this build", func(c *qt.C) {
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "current.db")
		writeAtlasApplyProjectMigration(c, dir, "V1__init.sql", "CREATE TABLE only_head (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)

		writeAtlasApplyProjectMigration(c, dir, "V2__next.sql", "CREATE TABLE only_head_2 (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		stdout, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	})

	c.Run("a goose directory whose encoding never changed", func(c *qt.C) {
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "goose.db")
		writeAtlasApplyProjectMigration(c, dir, "10000_init.sql", "-- +goose Up\nCREATE TABLE goose_legacy (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "goose")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=goose")
		c.Assert(err, qt.IsNil)

		// Version 10000 is recorded, which is a legacy-shaped Flyway version,
		// but this is a goose directory and goose never had the old encoding.
		writeAtlasApplyProjectMigration(c, dir, "20000_next.sql", "-- +goose Up\nCREATE TABLE goose_legacy_2 (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "goose")
		stdout, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=goose")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	})
}

// TestCompatMigrateApply_LegacyFlywayRefusalPrecedesExecutionOnDDL is the loud
// half of the same bug: a CREATE TABLE that re-runs fails and leaves a dirty
// revision behind, which then blocks every subsequent apply. The refusal has to
// come first so no dirty row is ever written.
func TestCompatMigrateApply_LegacyFlywayRefusalPrecedesExecutionOnDDL(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "ddl.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__init.sql", "CREATE TABLE ddl_once (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil)
	rewriteRevisionVersion(c, dbPath, "4611686018427469511", legacyFlywayV1)

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	c.Assert(errorText(err)+stderr, qt.Contains, "stokaro/ptah#982")
	// No dirty revision was recorded, so the operator is not left needing
	// --allow-dirty on top of the version rewrite.
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{legacyFlywayV1})
}

// TestCompatMigrateApply_FlywayBaselineAfterApply_KnownRegression pins a
// behavior this change BREAKS, so it is visible rather than latent.
//
// Adding a baseline to a directory that has already been applied is the ordinary
// Flyway baseline workflow. Measured 2026-08-02: Atlas CE applies it (exit 0,
// tables p,q,r,s) and so does ptah-compat built from 1c70ea1. This build refuses,
// because the converted baseline lands in the LOW band — below every version
// already recorded — and the migrator reads that as an out-of-order migration.
//
// The band is what makes a surviving baseline execute FIRST within one run,
// which is CE's order and the reason it exists. Removing it fixes this workflow
// and breaks that ordering; --exec-order=non-linear fixes this workflow and
// breaks the repeatable case, where CE itself refuses. No single setting gives
// parity on both, so the projection needs a decision rather than another
// unilateral change.
//
// This test asserts the CURRENT behavior only. It is not a statement that the
// behavior is acceptable.
func TestCompatMigrateApply_FlywayBaselineAfterApply_KnownRegression(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "squash.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__one.sql", "CREATE TABLE sq1 (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V2__two.sql", "CREATE TABLE sq2 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil)

	writeAtlasApplyProjectMigration(c, dir, "B3__base.sql", "CREATE TABLE sq3 (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V4__four.sql", "CREATE TABLE sq4 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations below current version")
	// It is NOT the #982 revision detector firing: this database was written by
	// this build, so the two failure modes are not being conflated.
	c.Assert(errorText(err)+stderr, qt.Not(qt.Contains), "stokaro/ptah#982")

	// --exec-order=non-linear reaches Atlas CE's outcome, which is the workaround
	// until the projection is decided.
	_, stderr, err = runCompat("migrate", "apply", "--exec-order", "non-linear",
		"--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
