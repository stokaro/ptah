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

// These tests cover the refusal that stops a database migrated by an older
// Ptah build from re-running migrations it has already applied.
//
// Before #1206 Ptah persisted the numeric ordering key as revision identity;
// before #982 it used an even older numeric projection. This build persists the
// exact Flyway source token, so either retired key matches no current file and
// would make the migration pending.
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

// rewriteRevisionVersion moves one recorded revision to another version and
// restores the generic operator marker an older Ptah build wrote.
func rewriteRevisionVersion(c *qt.C, dbPath, from, to string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	result, err := db.Exec(`UPDATE atlas_schema_revisions SET version = ?, operator_version = 'Ptah' WHERE version = ?`, to, from)
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
// rewrites both exact tokens back to the pre-#982 encoding.
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

	rewriteRevisionVersion(c, dbPath, "1", legacyFlywayV1)
	rewriteRevisionVersion(c, dbPath, "2", legacyFlywayV2)
	return dir, dbPath
}

func TestCompatMigrateApply_LegacyFlywayRevisionsRefused(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := legacyFlywayFixture(c)

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "internal ordering key")
	// The mapping is printed per file, so the operator can see which migration
	// each row belongs to rather than only that something is wrong.
	c.Assert(message, qt.Contains, "10000")
	c.Assert(message, qt.Contains, `"1"`)
	c.Assert(message, qt.Contains, "V1__init.sql")
	c.Assert(message, qt.Contains, "V2__seed.sql")
	// Nothing ran: the seed did not insert a second row, and the recorded
	// versions are untouched, so there is no dirty revision to clear either.
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 1)
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{legacyFlywayV1, legacyFlywayV2})
}

func TestCompatMigrateApply_DirtyLegacyFlywayRevisionRefusedBeforeRetry(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "dirty-legacy.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__seed.sql", `
CREATE TABLE IF NOT EXISTS dirty_legacy_seed (id INTEGER PRIMARY KEY);
INSERT INTO dirty_legacy_seed (id) VALUES (1);
`)
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompat(
		"migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	rewriteRevisionVersion(c, dbPath, "1", legacyFlywayV1)
	markAtlasRevisionDirty(c, dbPath, legacyFlywayV1, 2)

	_, stderr, err = runCompat(
		"migrate", "apply", "--allow-dirty", "--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "internal ordering key")
	c.Assert(countRows(c, dbPath, "dirty_legacy_seed"), qt.Equals, 1)
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{legacyFlywayV1})
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
	c.Assert(stdout, qt.Contains, "No migration files to execute")
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

// TestCompatMigrateApply_LegacyFlywayBothVersionsRecorded is the input that
// separates the detector's rule from the plausible simpler one.
//
// "A legacy version is recorded" and "a legacy version is recorded AND the
// version that file converts to today is not" agree on every input where the
// legacy row was MOVED — rewriting it removes the legacy version, so both rules
// go quiet together. They disagree only when both versions are present at once,
// which is what an operator who inserted the new row instead of updating the old
// one leaves behind. The migration has demonstrably already run under its new
// identity, so re-reporting it would send them at a row that is already correct.
func TestCompatMigrateApply_LegacyFlywayBothVersionsRecorded(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := legacyFlywayFixture(c)
	copyRevisionRow(c, dbPath, legacyFlywayV1, "1")

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	// V1 already has its exact row, so its obsolete duplicate is deleted; V2's
	// obsolete row is updated to its exact token.
	c.Assert(message, qt.Contains, "2 obsolete revision row(s)")
	c.Assert(message, qt.Contains, "V2__seed.sql")
	c.Assert(message, qt.Contains, "V1__init.sql")
	c.Assert(message, qt.Contains, `DELETE FROM "atlas_schema_revisions" WHERE version = '10000';`)
	c.Assert(extractUpdateStatements(message), qt.HasLen, 1)
}

// copyRevisionRow duplicates a revision under a second version, leaving both.
func copyRevisionRow(c *qt.C, dbPath, from, to string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	result, err := db.Exec(`
INSERT INTO atlas_schema_revisions
  (version, description, type, applied, total, executed_at, execution_time, hash, operator_version)
SELECT ?, description, type, applied, total, executed_at, execution_time, hash, operator_version
FROM atlas_schema_revisions WHERE version = ?`, to, from)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	c.Assert(affected, qt.Equals, int64(1))
}

// TestCompatMigrateApply_LegacyFlywayIgnoresNestedFiles pins that the
// reconstruction only pairs files the OLD build would have executed.
//
// The pre-#982 importer read one directory level, so a nested migration never
// produced a revision row and no recorded version can have come from one.
//
// Reaching that guard takes a specific shape, because the old file-name regexp
// is anchored and matches the whole slash path: for an ordinary nested file such
// as sub/V2__nested.sql it fails on the "s" and the guard never decides
// anything. It decides only when the DIRECTORY name also parses — here V2__sub
// holding V3__x.sql, whose full path V2__sub/V3__x.sql matches the old pattern
// with version "2" and description "sub/V3__x". Without the guard that file
// pairs to the old encoding's 20000, and any unrelated row at 20000 becomes a
// refusal invented out of a file the old build never read.
func TestCompatMigrateApply_LegacyFlywayIgnoresNestedFiles(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "nested.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__init.sql", "CREATE TABLE IF NOT EXISTS n1 (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, filepath.Join(dir, "V2__sub"), "V3__x.sql", "CREATE TABLE IF NOT EXISTS n2 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))

	// A stale row at the version the nested file WOULD have had under the old
	// encoding, if the old build had been able to see it.
	rewriteRevisionVersion(c, dbPath, "3", legacyFlywayV2)

	_, stderr, err = runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	// The nested file contributes no pairing, so nothing claims this database
	// was written by an older build.
	c.Assert(errorText(err)+stderr, qt.Not(qt.Contains), "internal ordering key")
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

func TestCompatMigrateApply_AmbiguousLegacyFlywayIdentityRefusesBeforeExecution(t *testing.T) {
	c := qt.New(t)

	c.Run("an older V1 ordering key is not mistaken for exact V10000", func(c *qt.C) {
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "legacy.db")
		writeAtlasApplyProjectMigration(c, dir, "V1__first.sql",
			"CREATE TABLE ambiguous_v1 (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)
		rewriteRevisionVersion(c, dbPath, "1", legacyFlywayV1)

		writeAtlasApplyProjectMigration(c, dir, "V10000__collision.sql",
			"CREATE TABLE ambiguous_v10000 (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=flyway")

		c.Assert(err, qt.IsNotNil)
		message := errorText(err) + stderr
		c.Assert(message, qt.Contains, "ambiguous between an exact source token and an older Ptah ordering key")
		c.Assert(message, qt.Contains, "V1__first.sql")
		c.Assert(message, qt.Contains, "V10000__collision.sql")
		c.Assert(message, qt.Contains, "no repair SQL has been generated")
		c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{legacyFlywayV1})
		c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"ambiguous_v1"})
	})

	c.Run("an exact V10000 row is not rewritten as V1 history", func(c *qt.C) {
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "exact.db")
		writeAtlasApplyProjectMigration(c, dir, "V10000__collision.sql",
			"CREATE TABLE exact_v10000 (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)

		writeAtlasApplyProjectMigration(c, dir, "V1__first.sql",
			"CREATE TABLE exact_v1 (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=flyway")

		c.Assert(err, qt.IsNotNil)
		c.Assert(errorText(err)+stderr, qt.Contains,
			"ambiguous between an exact source token and an older Ptah ordering key")
		c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"10000"})
		c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"exact_v10000"})
	})

	c.Run("a retired exact V10000 row is not rewritten as V1 history", func(c *qt.C) {
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "retired-exact.db")
		writeAtlasApplyProjectMigration(c, dir, "V10000__collision.sql",
			"CREATE TABLE retired_exact_v10000 (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)
		c.Assert(os.Remove(filepath.Join(dir, "V10000__collision.sql")), qt.IsNil)

		writeAtlasApplyProjectMigration(c, dir, "V1__first.sql",
			"CREATE TABLE retired_exact_v1_must_not_run (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=flyway")

		c.Assert(err, qt.IsNotNil)
		message := errorText(err) + stderr
		c.Assert(message, qt.Contains, "automatic recovery cannot determine")
		c.Assert(message, qt.Not(qt.Contains), "UPDATE atlas_schema_revisions SET version = '1'")
		c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"10000"})
		c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"retired_exact_v10000"})
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
	rewriteRevisionVersion(c, dbPath, "1", legacyFlywayV1)

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	c.Assert(errorText(err)+stderr, qt.Contains, "internal ordering key")
	// No dirty revision was recorded, so the operator is not left needing
	// --allow-dirty on top of the version rewrite.
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{legacyFlywayV1})
}

// TestCompatMigrateApply_FlywayBaselineAfterApply is the parity assertion that
// replaced a regression this change briefly carried.
//
// Adding a baseline to a directory that has already been applied is the ordinary
// Flyway baseline workflow, and Atlas CE runs it — measured, including the case
// where the baseline's SQL cannot survive a second run, which CE fails loudly on.
// So it is "run this now", not "record it as history".
//
// The converted baseline lands in the LOW band, below every recorded version,
// which the linear guard used to read as an out-of-order migration. That
// position encodes Atlas CE's sum order, not authoring order, so the version is
// exempted from the guard. See atlasmigrateimport.FlywaySurvivingBaseline.
func TestCompatMigrateApply_FlywayBaselineAfterApply(t *testing.T) {
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
	stdout, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	// The baseline's SQL actually ran, matching Atlas CE, which creates the
	// table rather than merely recording the version.
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"sq1", "sq2", "sq3", "sq4"})
}

// TestCompatMigrateApply_FlywayOutOfOrderStillRefused is the input that
// separates the exemption that shipped from the looser one it could have been.
//
// "Exempt the surviving baseline" and "exempt anything that lands below the
// current version" agree on every shape with a baseline in it. They disagree
// here: an ordinary versioned migration inserted below the high-water mark is
// NOT an encoding artifact, and Atlas CE refuses it too — measured,
// `migration file V2__b.sql was added out of order`. A loose exemption would
// silently apply it.
func TestCompatMigrateApply_FlywayOutOfOrderStillRefused(t *testing.T) {
	c := qt.New(t)

	c.Run("an ordinary versioned migration below the high-water mark", func(c *qt.C) {
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "ooo.db")
		writeAtlasApplyProjectMigration(c, dir, "V1__a.sql", "CREATE TABLE oa (id INTEGER PRIMARY KEY);")
		writeAtlasApplyProjectMigration(c, dir, "V3__c.sql", "CREATE TABLE oc (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)

		writeAtlasApplyProjectMigration(c, dir, "V2__b.sql", "CREATE TABLE ob (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

		c.Assert(err, qt.IsNotNil)
		// The refusal names the source token, not only the projected int64:
		// 4611686018427510315 appears in no file name and in no Atlas output,
		// so on its own it does not say which file to move (#1098).
		c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations for current version")
		c.Assert(errorText(err)+stderr, qt.Contains, `"2"`)
		c.Assert(errorText(err)+stderr, qt.Not(qt.Contains), "461168")
		c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"oa", "oc"})
	})

	c.Run("a versioned migration added after a repeatable", func(c *qt.C) {
		// The repeatable occupies the reserved top slot, so anything added later
		// sorts below it. Atlas CE refuses this too, by its own out-of-order
		// check, so the exemption must not reach it.
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "rep.db")
		writeAtlasApplyProjectMigration(c, dir, "V1__a.sql", "CREATE TABLE ra (id INTEGER PRIMARY KEY);")
		writeAtlasApplyProjectMigration(c, dir, "R__v.sql", "CREATE TABLE rv (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)

		writeAtlasApplyProjectMigration(c, dir, "V2__b.sql", "CREATE TABLE rb (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

		c.Assert(err, qt.IsNotNil)
		c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations for current version")
		// Atlas CE gives every repeatable the empty version string, so the
		// current mark here has an empty source version and V2 is refused
		// against it. Printing the reserved int64 alone would say nothing.
		c.Assert(errorText(err)+stderr, qt.Contains, `version ""`)
		c.Assert(errorText(err)+stderr, qt.Not(qt.Contains), "922337")
	})
}

// TestCompatMigrateApply_FlywayExemptionIsOneVersion checks the exemption
// narrows to the baseline even when there IS a baseline to exempt.
//
// The subtests above cannot see a loosened exemption: with no baseline in the
// directory the exempt list is empty and the filter returns early, so a mutation
// inside it is unreachable. This shape has both — B0__base.sql, whose token "0"
// squashes nothing, plus V2__b.sql inserted below the high-water mark. The
// baseline must be exempted and the ordinary migration must not.
func TestCompatMigrateApply_FlywayExemptionIsOneVersion(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "both.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__a.sql", "CREATE TABLE ea (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V3__c.sql", "CREATE TABLE ec (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil)

	writeAtlasApplyProjectMigration(c, dir, "B0__base.sql", "CREATE TABLE ebase (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V2__b.sql", "CREATE TABLE eb (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	// V2 is still out of order; exempting it as well would apply it silently.
	c.Assert(err, qt.IsNotNil)
	c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations for current version")
	c.Assert(errorText(err)+stderr, qt.Contains, `"2"`)
	c.Assert(errorText(err)+stderr, qt.Not(qt.Contains), "461168")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"ea", "ec"})
}

// TestCompatMigrateApply_GooseKeepsTheOutOfOrderGuard checks the exemption is
// computed only for the Flyway layout.
//
// The separating input needs a collision, because a stray B-prefixed file in a
// non-Flyway directory is otherwise inert: goose skips it and the exempt version
// matches no goose migration. B0__base.sql converts to Atlas version 40804, so a
// goose directory holding a real 40804_b.sql inserted below the high-water mark
// is the shape where answering for every layout would exempt a migration that
// has nothing to do with any baseline.
func TestCompatMigrateApply_GooseKeepsTheOutOfOrderGuard(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "goose.db")
	writeAtlasApplyProjectMigration(c, dir, "10000_a.sql", "-- +goose Up\nCREATE TABLE ga (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "50000_c.sql", "-- +goose Up\nCREATE TABLE gc (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "goose")
	_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=goose")
	c.Assert(err, qt.IsNil)

	writeAtlasApplyProjectMigration(c, dir, "40804_b.sql", "-- +goose Up\nCREATE TABLE gb (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "B0__base.sql", "CREATE TABLE gbase (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "goose")
	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=goose")

	c.Assert(err, qt.IsNotNil)
	// "below current version" is the NATIVE wording, and this row is the one
	// that holds it. A goose directory carries no source version tokens, so the
	// refusal keeps the message and the operand it always had; only a converted
	// Flyway directory reports source versions (#1098).
	c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations below current version")
	c.Assert(errorText(err)+stderr, qt.Not(qt.Contains), "source version")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"ga", "gc"})
}

// flywayMigration is one file written into the directory under test.
type flywayMigration struct {
	name string
	sql  string
}

// flywayBaselineRun is the measured result of the apply under test.
type flywayBaselineRun struct {
	stdout string
	stderr string
	err    error
	dir    string
	dbPath string
}

// message is everything the caller could have seen: the refusal reaches a
// caller both as the returned error and on stderr.
func (r flywayBaselineRun) message() string {
	return errorText(r.err) + r.stderr
}

// writeFlywayMigrations writes files into the directory under test.
func writeFlywayMigrations(c *qt.C, dir string, files []flywayMigration) {
	c.Helper()
	for _, file := range files {
		writeAtlasApplyProjectMigration(c, dir, file.name, file.sql)
	}
}

// seedFlyway applies files first, leaving the database with recorded migration
// history — the precondition every refusing row below depends on.
func seedFlyway(files ...flywayMigration) func(c *qt.C, dir, dbPath string) {
	return func(c *qt.C, dir, dbPath string) {
		c.Helper()
		writeFlywayMigrations(c, dir, files)
		hashConvertedApplyDir(c, dir, "flyway")
		stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		// A seed that recorded nothing would make the row below vacuous: the
		// check under test returns early on a database with no history.
		c.Assert(revisionVersions(c, dbPath), qt.HasLen, len(files))
	}
}

// seedSameTokenFlywayBaseline records the indistinguishable settled side of
// the V2/B2 collision. Both files stay in the directory, the surviving B2 body
// runs on the first invocation, and the second invocation exercises the
// baseline-type discriminator documented by checkFlywayBaselineHistory.
func seedSameTokenFlywayBaseline(c *qt.C, dir, dbPath string) {
	c.Helper()
	writeFlywayMigrations(c, dir, []flywayMigration{
		{"V2__base.sql", "CREATE TABLE settled_base (id INTEGER PRIMARY KEY);"},
		{"B2__base.sql", "CREATE TABLE settled_base (id INTEGER PRIMARY KEY);"},
	})
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"settled_base"})
	c.Assert(revisionType(c, dbPath, "2"), qt.Equals, 3)
}

func seedAtlasCESameTokenFlywayBaseline(c *qt.C, dir, dbPath string) {
	c.Helper()
	seedSameTokenFlywayBaseline(c, dir, dbPath)
	data, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	checksum := ""
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		name, value, ok := strings.Cut(strings.TrimRight(line, "\r"), " ")
		if ok && name == "B2__base.sql" {
			checksum = strings.TrimPrefix(value, "h1:")
		}
	}
	c.Assert(checksum, qt.Not(qt.Equals), "")
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.Exec(
		"UPDATE atlas_schema_revisions SET type = 2, hash = ?, operator_version = 'Atlas CLI v1.3.0' WHERE version = '2'",
		checksum,
	)
	c.Assert(err, qt.IsNil)
}

func revisionType(c *qt.C, dbPath, version string) int {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var revisionType int
	c.Assert(db.QueryRow(
		"SELECT type FROM atlas_schema_revisions WHERE version = ?",
		version,
	).Scan(&revisionType), qt.IsNil)
	return revisionType
}

// TestCompatMigrateApply_FlywayBaselineAgainstRecordedHistory replaces
// TestCompatMigrateApply_FlywayBaselineBelowHighWaterMark_KnownDivergence,
// which pinned the divergence this decides (stokaro/ptah#1003).
//
// That test used V1,V2,V3 + B2, a shape that separates nothing: B2's token "2"
// is already a recorded revision there, so "skipped because that version is
// recorded" and "skipped because it sorts below the mark" both predict what was
// observed. V2 applied + B10__base.sql is the input that tells them apart —
// "10" is in no revision row and is numerically above the mark — and the pinned
// Atlas CE v1.3.0 still skips it, silently, at exit 0.
//
// Ptah refuses instead of reproducing that skip, and refuses instead of running
// the baseline blindly. See checkFlywayBaselineHistory for why, and for the
// three incompatible answers CE gives files of this one class. Each row below
// is a measured cell of that matrix; the CE column is stated per row so a
// future change to either side is comparable against something.
func TestCompatMigrateApply_FlywayBaselineAgainstRecordedHistory(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		seed   func(c *qt.C, dir, dbPath string)
		added  []flywayMigration
		args   []string
		assert func(c *qt.C, run flywayBaselineRun)
	}{{
		// The discriminating cell. CE: `No migration files to execute`, exit 0,
		// table base absent and no output saying so.
		name:  "a baseline the recorded history does not reach",
		seed:  seedFlyway(flywayMigration{"V2__s2.sql", "CREATE TABLE s2 (id INTEGER PRIMARY KEY);"}),
		added: []flywayMigration{{"B10__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			// Naming the source file is the whole difference from the silent
			// skip: exit status alone cannot tell a reported outcome from an
			// unreported one, and a no-op baseline body would leave the same
			// tables either way.
			c.Assert(run.message(), qt.Contains, "B10__base.sql")
			c.Assert(run.message(), qt.Contains, "Flyway baseline")
			// The blocking migration is named by its file, not by the int64 the
			// token projects to. Atlas CE calls this migration `2`; a message
			// asserting that 4611686018427510315 is in the way names a number
			// the operator can find in neither their directory nor CE's output.
			c.Assert(run.message(), qt.Contains, "V2__s2.sql")
			// The route the refusal offers, spelled the way the row below runs
			// it, so a message that starts naming some other flag cannot drift
			// away from the one that was measured to work.
			c.Assert(run.message(), qt.Contains, "--exec-order=non-linear")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"s2"})
		},
	}, {
		// One digit apart from the row above, opposite outcome, which is what
		// proves the rule is not "refuse every baseline". B3 squashes V2 out of
		// the covered set, so nothing recorded here survives it. CE: executes
		// the baseline, exit 0.
		name:  "a baseline that squashes the whole recorded history",
		seed:  seedFlyway(flywayMigration{"V2__s2.sql", "CREATE TABLE s2 (id INTEGER PRIMARY KEY);"}),
		added: []flywayMigration{{"B3__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base", "s2"})
		},
	}, {
		// Atlas CE records an executed B2 as ordinary applied type 2. A revision
		// row does not retain the B prefix, and the checksum cannot distinguish a
		// B2 body from a byte-identical V2 body. Ptah therefore fails closed even
		// for a CE-applied baseline; only Ptah's type-3 execution marker proves a
		// settled B2 without weakening the unsafe V2 -> B2 transition.
		name: "an Atlas CE applied same-token baseline remains ambiguous",
		seed: seedAtlasCESameTokenFlywayBaseline,
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			c.Assert(run.message(), qt.Contains, "B2__base.sql")
			c.Assert(run.message(), qt.Contains, "V2__base.sql")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"settled_base"})
		},
	}, {
		// The same relation with the numbers reversed: "2" outranks "10" as a
		// string, so B2 squashes V10 and CE executes it, exit 0. Without this
		// row a rule keyed on the numeric high-water mark would pass.
		name:  "a baseline that squashes a two-digit recorded version",
		seed:  seedFlyway(flywayMigration{"V10__s10.sql", "CREATE TABLE s10 (id INTEGER PRIMARY KEY);"}),
		added: []flywayMigration{{"B2__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base", "s10"})
		},
	}, {
		// The fresh-install path, which is what makes a converted Flyway
		// directory usable for new environments at all. Same final directory as
		// the discriminating row, empty database. CE: executes both, exit 0.
		name: "the same directory against a database with no history",
		seed: seedNothing,
		added: []flywayMigration{
			{"V2__s2.sql", "CREATE TABLE s2 (id INTEGER PRIMARY KEY);"},
			{"B10__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"},
		},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base", "s2"})
		},
	}, {
		// Exact token identity also makes an already-applied baseline disappear
		// from the pending plan. Its stored checksum proves the row belongs to the
		// baseline itself, so the new same-token transition refusal must not turn
		// an ordinary second run into a permanent error.
		name:  "an applied exact-identity baseline remains settled",
		seed:  seedNothing,
		added: []flywayMigration{{"B2__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			stdout, stderr, err := compatApplyConverted(run.dir, "flyway", run.dbPath)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "No migration files to execute")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base"})
		},
	}, {
		// Half (a): CE exits 1 here, `migration file B2.5__base.sql was added
		// out of order`, while ptah-compat applied it at exit 0 before this
		// change. The baseline's own token lands between two applied versions.
		name: "a baseline whose token lands between two applied versions",
		seed: seedFlyway(
			flywayMigration{"V1__p.sql", "CREATE TABLE p (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V2__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V3__t.sql", "CREATE TABLE t (id INTEGER PRIMARY KEY);"},
		),
		added: []flywayMigration{{"B2.5__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			c.Assert(run.message(), qt.Contains, "B2.5__base.sql")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"p", "q", "t"})
		},
	}, {
		// Half (a) again, with two-digit versions: CE exits 1 naming
		// B15__base.sql, ptah-compat applied it at exit 0 before this change.
		name: "a two-digit baseline between two applied versions",
		seed: seedFlyway(
			flywayMigration{"V10__p.sql", "CREATE TABLE p (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V20__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);"},
		),
		added: []flywayMigration{{"B15__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			c.Assert(run.message(), qt.Contains, "B15__base.sql")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"p", "q"})
		},
	}, {
		// The ordinary squash: B2 retires V1 and V2, and version 2 is already
		// applied here under the file that was retired. Nothing recorded
		// survives in the covered set, so only the second clause of the rule
		// sees this one. CE: silent skip, exit 0.
		name: "a baseline carrying a version already applied",
		seed: seedFlyway(
			flywayMigration{"V1__p.sql", "CREATE TABLE p (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V2__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);"},
		),
		added: []flywayMigration{{"B2__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			c.Assert(run.message(), qt.Contains, "B2__base.sql")
			// The already-applied migration of the same version is named by the
			// file it is, which is what the operator can look at. It is also the
			// file the rows below prove is NOT found by a numeric comparison.
			c.Assert(run.message(), qt.Contains, "V2__q.sql")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"p", "q"})
		},
	}, {
		// Zero padding is ordinary Flyway practice, and flywayOrderingKey scores
		// "2" and "02" identically ON PURPOSE — the two must run in the same
		// position. Identity is the other question, and Atlas CE answers it on
		// the token: measured, its stdout here is `Migrating to version 2 from
		// 02` and it executes the baseline at exit 0, as did Ptah before the
		// refusal landed. Answering identity with the ordering key instead
		// refused this directory; reverting the fix prints `B2__base.sql is a
		// Flyway baseline and this database already has migration history ...
		// version 4611686018427510315 — a migration of the same version — is
		// already applied` at exit 1, leaving tables p and q without base.
		name: "a baseline beside two-digit zero-padded versions",
		seed: seedFlyway(
			flywayMigration{"V01__p.sql", "CREATE TABLE p (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V02__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);"},
		),
		added: []flywayMigration{{"B2__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base", "p", "q"})
		},
	}, {
		// Neither description nor SQL can distinguish the exact-token
		// transition: both files record "base" and carry the same body. The
		// converted directory has no per-file atlas.sum checksum, and the revision
		// row carries no V/B prefix or source filename, so it cannot prove which
		// one ran. The only safe answer is the named fail-closed refusal.
		name:  "a same-token baseline sharing the recorded description",
		seed:  seedFlyway(flywayMigration{"V2__base.sql", "CREATE TABLE old_base (id INTEGER PRIMARY KEY);"}),
		added: []flywayMigration{{"B2__base.sql", "CREATE TABLE old_base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			c.Assert(run.message(), qt.Contains, "B2__base.sql")
			c.Assert(run.message(), qt.Contains, "V2__base.sql")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"old_base"})
		},
	}, {
		// This is the other side of the otherwise-indistinguishable state. A
		// fresh directory applied the B2 body on the first run and Ptah recorded
		// its baseline type, so a later run can prove this is the settled B2 rather
		// than the unsafe V2 -> B2 transition above.
		name: "a settled same-token baseline remains a no-op on a later run",
		seed: seedSameTokenFlywayBaseline,
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(run.stdout, qt.Contains, "No migration files to execute")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"settled_base"})
		},
	}, {
		// The padding on the BASELINE's own token, against versions padded one
		// digit further, so neither side of the comparison is the bare number.
		// CE: `Migrating to version 02 from 002`, exit 0. Reverting the fix
		// prints `B02__base.sql is a Flyway baseline ... version
		// 4611686018427510315 — a migration of the same version — is already
		// applied` at exit 1, leaving tables p and q without base.
		name: "a zero-padded baseline beside three-digit zero-padded versions",
		seed: seedFlyway(
			flywayMigration{"V001__p.sql", "CREATE TABLE p (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V002__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);"},
		),
		added: []flywayMigration{{"B02__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base", "p", "q"})
		},
	}, {
		// A baseline landing in the MIDDLE of a padded run, so the collided
		// version is neither the first nor the last revision — the ordering key
		// picks it out of the middle of the recorded set just as readily. CE:
		// `Migrating to version 6 from 07`, exit 0. Reverting the fix prints
		// `B6__base.sql is a Flyway baseline ... version 4611686018427673531 — a
		// migration of the same version — is already applied` at exit 1, leaving
		// tables p, q and t without base.
		name: "a baseline matching the middle of a zero-padded run",
		seed: seedFlyway(
			flywayMigration{"V05__p.sql", "CREATE TABLE p (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V06__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V07__t.sql", "CREATE TABLE t (id INTEGER PRIMARY KEY);"},
		),
		added: []flywayMigration{{"B6__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base", "p", "q", "t"})
		},
	}, {
		// The way forward the refusal prints, executed verbatim, and then the
		// ordinary run that follows it. A refusal offering a route nobody ran is
		// one failure mode; a route that leaves the directory refused forever is
		// the other. CE applies a below-mark baseline under this flag too —
		// measured on B2.5.
		name:  "the printed escape hatch runs the baseline and settles the directory",
		seed:  seedFlyway(flywayMigration{"V2__s2.sql", "CREATE TABLE s2 (id INTEGER PRIMARY KEY);"}),
		added: []flywayMigration{{"B10__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		args:  []string{"--exec-order", "non-linear"},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"base", "s2"})

			// The baseline is recorded now, so the next linear apply must be a
			// quiet no-op rather than the same refusal a second time.
			stdout, stderr, err := compatApplyConverted(run.dir, "flyway", run.dbPath)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "No migration files to execute")
		},
	}, {
		// The prefix control: identical token, identical database, only B -> V.
		// The new check must not have become a blanket "anything below the mark
		// is refused", and the out-of-order guard must still be the one that
		// speaks for an ordinary migration.
		name:  "an ordinary migration below the mark keeps the out-of-order refusal",
		seed:  seedFlyway(flywayMigration{"V2__s2.sql", "CREATE TABLE s2 (id INTEGER PRIMARY KEY);"}),
		added: []flywayMigration{{"V1__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);"}},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			// A converted directory names the SOURCE tokens (#1098). The int64
			// appears in no file name and in no Atlas output, so printing it
			// alone does not say which file to move. Reverting #1098's message
			// change prints "below current version" and no source version here.
			c.Assert(run.message(), qt.Contains, "out-of-order pending migrations for current version")
			c.Assert(run.message(), qt.Contains, `version "2"`)
			c.Assert(run.message(), qt.Contains, `: "1"`)
			c.Assert(run.message(), qt.Not(qt.Contains), "461168")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"s2"})
		},
	}, {
		// Both refusals apply to this directory, and the out-of-order one wins:
		// it is the refusal CE reports for the same shape (measured, naming
		// V2__b.sql) and the one whose flags resolve it.
		name: "an out-of-order migration beside a baseline reports the guard",
		seed: seedFlyway(
			flywayMigration{"V1__a.sql", "CREATE TABLE ea (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V3__c.sql", "CREATE TABLE ec (id INTEGER PRIMARY KEY);"},
		),
		added: []flywayMigration{
			{"B0__base.sql", "CREATE TABLE ebase (id INTEGER PRIMARY KEY);"},
			{"V2__b.sql", "CREATE TABLE eb (id INTEGER PRIMARY KEY);"},
		},
		assert: func(c *qt.C, run flywayBaselineRun) {
			c.Assert(run.err, qt.IsNotNil, qt.Commentf("stdout:\n%s", run.stdout))
			c.Assert(run.message(), qt.Contains, "out-of-order pending migrations for current version")
			c.Assert(run.message(), qt.Contains, `version "3"`)
			c.Assert(run.message(), qt.Contains, `"2"`)
			c.Assert(run.message(), qt.Not(qt.Contains), "461168")
			c.Assert(run.message(), qt.Not(qt.Contains), "Flyway baseline")
			c.Assert(userTables(c, run.dbPath), qt.DeepEquals, []string{"ea", "ec"})
		},
	}}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			root := c.TempDir()
			dir := filepath.Join(root, "migrations")
			dbPath := filepath.Join(root, "baseline.db")
			test.seed(c, dir, dbPath)
			writeFlywayMigrations(c, dir, test.added)
			hashConvertedApplyDir(c, dir, "flyway")

			stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath, test.args...)

			test.assert(c, flywayBaselineRun{stdout: stdout, stderr: stderr, err: err, dir: dir, dbPath: dbPath})
		})
	}
}

// userTables lists the non-Atlas tables in a sqlite database, sorted.
func userTables(c *qt.C, dbPath string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'
		AND name NOT LIKE 'atlas%' AND name NOT LIKE 'sqlite%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var out []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		out = append(out, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return out
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
