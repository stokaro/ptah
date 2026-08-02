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
	copyRevisionRow(c, dbPath, legacyFlywayV1, "4611686018427469511")

	_, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	// Only V2 is outstanding; V1 already has its new row.
	c.Assert(message, qt.Contains, "1 already-applied migration(s)")
	c.Assert(message, qt.Contains, "V2__seed.sql")
	c.Assert(message, qt.Not(qt.Contains), "V1__init.sql")
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
	rewriteRevisionVersion(c, dbPath, "4611686018427551119", legacyFlywayV2)

	_, stderr, err = runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

	// The nested file contributes no pairing, so nothing claims this database
	// was written by an older build.
	c.Assert(errorText(err)+stderr, qt.Not(qt.Contains), "stokaro/ptah#982")
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
// exempted from the guard. See atlasmigrateimport.FlywayBaselineAtlasVersion.
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
		c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations below current version")
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
		c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations below current version")
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
	c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations below current version")
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
	c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations below current version")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"ga", "gc"})
}

// TestCompatMigrateApply_FlywayBaselineBelowHighWaterMark_KnownDivergence pins
// the two shapes where Ptah is LOUDER than Atlas CE, deliberately.
//
// CE decides pending-ness by comparing the version token as a STRING against the
// current high-water mark, so a baseline whose token does not sort above it is
// silently considered applied and never runs. Measured:
//
//	V2 applied, B10 added   -> CE "No migration files to execute", table base absent
//	V1,V2,V3 applied, B2 added -> CE "No migration files to execute", base absent
//
// Ptah keys pending-ness on the recorded set instead, so it runs the baseline.
// Whether ptah-compat should reproduce the silent skip for drop-in fidelity is
// stokaro/ptah#1003; it is a decision, not a defect, and neither side is
// implemented here.
func TestCompatMigrateApply_FlywayBaselineBelowHighWaterMark_KnownDivergence(t *testing.T) {
	c := qt.New(t)

	c.Run("a baseline whose token does not sort above the high-water mark", func(c *qt.C) {
		root := c.TempDir()
		dir := filepath.Join(root, "migrations")
		dbPath := filepath.Join(root, "skip.db")
		writeAtlasApplyProjectMigration(c, dir, "V1__p.sql", "CREATE TABLE kp (id INTEGER PRIMARY KEY);")
		writeAtlasApplyProjectMigration(c, dir, "V2__q.sql", "CREATE TABLE kq (id INTEGER PRIMARY KEY);")
		writeAtlasApplyProjectMigration(c, dir, "V3__t.sql", "CREATE TABLE kt (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)

		// B2 supersedes V1 and V2 in the covered set. Its token "2" does not
		// sort above CE's current "3", so CE never runs it.
		writeAtlasApplyProjectMigration(c, dir, "B2__base.sql", "CREATE TABLE kbase (id INTEGER PRIMARY KEY);")
		hashConvertedApplyDir(c, dir, "flyway")
		stdout, stderr, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		// Ptah runs it; Atlas CE leaves kbase absent.
		c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"kbase", "kp", "kq", "kt"})
	})
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
