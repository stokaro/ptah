package atlas_test

import (
	"database/sql"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"
)

// These tests cover the refusal that stops a converted Flyway directory from
// re-running migrations another Atlas implementation has already applied
// through it (stokaro/ptah#1100).
//
// Atlas CE records a converted Flyway migration under its SOURCE version token
// — measured on the pinned community binary v1.3.0, `1` and `2` for V1__a.sql
// and V2__b.sql — while Ptah's migrator records the int64 that token projects
// to. Each revision table is therefore unreadable to the other implementation,
// and in the direction where Ptah reads CE's table every file matches no row
// and the whole directory reads as pending.
//
// A foreign revision table is simulated by applying with THIS build and then
// rewriting each row into the other spelling, which keeps the fixture honest
// without needing a second binary on PATH.
const (
	foreignFlywayV1 = "4611686018427469511"
	foreignFlywayV2 = "4611686018427510315"
	// foreignFlywayHash is a checksum in the encoding the OTHER implementation
	// writes: measured, it records a base64 h1 digest where Ptah records hex
	// sha256. The fixture rewrites the hash as well as the version so that no
	// assertion below can pass because a checksum happened to match Ptah's own.
	foreignFlywayHash = "9mRxDpig5tZbhHslnEoWdaeiT7fFPnkxapig7dheO1w="
)

// recordForeignFlywayVersions rewrites the revision rows this build just wrote
// into the spelling the other implementation uses.
//
// Rows are paired with tokens by position. That is sound for these fixtures
// because a converted Flyway directory executes in the order the source tool
// executes it — that ordering is what flywayOrderingKey reproduces and it is
// measured against the pinned community binary — so the nth recorded revision
// is the nth token.
func recordForeignFlywayVersions(c *qt.C, dbPath string, tokens []string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	current := revisionVersionsByOrder(c, db)
	// A fixture that rewrote a different number of rows than it meant to would
	// leave the database on Ptah's own encoding and make every assertion below
	// vacuous.
	c.Assert(current, qt.HasLen, len(tokens))
	for i, from := range current {
		result, err := db.Exec(
			`UPDATE atlas_schema_revisions SET version = ?, hash = ? WHERE version = ?`,
			tokens[i], foreignFlywayHash, from,
		)
		c.Assert(err, qt.IsNil)
		affected, err := result.RowsAffected()
		c.Assert(err, qt.IsNil)
		c.Assert(affected, qt.Equals, int64(1))
	}
	c.Assert(revisionVersionsByOrder(c, db), qt.DeepEquals, tokens)
}

// renameForeignFlywayVersion rewrites ONE revision into the other spelling,
// leaving the rest on this build's own encoding.
//
// That is the mixed database: some migrations recorded here, some by the tool
// that wrote the source directory.
func renameForeignFlywayVersion(c *qt.C, dbPath, from, to string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	result, err := db.Exec(
		`UPDATE atlas_schema_revisions SET version = ?, hash = ? WHERE version = ?`,
		to, foreignFlywayHash, from,
	)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	// A fixture that rewrote nothing would leave the database entirely on this
	// build's encoding and make every assertion below vacuous.
	c.Assert(affected, qt.Equals, int64(1))
}

func revisionVersionsByOrder(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.Query(`SELECT version FROM atlas_schema_revisions ORDER BY CAST(version AS INTEGER)`)
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

// foreignFlywayFixture builds a hashed Flyway directory, applies it, and then
// rewrites both revisions into the other implementation's spelling.
//
// The bodies are deliberately replayable. That is the shape the refusal exists
// for: measured, a CREATE TABLE re-run fails loudly and strands a dirty
// revision, but this pair re-runs at exit 0 and inserts the seed row a second
// time with nothing in the exit status, stdout or `migrate status` saying so.
func foreignFlywayFixture(c *qt.C) (dir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	dbPath = filepath.Join(root, "foreign.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__init.sql", "CREATE TABLE IF NOT EXISTS seeded (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V2__seed.sql", "INSERT INTO seeded (id) VALUES (1);")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 1)

	recordForeignFlywayVersions(c, dbPath, []string{"1", "2"})
	return dir, dbPath
}

// TestCompatMigrateApply_ForeignFlywayRevisionsRefused is the reproduction.
//
// Reverting the change prints `Migrating to version 4611686018427510315 from 2
// pending migrations.` followed by `Migration complete.` at exit 0, and leaves
// two rows in `seeded` where there was one.
func TestCompatMigrateApply_ForeignFlywayRevisionsRefused(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := foreignFlywayFixture(c)

	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "another Atlas implementation")
	// The mapping is printed per file, because the int64 appears in no file
	// name and in no Atlas output: on its own it says nothing an operator can
	// act on.
	c.Assert(message, qt.Contains, foreignFlywayV1)
	c.Assert(message, qt.Contains, foreignFlywayV2)
	c.Assert(message, qt.Contains, "V1__init.sql")
	c.Assert(message, qt.Contains, "V2__seed.sql")
	// Nothing ran: the seed did not insert a second row, and the recorded
	// versions are untouched, so there is no dirty revision to clear either.
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 1)
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "2"})
}

// TestCompatMigrateApply_ForeignFlywayDryRunRefused covers the preview, which
// was not merely uninformative but wrong.
//
// Reverting the change prints `Dry run mode: no changes will be made.` and
// `Would have applied 2 migrations.` at exit 0 — a preview of work the database
// has already done.
func TestCompatMigrateApply_ForeignFlywayDryRunRefused(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := foreignFlywayFixture(c)

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath, "--dry-run")

	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(errorText(err)+stderr, qt.Contains, "another Atlas implementation")
	c.Assert(stdout, qt.Not(qt.Contains), "Would have applied")
}

// migrateSetVersion pulls the version out of the route the refusal prints, so
// a change to what is printed and a change to what works cannot drift apart.
var migrateSetVersion = regexp.MustCompile("`migrate set ([0-9]+)`")

// foreignFlywaySetOperandV2 is the operand the route prints for a database
// whose head is V2__*.sql: the SOURCE version token, not the ordering key it
// converts to.
//
// Since stokaro/ptah#1206 `migrate set` on a converted Flyway directory takes
// the token, and the ordering key — measured on the pinned community binary
// v1.3.0 — is a spelling that binary has always refused. Pinning the two values
// apart is what makes this an assertion rather than a restatement: before the
// change the route printed foreignFlywayV2 and running it worked; after it,
// printing foreignFlywayV2 would still match the regexp and then fail to run.
const foreignFlywaySetOperandV2 = "2"

// TestCompatMigrateApply_ForeignFlywayRefusalPrintsWorkingRecovery runs the
// route the refusal prints, verbatim, and then applies again.
//
// A refusal offering a repair nobody executed is the failure mode here, and the
// hand-written UPDATE the pre-#982 refusal prints does NOT work across
// implementations — measured, a bare version rewrite then fails with `migration
// 4611686018427469511 checksum mismatch: stored <base64>, current <hex>`,
// because the two also record the checksum differently. `migrate set` writes
// whole rows, which is why it is the route offered.
//
// WHAT THIS TEST DELIBERATELY NO LONGER ASSERTS. It used to end by adding
// `V3__more.sql` and requiring the apply to succeed, under the heading "the
// directory keeps working". That state is one the pinned community binary
// v1.3.0 REFUSES: measured, on the revision table this route leaves behind —
// `1`, `2`, 4611686018427469511, 4611686018427510315 — it prints `migration
// file V3__c.sql was added out of order` at exit 1 while this build applies at
// exit 0. Controls separate that from the added file itself: the same
// three-file directory on a fresh database, and on a database carrying only
// `1`,`2`, both apply at exit 0 on that binary. Pinning the old assertion
// pinned ptah-compat exiting 0 where the binary exits 1 as the desired result,
// so it is gone and the message says the switch is one way instead.
//
// Reverting the change makes the first apply succeed, so no message is produced
// and the regexp finds nothing: the test fails on the `qt.IsNotNil` above it.
func TestCompatMigrateApply_ForeignFlywayRefusalPrintsWorkingRecovery(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := foreignFlywayFixture(c)

	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNotNil)

	message := errorText(err) + stderr
	match := migrateSetVersion.FindStringSubmatch(message)
	c.Assert(match, qt.HasLen, 2)
	c.Assert(match[1], qt.Equals, foreignFlywaySetOperandV2)
	// The version alone does not say the route was OFFERED: the withdrawal
	// names the same command while telling the reader not to run it. Both
	// wordings have to be pinned or one can silently become the other.
	c.Assert(message, qt.Contains, "adopt the versions this build uses:")
	c.Assert(message, qt.Not(qt.Contains), "has no safe route here")
	// The two claims the offer now makes, both measured: it records only what
	// this database already ran, and it closes the other way forward.
	c.Assert(message, qt.Contains, "nothing is recorded that did not execute")
	c.Assert(message, qt.Contains, "one-way switch")
	c.Assert(message, qt.Not(qt.Contains), "still reads the result as up to date")

	stdout, stderr, err := runCompat(
		"migrate", "set", match[1],
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir+"?format=flyway",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	// Every version the route records is one of the two the refusal listed, so
	// "nothing is recorded that did not execute" is checked against what the
	// command actually wrote rather than against the sentence.
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals,
		[]string{"1", "2", foreignFlywayV1, foreignFlywayV2})

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")
	// The seed did not run a second time.
	c.Assert(countRows(c, dbPath, "seeded"), qt.Equals, 1)
}

// TestCompatMigrateApply_ForeignFlywaySetRouteWithdrawnWhenItWouldRecordUnrunMigrations
// is the input that separates a route which records only what ran from one
// that records whatever sits below the head.
//
// `migrate set V` writes a revision row for EVERY covered migration up to V,
// run or not. The head is the largest version among the migrations the other
// implementation DID run, and nothing makes the covered migrations below it a
// subset of those: `V1__g1.sql` and `V3__g3.sql` recorded as `1` and `3`, with
// `V2__g2.sql` added afterwards, puts an unapplied file underneath.
//
// Measured with the route still offered there: `migrate set 4611686018427551119`
// reports `(3 set)` — `+ 4611686018427510315 (g2)` among them — the next
// `migrate apply` prints `No migration files to execute` at exit 0, and table
// g2 is absent with its version recorded, so it can never run again. Following
// the printed instruction lost a migration and the refusal said nothing.
//
// The database is also worse off for the other implementation afterwards: on
// that five-row table the pinned community binary v1.3.0 prints `migration file
// V2__g2.sql was added out of order` at exit 1.
//
// Reverting the unrun clause prints `- adopt the versions this build uses:
// `migrate set 4611686018427551119“ as a way forward, which is the command
// that retires g2 unrun.
func TestCompatMigrateApply_ForeignFlywaySetRouteWithdrawnWhenItWouldRecordUnrunMigrations(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "gap.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__g1.sql", "CREATE TABLE g1 (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V3__g3.sql", "CREATE TABLE g3 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	recordForeignFlywayVersions(c, dbPath, []string{"1", "3"})

	// The gap: a migration between the two the other implementation ran.
	writeAtlasApplyProjectMigration(c, dir, "V2__g2.sql", "CREATE TABLE g2 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")

	_, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "has no safe route here")
	c.Assert(message, qt.Not(qt.Contains), "adopt the versions this build uses:")
	// The row it would have asserted is named — file and version — so the
	// operator can see what the command would claim rather than being told it
	// is safe.
	c.Assert(message, qt.Contains, "WITHOUT running them")
	c.Assert(message, qt.Contains, "V2__g2.sql ("+foreignFlywayV2+")")
	// Nothing ran and nothing was recorded: the database is exactly where the
	// other implementation left it.
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"g1", "g3"})
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "3"})
}

// TestCompatMigrateApply_ForeignFlywaySetRouteOfferedWithPendingAboveTheHead is
// the input that separates "records something that never ran" from "the
// directory has anything unapplied in it at all".
//
// `migrate set <head>` writes rows up to the head and no further, so a
// migration ABOVE the head is untouched by it and stays pending — which is the
// ordinary shape of an operator who switched tools with new work in flight.
// Withdrawing the route there would strand every such directory.
//
// Dropping the `migration.Version > head` skip in foreignFlywayUnrunBelowHead
// prints `adopting the versions this build uses has no safe route here ... it
// would record 1 migration(s) as applied WITHOUT running them — V3__c.sql
// (4611686018427551119)`, on a database `migrate set` would not touch.
func TestCompatMigrateApply_ForeignFlywaySetRouteOfferedWithPendingAboveTheHead(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "pending.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__a.sql", "CREATE TABLE pa (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V2__b.sql", "CREATE TABLE pb (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	recordForeignFlywayVersions(c, dbPath, []string{"1", "2"})

	// New work, above everything the other implementation ran.
	writeAtlasApplyProjectMigration(c, dir, "V3__c.sql", "CREATE TABLE pc (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")

	_, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "adopt the versions this build uses:")
	c.Assert(message, qt.Not(qt.Contains), "has no safe route here")

	match := migrateSetVersion.FindStringSubmatch(message)
	c.Assert(match, qt.HasLen, 2)
	c.Assert(match[1], qt.Equals, foreignFlywaySetOperandV2)
	stdout, stderr, err = runCompat(
		"migrate", "set", match[1],
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir+"?format=flyway",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	// V3 is neither recorded nor run: the route asserted only the two
	// migrations the refusal listed, and left the pending one pending.
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals,
		[]string{"1", "2", foreignFlywayV1, foreignFlywayV2})
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"pa", "pb"})
}

// TestCompatMigrateApply_ForeignFlywaySetRouteCountsThisBuildsOwnRowsAsRun is
// the mixed database: some migrations recorded here, one recorded by the tool
// that wrote the directory.
//
// A migration this build already recorded HAS run, so `migrate set` writing its
// row again asserts nothing new. Reading only the foreign half as evidence of
// execution would withdraw the route on a database where it loses nothing.
//
// Dropping the `slices.Contains(applied, migration.Version)` half of the skip
// prints `it would record 1 migration(s) as applied WITHOUT running them —
// V1__m.sql (4611686018427469511)` for a migration this build ran itself and
// whose revision row is right there.
func TestCompatMigrateApply_ForeignFlywaySetRouteCountsThisBuildsOwnRowsAsRun(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "mixed.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__m.sql", "CREATE TABLE m1 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))

	writeAtlasApplyProjectMigration(c, dir, "V2__n.sql", "CREATE TABLE m2 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	// Only the second row moves to the other spelling, so V1 stays recorded
	// under the version this build uses.
	renameForeignFlywayVersion(c, dbPath, foreignFlywayV2, "2")
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"2", foreignFlywayV1})

	_, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "V2__n.sql")
	c.Assert(message, qt.Contains, "adopt the versions this build uses:")
	c.Assert(message, qt.Not(qt.Contains), "WITHOUT running them")
	c.Assert(message, qt.Not(qt.Contains), "has no safe route here")
}

// TestCompatMigrateApply_ForeignFlywayTokenWithLeadingSpace is the input that
// separates reading the token the way the revision reader reads a recorded
// version from reading it raw.
//
// `V 1__x.sql` is an ordinary migration to Atlas CE: measured, it prints
// `-- migrating version  1` and records the version `[ 1]`, leading space and
// all. Ptah's parseAtlasRevisionVersion trims before parsing, so that row comes
// back as 1, and a comparison that did not trim the token would match nothing
// and let the file run a second time.
//
// Reverting the strings.TrimSpace prints `Migrating to version
// 4611686018427469511 from 2 pending migrations.` at exit 0 and inserts the
// seed row twice.
func TestCompatMigrateApply_ForeignFlywayTokenWithLeadingSpace(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "space.db")
	writeAtlasApplyProjectMigration(c, dir, "V 1__init.sql", "CREATE TABLE IF NOT EXISTS spaced (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V 2__seed.sql", "INSERT INTO spaced (id) VALUES (1);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	recordForeignFlywayVersions(c, dbPath, []string{" 1", " 2"})

	_, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(errorText(err)+stderr, qt.Contains, "another Atlas implementation")
	c.Assert(countRows(c, dbPath, "spaced"), qt.Equals, 1)
}

// TestCompatMigrateApply_ForeignFlywayBaselineOverForeignHistory covers a cell
// stokaro/ptah#1100 did not name and this change also closes.
//
// A baseline added to a directory another implementation has already applied is
// not a re-run of the SAME file, it is a squash executed on top of the history
// it squashes. Measured on the pinned community binary v1.3.0 with V1 and V2
// recorded as `1` and `2` and B2__base.sql added: the binary prints `No
// migration files to execute` at exit 0 and never creates the table, while
// master ptah-compat prints `Migrating to version 122412 from 1 pending
// migrations.` at exit 0 and creates it.
//
// The existing baseline refusal (stokaro/ptah#1003) does not reach this: it
// looks for the CONVERTED version of a same-token migration in the applied set,
// and on a foreign revision table that version is not there. The token
// comparison is what sees it.
//
// Reverting the change prints `Migrating to version 122412 from 1 pending
// migrations.` at exit 0 and leaves table base beside p and q.
func TestCompatMigrateApply_ForeignFlywayBaselineOverForeignHistory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "baseline.db")
	writeAtlasApplyProjectMigration(c, dir, "V1__p.sql", "CREATE TABLE p (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V2__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	recordForeignFlywayVersions(c, dbPath, []string{"1", "2"})

	writeAtlasApplyProjectMigration(c, dir, "B2__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")

	_, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "B2__base.sql")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"p", "q"})

	// The route the refusal prints settles this shape too: it removes nothing,
	// and the baseline the community binary skips is recorded rather than run.
	match := migrateSetVersion.FindStringSubmatch(message)
	c.Assert(match, qt.HasLen, 2)
	stdout, stderr, err = runCompat(
		"migrate", "set", match[1],
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir+"?format=flyway",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"p", "q"})
}

// TestCompatMigrateApply_ForeignFlywaySetRouteWithdrawnWhenUnsafe is the input
// that separates offering the route from offering it unconditionally.
//
// `migrate set V` moves the database to EXACTLY V and removes every revision
// above it. A converted baseline sits in the LOW band — B2__base.sql becomes
// 122412 — and a baseline squashes files whose token sorts at or below its own
// AS A STRING, so `V1000000__z.sql` ("1000000" < "2") is squashed out of the
// covered set while its revision row, 1000000, sits far above the head. Printing
// `migrate set 122412` there would delete the migration the database ran.
//
// Reverting the withdrawal prints `- adopt the versions this build uses:
// `migrate set 122412` ... records every migration up to and including that
// version as applied`, which is a command that retires revision 1000000.
func TestCompatMigrateApply_ForeignFlywaySetRouteWithdrawnWhenUnsafe(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "unsafe.db")
	writeAtlasApplyProjectMigration(c, dir, "V2__q.sql", "CREATE TABLE q (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V1000000__z.sql", "CREATE TABLE z (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	recordForeignFlywayVersions(c, dbPath, []string{"2", "1000000"})

	writeAtlasApplyProjectMigration(c, dir, "B2__base.sql", "CREATE TABLE base (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")

	_, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "has no safe route here")
	// The row it would have deleted is named, so the claim can be checked.
	c.Assert(message, qt.Contains, "1000000")
	c.Assert(message, qt.Not(qt.Contains), "adopt the versions this build uses:")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"q", "z"})
}

// TestCompatMigrateApply_ForeignFlywayTokenIsAlsoAConvertedVersion is the input
// that separates the rule that shipped from the looser one it could have been.
//
// A source token and a converted version share the int64 space, and a baseline
// converts into the LOW band: B10__base.sql becomes 448844. A directory that
// also holds V448844__x.sql therefore has a token equal to a version this same
// directory produces, so recording 448844 says nothing about which
// implementation wrote the row — and here THIS build wrote it, by applying the
// baseline alone.
//
// Reverting the third clause prints `this database records converted Flyway
// migrations under their SOURCE version token ... 448844 ->
// 4611686036742059283  V448844__x.sql` at exit 1 on a database ptah-compat had
// just written itself, and leaves table cv uncreated.
func TestCompatMigrateApply_ForeignFlywayTokenIsAlsoAConvertedVersion(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "collide.db")
	writeAtlasApplyProjectMigration(c, dir, "B10__base.sql", "CREATE TABLE cb (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, dir, "V448844__x.sql", "CREATE TABLE cv (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")

	// The baseline sorts first, so one migration is exactly the baseline.
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath, "1")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"448844"})

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"cb", "cv"})
}

// TestCompatMigrateApply_ForeignFlywayRepeatableCarriesNoToken is the input
// that separates reporting Atlas CE's version for a file from reporting the
// file's own token.
//
// Every repeatable is version "" to Atlas CE whatever its name, so `R2__r.sql`
// is not a migration CE ever records as `2`. Reporting its own token instead
// pairs it with any revision row that happens to be `2` and refuses a database
// on the strength of a file that cannot have written that row.
//
// The database here carries BOTH spellings of `V2__q.sql` — the shape an
// operator who inserted the new row instead of moving the old one leaves behind
// — so V2 itself is settled and the stray `2` is the only thing left to blame.
// The repeatable is then added, unapplied, so only the token decides.
//
// A repeatable added beside an applied `V2` IS refused, by the out-of-order
// guard, and that is the refusal Atlas CE reports for the same directory: its
// empty version sorts below the mark it compares against (stokaro/ptah#1098).
// So the assertion is on WHICH refusal speaks, not on whether one does.
//
// Reverting flywayCEVersion to file.version reports `this database records
// converted Flyway migrations under their SOURCE version token ... 2 ->
// 9223372036854775807  R2__r.sql` instead, blaming a file that cannot have
// written the row `2` and hiding the diagnosis CE agrees with.
func TestCompatMigrateApply_ForeignFlywayRepeatableCarriesNoToken(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "repeatable.db")
	writeAtlasApplyProjectMigration(c, dir, "V2__q.sql", "CREATE TABLE rq (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))

	// The row the other implementation would have written for V2, left beside
	// the one this build wrote rather than replacing it.
	copyRevisionRow(c, dbPath, foreignFlywayV2, "2")
	writeAtlasApplyProjectMigration(c, dir, "R2__r.sql", "CREATE TABLE rr (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, dir, "flyway")

	_, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "out-of-order pending migrations")
	c.Assert(message, qt.Not(qt.Contains), "SOURCE version token")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"rq"})
}

// foreignFlywayCase is one database that must keep applying.
type foreignFlywayCase struct {
	name   string
	format string
	// seed leaves the database and directory in the state under test.
	seed func(c *qt.C, dir, dbPath string)
	// added is written after the seed, then the directory is re-hashed.
	added []flywayMigration
	// tables is the full set of user tables expected afterwards.
	tables []string
}

// TestCompatMigrateApply_ForeignFlywayDetectorDoesNotOverRefuse covers the
// databases that must keep working. The detector keys on a file's SOURCE token
// being recorded while the version that file converts to is NOT, so a database
// this build wrote, a fresh one, and an ordinary baseline squash all pass
// through it.
//
// Every row here passes on master too — that is the point of a non-interference
// control. What each row would print under a BROKEN version of the check is
// stated on the row.
func TestCompatMigrateApply_ForeignFlywayDetectorDoesNotOverRefuse(t *testing.T) {
	tests := []foreignFlywayCase{{
		// Dropping the `slices.Contains(applied, migration.Version)` clause
		// refuses this one: `1 already-applied migration(s) read as pending`.
		name:   "a database migrated only by this build",
		format: "flyway",
		seed: seedFlyway(
			flywayMigration{"V1__one.sql", "CREATE TABLE h1 (id INTEGER PRIMARY KEY);"},
		),
		added:  []flywayMigration{{"V2__two.sql", "CREATE TABLE h2 (id INTEGER PRIMARY KEY);"}},
		tables: []string{"h1", "h2"},
	}, {
		// Dropping the `len(applied) == 0` early return leaves this unchanged,
		// so this row is a control rather than a discriminator: a fresh install
		// of a converted directory has to keep working at all.
		name:   "a fresh database",
		format: "flyway",
		seed:   seedNothing,
		added: []flywayMigration{
			{"V1__one.sql", "CREATE TABLE f1 (id INTEGER PRIMARY KEY);"},
			{"V2__two.sql", "CREATE TABLE f2 (id INTEGER PRIMARY KEY);"},
		},
		tables: []string{"f1", "f2"},
	}, {
		// The ordinary Flyway squash. B3 retires V2 from the covered set, so the
		// only recorded version belongs to no covered file at all. A rule phrased
		// as "a recorded version no converted file produces" — the shape the
		// issue sketched — refuses this, and Atlas CE executes it.
		name:   "a baseline that retires the applied history",
		format: "flyway",
		seed: seedFlyway(
			flywayMigration{"V2__s2.sql", "CREATE TABLE b2 (id INTEGER PRIMARY KEY);"},
		),
		added:  []flywayMigration{{"B3__base.sql", "CREATE TABLE bbase (id INTEGER PRIMARY KEY);"}},
		tables: []string{"b2", "bbase"},
	}, {
		// The layout gate. A goose directory is hashed over every .sql it holds
		// — measured, the pinned community binary covers both files below — but
		// goose executes only 1_a.sql, recording version 1. Removing the
		// FormatFlyway guard inside FlywayCoveredSourceVersions makes V1__x.sql
		// report token "1" against converted version 4611686018427469511, and
		// this apply refuses with `1 already-applied migration(s) read as
		// pending` on a goose database that has nothing to do with Flyway.
		name:   "a goose directory holding a Flyway-shaped file",
		format: "goose",
		seed: seedGoose(
			flywayMigration{"1_a.sql", "-- +goose Up\nCREATE TABLE ga (id INTEGER PRIMARY KEY);"},
			flywayMigration{"V1__x.sql", "CREATE TABLE gv (id INTEGER PRIMARY KEY);"},
		),
		added:  []flywayMigration{{"2_b.sql", "-- +goose Up\nCREATE TABLE gb (id INTEGER PRIMARY KEY);"}},
		tables: []string{"ga", "gb"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			dir := filepath.Join(root, "migrations")
			dbPath := filepath.Join(root, "keep.db")
			test.seed(c, dir, dbPath)
			writeFlywayMigrations(c, dir, test.added)
			hashConvertedApplyDir(c, dir, test.format)

			stdout, stderr, err := compatApplyConverted(dir, test.format, dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(userTables(c, dbPath), qt.DeepEquals, test.tables)
		})
	}
}

// seedGoose applies goose files first, leaving recorded migration history.
func seedGoose(files ...flywayMigration) func(c *qt.C, dir, dbPath string) {
	return func(c *qt.C, dir, dbPath string) {
		c.Helper()
		writeFlywayMigrations(c, dir, files)
		hashConvertedApplyDir(c, dir, "goose")
		stdout, stderr, err := compatApplyConverted(dir, "goose", dbPath)
		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		// A seed that recorded nothing would make the row vacuous: the check
		// under test returns early on a database with no history.
		c.Assert(revisionVersions(c, dbPath), qt.Not(qt.HasLen), 0)
	}
}
