//go:build integration

package ydb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/migration/migrator"
)

// The migrator's tests each own one directory, so they can clean up after
// themselves without touching what the rest of the package writes.

// migrationFiles is a migration directory held in memory.
func migrationFiles(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// newMigrator builds a migrator over files whose revision table lives in dir.
func newMigrator(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	files map[string]string,
	format migrator.RevisionTableFormat,
	dir string,
) *migrator.Migrator {
	c.Helper()
	m, err := migrator.NewFSMigrator(conn, migrationFiles(files))
	c.Assert(err, qt.IsNil)
	return m.WithRevisionTableFormat(format).WithMigrationsTable(dir, "")
}

// dropDirectory drops the tables a test made in dir, the migrator's own
// included, which the schema reader leaves out.
func dropDirectory(c *qt.C, conn *dbschema.DatabaseConnection, dir string, tables ...string) {
	c.Helper()
	for _, table := range append(tables, "schema_migrations", "schema_migrations_log", "atlas_schema_revisions",
		"ptah_migration_tags") {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(),
			fmt.Sprintf("DROP TABLE IF EXISTS `%s/%s`", dir, table)), qt.IsNil)
	}
}

// scalar reads one integer.
func scalar(c *qt.C, conn *dbschema.DatabaseConnection, query string) int64 {
	c.Helper()
	var value int64
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("query: %s", query))
	return value
}

// progress is what a test reads off one revision.
type progress struct {
	Version int64
	State   string
	Applied int
	Total   int
}

func revisionProgress(c *qt.C, m *migrator.Migrator) []progress {
	c.Helper()
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	out := make([]progress, 0, len(revisions))
	for _, revision := range revisions {
		out = append(out, progress{
			Version: revision.Version, State: revision.State, Applied: revision.Applied, Total: revision.Total,
		})
	}
	return out
}

// A YDB migration runs as the queries YDB runs it as: each scheme statement a
// query of its own, each run of data statements one query, and a named
// expression carried into every query after the one that defines it -- the
// UPDATE below uses $name across an ALTER TABLE, and the scheme query between
// them runs alone. The revision counts those queries, in both revision table
// formats, and a rollback undoes the migration.
func TestYDBMigrator_RunsQueriesAndRollsBack(t *testing.T) {
	tests := []struct {
		name   string
		format migrator.RevisionTableFormat
		dir    string
	}{
		{name: "ptah format", format: migrator.RevisionTableFormatPtah, dir: "ptah_ydb_mig_native"},
		{name: "atlas format", format: migrator.RevisionTableFormatAtlas, dir: "ptah_ydb_mig_atlas"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c)
			dropDirectory(c, conn, test.dir, "items", "tags")
			c.Cleanup(func() { dropDirectory(c, conn, test.dir, "items", "tags") })
			files := map[string]string{
				"0000000001_items.up.sql": "--!syntax_v1\n" +
					"CREATE TABLE `" + test.dir + "/items` (id Int64 NOT NULL, name Utf8, PRIMARY KEY (id));\n" +
					"$name = 'first'u;\n" +
					"UPSERT INTO `" + test.dir + "/items` (id, name) VALUES (1l, $name);\n" +
					"UPSERT INTO `" + test.dir + "/items` (id, name) VALUES (2l, $name || 'x'u);\n" +
					"ALTER TABLE `" + test.dir + "/items` ADD COLUMN note Utf8;\n" +
					"UPDATE `" + test.dir + "/items` SET note = $name WHERE id = 1l;\n",
				"0000000001_items.down.sql": "DROP TABLE `" + test.dir + "/items`;\n",
				"0000000002_tags.up.sql": "CREATE TABLE `" + test.dir + "/tags` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
					"INSERT INTO `" + test.dir + "/tags` (id) VALUES (1l);\n",
				"0000000002_tags.down.sql": "DROP TABLE `" + test.dir + "/tags`;\n",
			}
			m := newMigrator(c, conn, files, test.format, test.dir)

			presentBefore, err := m.MetadataPresent(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(presentBefore, qt.IsFalse)

			c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
			c.Assert(revisionProgress(c, m), qt.DeepEquals, []progress{
				{Version: 1, State: "applied", Applied: 4, Total: 4},
				{Version: 2, State: "applied", Applied: 2, Total: 2},
			})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+test.dir+"/items`"), qt.Equals, int64(2))
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+test.dir+"/items` WHERE note = 'first'u AND id = 1l"),
				qt.Equals, int64(1))
			c.Assert(tableNames(readScoped(c, conn, []string{test.dir})), qt.DeepEquals,
				[]string{test.dir + "|items", test.dir + "|tags"})

			c.Assert(m.MigrateDownTo(c.Context(), 0), qt.IsNil)
			c.Assert(revisionProgress(c, m), qt.HasLen, 0)
			c.Assert(tableNames(readScoped(c, conn, []string{test.dir})), qt.HasLen, 0)
		})
	}
}

// A migration that fails partway keeps the queries that ran before the
// failure -- YDB commits each on its own -- and records how many. After the
// failing query is fixed, an --allow-dirty run resumes at that query: the
// INSERT before it is not run again, which would answer `Conflict with
// existing key`, and its query, which opens with a named expression, carries
// no session state the resumed run would have to replay.
func TestYDBMigrator_ResumesWhereAFailedMigrationStopped(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	const dir = "ptah_ydb_mig_resume"
	dropDirectory(c, conn, dir, "a", "b")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "a", "b") })
	head := "CREATE TABLE `" + dir + "/a` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
		"$id = 1l;\nINSERT INTO `" + dir + "/a` (id) VALUES ($id);\n"
	broken := map[string]string{
		"0000000001_tables.up.sql":   head + "CREATE TABLE `" + dir + "/b` (id NoSuchType NOT NULL, PRIMARY KEY (id));\n",
		"0000000001_tables.down.sql": "DROP TABLE `" + dir + "/b`;\nDROP TABLE `" + dir + "/a`;\n",
	}
	fixed := map[string]string{
		"0000000001_tables.up.sql":   head + "CREATE TABLE `" + dir + "/b` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
		"0000000001_tables.down.sql": broken["0000000001_tables.down.sql"],
	}

	failed := newMigrator(c, conn, broken, migrator.RevisionTableFormatPtah, dir).MigrateUp(c.Context())

	c.Assert(failed, qt.ErrorMatches, `(?s)failed to apply migration 1: .*NoSuchType.*`)
	c.Assert(revisionProgress(c, newMigrator(c, conn, broken, migrator.RevisionTableFormatPtah, dir)),
		qt.DeepEquals, []progress{{Version: 1, State: "failed", Applied: 2, Total: 3}})
	c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/a`"), qt.Equals, int64(1))

	resumed := newMigrator(c, conn, fixed, migrator.RevisionTableFormatPtah, dir)
	c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	c.Assert(revisionProgress(c, resumed), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 3, Total: 3}})
	c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/a`"), qt.Equals, int64(1))
	c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|a", dir + "|b"})
}

// The data statements between two scheme statements run as one transaction:
// when the second INSERT conflicts, the first is not applied either, and the
// revision says no data query ran. The fixed file resumes at that query.
func TestYDBMigrator_RunsADataQueryAsOneTransaction(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	const dir = "ptah_ydb_mig_atomic"
	dropDirectory(c, conn, dir, "c")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "c") })
	create := "CREATE TABLE `" + dir + "/c` (id Int64 NOT NULL, PRIMARY KEY (id));\n"
	conflicting := map[string]string{
		"0000000001_rows.up.sql": create +
			"INSERT INTO `" + dir + "/c` (id) VALUES (1l);\nINSERT INTO `" + dir + "/c` (id) VALUES (1l);\n",
		"0000000001_rows.down.sql": "DROP TABLE `" + dir + "/c`;\n",
	}
	fixed := map[string]string{
		"0000000001_rows.up.sql": create +
			"INSERT INTO `" + dir + "/c` (id) VALUES (1l);\nINSERT INTO `" + dir + "/c` (id) VALUES (2l);\n",
		"0000000001_rows.down.sql": conflicting["0000000001_rows.down.sql"],
	}

	failed := newMigrator(c, conn, conflicting, migrator.RevisionTableFormatPtah, dir).MigrateUp(c.Context())

	c.Assert(failed, qt.ErrorMatches, `(?s)failed to apply migration 1: .*Conflict with existing key.*`)
	c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/c`"), qt.Equals, int64(0))
	c.Assert(revisionProgress(c, newMigrator(c, conn, conflicting, migrator.RevisionTableFormatPtah, dir)),
		qt.DeepEquals, []progress{{Version: 1, State: "failed", Applied: 1, Total: 2}})

	resumed := newMigrator(c, conn, fixed, migrator.RevisionTableFormatPtah, dir)
	c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/c`"), qt.Equals, int64(2))
}

// A body the migrator cannot split the way YDB reads it is refused before any
// of its statements runs and before its revision is written.
func TestYDBMigrator_RefusesWhatItCannotSplit(t *testing.T) {
	const dir = "ptah_ydb_mig_refuse"
	create := "CREATE TABLE `" + dir + "/r` (id Int64 NOT NULL, PRIMARY KEY (id));\n"
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "a client delimiter", body: "DELIMITER //\n" + create,
			wantErr: `(?s)migration 1 cannot run up on ydb: client delimiter directive in YQL: "DELIMITER //".*`},
		{name: "an Atlas delimiter", body: "-- atlas:delimiter //\n" + create,
			wantErr: `(?s)migration 1 cannot run up on ydb: client delimiter directive in YQL: "-- atlas:delimiter //".*`},
		{name: "the ANSI lexer", body: "--!ansi_lexer\n" + create,
			wantErr: `(?s)migration 1 cannot run up on ydb: unsupported YQL translation setting "--!ansi_lexer".*`},
		{name: "a commit", body: create + "INSERT INTO `" + dir + "/r` (id) VALUES (1l);\nCOMMIT;\n",
			wantErr: `(?s)migration 1 cannot run up on ydb: "COMMIT" controls a transaction.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c)
			dropDirectory(c, conn, dir, "r")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "r") })
			files := map[string]string{
				"0000000001_r.up.sql":   test.body,
				"0000000001_r.down.sql": "DROP TABLE `" + dir + "/r`;\n",
			}
			m := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir)

			err := m.MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(revisionProgress(c, m), qt.HasLen, 0)
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.HasLen, 0)
		})
	}
}

// A run waits for the migration lock another session holds: with a timeout it
// gives up and applies nothing, and without one it applies once the holder
// releases, and not before.
func TestYDBMigrator_WaitsForTheMigrationLock(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	holder := openYDB(c)
	const dir = "ptah_ydb_mig_lock"
	dropDirectory(c, conn, dir, "w")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "w") })
	files := map[string]string{
		"0000000001_w.up.sql":   "CREATE TABLE `" + dir + "/w` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
		"0000000001_w.down.sql": "DROP TABLE `" + dir + "/w`;\n",
	}
	m := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir).WithMigrationLockName("ptah_ydb_mig_lock")

	lock, err := dblock.Acquire(c.Context(), holder, "ptah_ydb_mig_lock", 0)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.Supported(), qt.IsTrue)

	timedOut := m.WithMigrationLockTimeout(time.Second).MigrateUp(c.Context())
	c.Assert(migrator.IsMigrationLockTimeout(timedOut), qt.IsTrue, qt.Commentf("error: %v", timedOut))
	c.Assert(timedOut, qt.ErrorMatches, `.*timed out acquiring migration lock "ptah_ydb_mig_lock" for ydb after 1s`)

	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- m.MigrateUp(context.Background()) }()
	time.Sleep(2 * time.Second)
	c.Assert(len(done), qt.Equals, 0)
	c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.HasLen, 0)
	c.Assert(lock.Release(context.Background()), qt.IsNil)

	c.Assert(<-done, qt.IsNil)
	c.Assert(time.Since(started) >= 2*time.Second, qt.IsTrue)
	c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|w"})
}

// Two runs started together over the same history apply it once: the lock
// makes the second wait, and it then finds nothing pending. The INSERT would
// answer `Conflict with existing key` if both ran it, and the revision
// INSERT would conflict as well.
func TestYDBMigrator_ConcurrentRunsApplyOnce(t *testing.T) {
	c := qt.New(t)
	const dir = "ptah_ydb_mig_parallel"
	conn := openYDB(c)
	dropDirectory(c, conn, dir, "p")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "p") })
	files := map[string]string{
		"0000000001_p.up.sql": "CREATE TABLE `" + dir + "/p` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
			"INSERT INTO `" + dir + "/p` (id) VALUES (1l);\n",
		"0000000001_p.down.sql": "DROP TABLE `" + dir + "/p`;\n",
	}
	first := newMigrator(c, openYDB(c), files, migrator.RevisionTableFormatPtah, dir)
	second := newMigrator(c, openYDB(c), files, migrator.RevisionTableFormatPtah, dir)

	results := make(chan error, 2)
	go func() { results <- first.MigrateUp(context.Background()) }()
	go func() { results <- second.MigrateUp(context.Background()) }()

	c.Assert(errors.Join(<-results, <-results), qt.IsNil)
	c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/p`"), qt.Equals, int64(1))
	c.Assert(revisionProgress(c, first), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 2, Total: 2}})
}

// The lock is a semaphore on a coordination node at the database root: a
// second session is refused it at once under dblock.NoWait while the first
// holds it, and takes it once the first releases.
func TestYDBLock_ExcludesASecondSession(t *testing.T) {
	c := qt.New(t)
	first := openYDB(c)
	second := openYDB(c)

	held, err := dblock.Acquire(c.Context(), first, "ptah_ydb_lock_probe", dblock.NoWait)
	c.Assert(err, qt.IsNil)
	refused, refusal := dblock.Acquire(c.Context(), second, "ptah_ydb_lock_probe", dblock.NoWait)
	c.Assert(refusal, qt.ErrorMatches, `advisory lock "ptah_ydb_lock_probe" on ydb is held by another session`)
	c.Assert(dblock.IsTimeout(refusal), qt.IsTrue)
	c.Assert(refused, qt.IsNil)
	c.Assert(held.Release(context.Background()), qt.IsNil)

	taken, err := dblock.Acquire(c.Context(), second, "ptah_ydb_lock_probe", dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Assert(taken.Release(context.Background()), qt.IsNil)
	c.Assert(directoryNames(c, c.Context()), qt.Contains, dblock.YDBLockNode)
}

// A tag is written with UPSERT, so recording it again moves it, and the log
// records each attempt. Both tables live in the migrations directory.
func TestYDBMigrator_RecordsTagsAndTheLog(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	const dir = "ptah_ydb_mig_tags"
	dropDirectory(c, conn, dir, "t")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "t") })
	files := map[string]string{
		"0000000001_t.up.sql":   "CREATE TABLE `" + dir + "/t` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
		"0000000001_t.down.sql": "DROP TABLE `" + dir + "/t`;\n",
	}
	m := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

	c.Assert(m.RecordMigrationTag(c.Context(), "release", 0), qt.IsNil)
	c.Assert(m.RecordMigrationTag(c.Context(), "release", 1), qt.IsNil)
	version, err := m.ResolveMigrationTag(c.Context(), "release")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(1))
	tags, err := m.MigrationTags(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.HasLen, 1)
	c.Assert(m.DeleteMigrationTag(c.Context(), "release"), qt.IsNil)
	c.Assert(m.DeleteMigrationTag(c.Context(), "release"), qt.ErrorIs, migrator.ErrMigrationTagNotFound)

	attempts, err := m.MigrationLog(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(string(attempts[0].Outcome.State), qt.Equals, "applied")
}

// Progress is recorded after each query rather than once the file is done:
// read during the run, the revision counts one more query after each of them.
// The file holds a scheme query, a run of data statements and another scheme
// query, and YDB commits each on its own.
func TestYDBMigrator_RecordsProgressAfterEachQuery(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	observer := openYDB(c)
	const dir = "ptah_ydb_mig_progress"
	dropDirectory(c, conn, dir, "g")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "g") })
	files := migrationFiles(map[string]string{
		"0000000001_g.up.sql": "CREATE TABLE `" + dir + "/g` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
			"INSERT INTO `" + dir + "/g` (id) VALUES (1l);\nINSERT INTO `" + dir + "/g` (id) VALUES (2l);\n" +
			"ALTER TABLE `" + dir + "/g` ADD COLUMN note Utf8;\n",
		"0000000001_g.down.sql": "DROP TABLE `" + dir + "/g`;\n",
	})
	var seen []int64
	record := migrator.StatementObserverFunc(func(ctx context.Context, _ migrator.StatementEvent) error {
		var applied int64
		err := observer.QueryRowContext(ctx,
			"SELECT applied FROM `"+dir+"/schema_migrations` WHERE version = 1l").Scan(&applied)
		seen = append(seen, applied)
		return err
	})
	m, err := migrator.NewFSMigrator(conn, files, migrator.WithStatementObserver(record))
	c.Assert(err, qt.IsNil)

	c.Assert(m.WithMigrationsTable(dir, "").MigrateUp(c.Context()), qt.IsNil)
	c.Assert(seen, qt.DeepEquals, []int64{1, 2, 3})
}

// A resume skips only a prefix it can prove unchanged: the digest of the
// committed queries is recorded with the progress, and a file whose committed
// INSERT was edited is refused rather than resumed past the edit.
func TestYDBMigrator_RefusesToResumeOverAnEditedPrefix(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	const dir = "ptah_ydb_mig_edited"
	dropDirectory(c, conn, dir, "e", "f")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "e", "f") })
	// The edit is in the INSERT, the second statement of the second query, so
	// a digest over statements rather than queries would stop short of it.
	body := func(value, fType string) string {
		return "CREATE TABLE `" + dir + "/e` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
			"$id = 1l;\nINSERT INTO `" + dir + "/e` (id) VALUES (" + value + ");\n" +
			"CREATE TABLE `" + dir + "/f` (id " + fType + " NOT NULL, PRIMARY KEY (id));\n"
	}
	files := func(up string) map[string]string {
		return map[string]string{
			"0000000001_e.up.sql":   up,
			"0000000001_e.down.sql": "DROP TABLE `" + dir + "/f`;\nDROP TABLE `" + dir + "/e`;\n",
		}
	}
	failed := newMigrator(c, conn, files(body("$id", "NoSuchType")), migrator.RevisionTableFormatPtah, dir).
		MigrateUp(c.Context())
	c.Assert(failed, qt.IsNotNil)

	edited := newMigrator(c, conn, files(body("$id * 7l", "Int64")), migrator.RevisionTableFormatPtah, dir)
	err := edited.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.ErrorMatches, `migration 1 cannot resume automatically: the already committed statement prefix `+
		`changed after 2 of 3 statements committed; inspect the database before choosing a repair point`)
	c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/e` WHERE id = 7l"), qt.Equals, int64(0))
	c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|e"})
}

// YDB cannot add a NOT NULL column to a table that has rows on every line
// Ptah supports, and no Ptah wrote a revision table there in an older layout,
// so a revision table missing a column was made by something else and is
// refused rather than altered.
func TestYDBMigrator_RefusesARevisionTableItDidNotCreate(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	const dir = "ptah_ydb_mig_foreign"
	dropDirectory(c, conn, dir)
	c.Cleanup(func() { dropDirectory(c, conn, dir) })
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE `"+dir+"/schema_migrations` "+
		"(version Int64 NOT NULL, description Utf8 NOT NULL, applied_at Timestamp NOT NULL, PRIMARY KEY (version))"),
		qt.IsNil)
	m := newMigrator(c, conn, map[string]string{
		"0000000001_x.up.sql":   "CREATE TABLE `" + dir + "/x` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
		"0000000001_x.down.sql": "DROP TABLE `" + dir + "/x`;\n",
	}, migrator.RevisionTableFormatPtah, dir)

	err := m.Initialize(c.Context())

	c.Assert(err, qt.ErrorMatches, "failed to prepare migrations revision columns: migration metadata table `"+
		dir+"/schema_migrations` has no column state, applied, total, error, error_stmt, execution_time_ms, "+
		"checksum, so Ptah did not create it; drop it and let Ptah create it, or configure another migrations table")
	c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.HasLen, 0)
}

