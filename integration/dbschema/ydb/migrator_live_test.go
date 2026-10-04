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
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)
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
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
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
		})
	}
}

// The data statements between two scheme statements run as one transaction:
// when the second INSERT conflicts, the first is not applied either, and the
// revision says no data query ran. The fixed file resumes at that query.
func TestYDBMigrator_RunsADataQueryAsOneTransaction(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
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
		})
	}
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
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)
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
		})
	}
}

// A run waits for the migration lock another session holds: with a timeout it
// gives up and applies nothing, and without one it applies once the holder
// releases, and not before.
func TestYDBMigrator_WaitsForTheMigrationLock(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			holder := openYDB(c, line)
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
			c.Assert(done, qt.HasLen, 0)
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.HasLen, 0)
			c.Assert(lock.Release(context.Background()), qt.IsNil)

			c.Assert(<-done, qt.IsNil)
			c.Assert(time.Since(started) >= 2*time.Second, qt.IsTrue)
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|w"})
		})
	}
}

// Two runs started together over the same history apply it once: the lock
// makes the second wait, and it then finds nothing pending. The INSERT would
// answer `Conflict with existing key` if both ran it, and the revision
// INSERT would conflict as well.
func TestYDBMigrator_ConcurrentRunsApplyOnce(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			const dir = "ptah_ydb_mig_parallel"
			conn := openYDB(c, line)
			dropDirectory(c, conn, dir, "p")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "p") })
			files := map[string]string{
				"0000000001_p.up.sql": "CREATE TABLE `" + dir + "/p` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
					"INSERT INTO `" + dir + "/p` (id) VALUES (1l);\n",
				"0000000001_p.down.sql": "DROP TABLE `" + dir + "/p`;\n",
			}
			first := newMigrator(c, openYDB(c, line), files, migrator.RevisionTableFormatPtah, dir)
			second := newMigrator(c, openYDB(c, line), files, migrator.RevisionTableFormatPtah, dir)

			results := make(chan error, 2)
			go func() { results <- first.MigrateUp(context.Background()) }()
			go func() { results <- second.MigrateUp(context.Background()) }()

			c.Assert(errors.Join(<-results, <-results), qt.IsNil)
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/p`"), qt.Equals, int64(1))
			c.Assert(revisionProgress(c, first), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 2, Total: 2}})
		})
	}
}

// The lock is a semaphore on a coordination node at the database root: a
// second session is refused it at once under dblock.NoWait while the first
// holds it, and takes it once the first releases.
func TestYDBLock_ExcludesASecondSession(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			first := openYDB(c, line)
			second := openYDB(c, line)

			held, err := dblock.Acquire(c.Context(), first, "ptah_ydb_lock_probe", dblock.NoWait)
			c.Assert(err, qt.IsNil)
			asked := time.Now()
			refused, refusal := dblock.Acquire(c.Context(), second, "ptah_ydb_lock_probe", dblock.NoWait)
			// NoWait asks the server not to queue the request: measured, it answers
			// within milliseconds rather than after a wait.
			c.Assert(time.Since(asked) < 10*time.Second, qt.IsTrue, qt.Commentf("waited %s", time.Since(asked)))
			c.Assert(refusal, qt.ErrorMatches, `advisory lock "ptah_ydb_lock_probe" on ydb is held by another session`)
			c.Assert(dblock.IsTimeout(refusal), qt.IsTrue)
			c.Assert(refused, qt.IsNil)
			c.Assert(held.Release(context.Background()), qt.IsNil)

			taken, err := dblock.Acquire(c.Context(), second, "ptah_ydb_lock_probe", dblock.NoWait)
			c.Assert(err, qt.IsNil)
			c.Assert(taken.Release(context.Background()), qt.IsNil)
			c.Assert(directoryNames(c, c.Context(), line), qt.Contains, dblock.YDBLockNode)
		})
	}
}

// A tag is written with UPSERT, so recording it again moves it, and the log
// records each attempt. Both tables live in the migrations directory.
func TestYDBMigrator_RecordsTagsAndTheLog(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
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
			c.Assert(directoryNames(c, c.Context(), line, dir), qt.Contains, "ptah_migration_tags")
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|t"})
		})
	}
}

// Progress is recorded after each query rather than once the file is done:
// read during the run, the revision counts one more query after each of them.
// The file holds a scheme query, a run of data statements and another scheme
// query, and YDB commits each on its own.
func TestYDBMigrator_RecordsProgressAfterEachQuery(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			observer := openYDB(c, line)
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
		})
	}
}

// A resume skips only a prefix it can prove unchanged: the digest of the
// committed queries is recorded with the progress, and a file whose committed
// INSERT was edited is refused rather than resumed past the edit.
func TestYDBMigrator_RefusesToResumeOverAnEditedPrefix(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
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
		})
	}
}

// YDB cannot add a NOT NULL column to a table that has rows on every line
// Ptah supports, and no Ptah wrote a revision table there in an older layout,
// so a revision table missing a column was made by something else and is
// refused rather than altered.
func TestYDBMigrator_RefusesARevisionTableItDidNotCreate(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
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
		})
	}
}

// The migrations directory need not exist: a revision table there is absent
// until the first run creates the directory along with it, and asking about
// it creates nothing.
func TestYDBMigrator_AsksAboutADirectoryThatDoesNotExist(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dir := fmt.Sprintf("ptah_ydb_mig_absent_%d", time.Now().UnixNano())
			m := newMigrator(c, conn, map[string]string{
				"0000000001_a.up.sql":   "CREATE TABLE `" + dir + "/a` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
				"0000000001_a.down.sql": "DROP TABLE `" + dir + "/a`;\n",
			}, migrator.RevisionTableFormatPtah, dir)

			present, err := m.MetadataPresent(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(present, qt.IsFalse)
			c.Assert(directoryNames(c, c.Context(), line), qt.Not(qt.Contains), dir)
		})
	}
}

// offeredStatements is an interceptor that takes over nothing and records each
// statement the migrator offered it before running the statement itself.
type offeredStatements struct{ offered []string }

func (o *offeredStatements) ValidateDirectives(map[string]string) error { return nil }

func (o *offeredStatements) ExecuteStatement(
	_ context.Context, _ *dbschema.DatabaseConnection, statement string, _ map[string]string,
) (bool, error) {
	o.offered = append(o.offered, statement)
	return false, nil
}

// A data query is not run the way a scheme query is -- marked in flight,
// executed, then checkpointed -- but in one transaction with its checkpoint,
// so the steps a scheme query passes through, an interceptor among them, never
// see it.
func TestYDBMigrator_RunsADataQueryWithItsCheckpoint(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_commit"
			dropDirectory(c, conn, dir, "h")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "h") })
			files := migrationFiles(map[string]string{
				"0000000001_h.up.sql": "CREATE TABLE `" + dir + "/h` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
					"INSERT INTO `" + dir + "/h` (id) VALUES (1l);\n" +
					"ALTER TABLE `" + dir + "/h` ADD COLUMN note Utf8;\n",
				"0000000001_h.down.sql": "DROP TABLE `" + dir + "/h`;\n",
			})
			interceptor := &offeredStatements{}
			m, err := migrator.NewFSMigrator(conn, files, migrator.WithStatementInterceptor(interceptor))
			c.Assert(err, qt.IsNil)

			c.Assert(m.WithMigrationsTable(dir, "").MigrateUp(c.Context()), qt.IsNil)
			c.Assert(interceptor.offered, qt.DeepEquals, []string{
				"CREATE TABLE `" + dir + "/h` (id Int64 NOT NULL, PRIMARY KEY (id))",
				"ALTER TABLE `" + dir + "/h` ADD COLUMN note Utf8",
			})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/h`"), qt.Equals, int64(1))
		})
	}
}

// lostAnswer is an interceptor that runs one statement on a connection of its
// own, records it as committed the way a checkpoint does, and then reports a
// failure: a client whose server applied a statement and whose answer was lost
// on the way back. Every other statement it leaves to the migrator.
type lostAnswer struct {
	other      *dbschema.DatabaseConnection
	statement  string
	checkpoint string
}

func (l *lostAnswer) ValidateDirectives(map[string]string) error { return nil }

func (l *lostAnswer) ExecuteStatement(
	ctx context.Context, _ *dbschema.DatabaseConnection, statement string, _ map[string]string,
) (bool, error) {
	if statement != l.statement {
		return false, nil
	}
	if err := l.other.Writer().ExecuteSQL(ctx, statement); err != nil {
		return false, err
	}
	if err := l.other.Writer().ExecuteSQL(ctx, l.checkpoint); err != nil {
		return false, err
	}
	return false, errors.New("the connection closed before the answer arrived")
}

// The revision row is the record of what a body committed, and a failure
// never records less: a statement whose answer was lost after the row
// recorded it stays recorded, and the resume does not run it again -- here a
// second CREATE TABLE, which would fail; for a data query, a second UPDATE.
func TestYDBMigrator_FailureKeepsTheProgressTheRowRecords(t *testing.T) {
	tests := []struct {
		name       string
		format     migrator.RevisionTableFormat
		dir        string
		checkpoint string
	}{
		{
			name:   "ptah format",
			format: migrator.RevisionTableFormatPtah,
			dir:    "ptah_ydb_mig_lost_native",
			checkpoint: "UPDATE `ptah_ydb_mig_lost_native/schema_migrations` " +
				"SET applied = 2l, error = NULL, error_stmt = NULL WHERE version = 1l",
		},
		{
			name:   "atlas format",
			format: migrator.RevisionTableFormatAtlas,
			dir:    "ptah_ydb_mig_lost_atlas",
			checkpoint: "UPDATE `ptah_ydb_mig_lost_atlas/atlas_schema_revisions` " +
				"SET applied = 2l, error = ''u, error_stmt = ''u WHERE version = '1'u",
		},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)
					dropDirectory(c, conn, test.dir, "l", "m")
					c.Cleanup(func() { dropDirectory(c, conn, test.dir, "l", "m") })
					lost := "CREATE TABLE `" + test.dir + "/m` (id Int64 NOT NULL, PRIMARY KEY (id))"
					files := migrationFiles(map[string]string{
						"0000000001_l.up.sql": "CREATE TABLE `" + test.dir + "/l` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
							lost + ";\nINSERT INTO `" + test.dir + "/l` (id) VALUES (1l);\n",
						"0000000001_l.down.sql": "DROP TABLE `" + test.dir + "/m`;\nDROP TABLE `" + test.dir + "/l`;\n",
					})
					interceptor := &lostAnswer{other: openYDB(c, line), statement: lost, checkpoint: test.checkpoint}
					failing, err := migrator.NewFSMigrator(conn, files, migrator.WithStatementInterceptor(interceptor))
					c.Assert(err, qt.IsNil)

					failed := failing.WithRevisionTableFormat(test.format).WithMigrationsTable(test.dir, "").
						MigrateUp(c.Context())

					c.Assert(failed, qt.ErrorMatches, `(?s).*the connection closed before the answer arrived.*`)
					resumed, err := migrator.NewFSMigrator(conn, files)
					c.Assert(err, qt.IsNil)
					resumed = resumed.WithRevisionTableFormat(test.format).WithMigrationsTable(test.dir, "")
					c.Assert(revisionProgress(c, resumed), qt.DeepEquals,
						[]progress{{Version: 1, State: "failed", Applied: 2, Total: 3}})

					c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
					c.Assert(revisionProgress(c, resumed), qt.DeepEquals,
						[]progress{{Version: 1, State: "applied", Applied: 3, Total: 3}})
					c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+test.dir+"/l`"), qt.Equals, int64(1))
				})
			}
		})
	}
}

// text reads one Utf8 value.
func text(c *qt.C, conn *dbschema.DatabaseConnection, query string) string {
	c.Helper()
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("query: %s", query))
	return value
}

// A repair that resumes the body runs a data query the way a run does: in one
// transaction with its checkpoint, with no in-flight mark written before it.
// The INSERT below copies the revision row's failing statement, so it reads
// the failure the first run recorded; a mark would have replaced that with the
// INSERT itself.
func TestYDBMigrator_RepairRunsADataQueryWithItsCheckpoint(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_repair"
			dropDirectory(c, conn, dir, "r")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "r") })
			head := "CREATE TABLE `" + dir + "/r` (id Int64 NOT NULL, note Utf8, PRIMARY KEY (id));\n"
			broken := map[string]string{
				"0000000001_r.up.sql":   head + "CREATE TABLE `" + dir + "/bad` (id NoSuchType NOT NULL, PRIMARY KEY (id));\n",
				"0000000001_r.down.sql": "DROP TABLE `" + dir + "/r`;\n",
			}
			fixed := map[string]string{
				"0000000001_r.up.sql": head + "INSERT INTO `" + dir + "/r` " +
					"SELECT 1l AS id, error_stmt AS note FROM `" + dir + "/schema_migrations` WHERE version = 1l;\n",
				"0000000001_r.down.sql": broken["0000000001_r.down.sql"],
			}
			c.Assert(newMigrator(c, conn, broken, migrator.RevisionTableFormatPtah, dir).MigrateUp(c.Context()), qt.IsNotNil)
			failedStatement := text(c, conn, "SELECT error_stmt FROM `"+dir+"/schema_migrations` WHERE version = 1l")

			repaired := newMigrator(c, conn, fixed, migrator.RevisionTableFormatPtah, dir)
			err := repaired.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2})

			c.Assert(err, qt.IsNil)
			c.Assert(failedStatement, qt.Equals, "CREATE TABLE `"+dir+"/bad` (id NoSuchType NOT NULL, PRIMARY KEY (id))")
			c.Assert(text(c, conn, "SELECT note FROM `"+dir+"/r` WHERE id = 1l"), qt.Equals, failedStatement)
			c.Assert(revisionProgress(c, repaired), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 2, Total: 2}})
		})
	}
}

// A repair that resumes the body refuses what a run refuses, before any of the
// resumed statements runs: the CREATE TABLE after the client delimiter, which
// a splitter honoring the directive would run, is not run.
func TestYDBMigrator_RepairRefusesWhatItCannotSplit(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_repair_refused"
			dropDirectory(c, conn, dir, "r", "s")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "r", "s") })
			head := "CREATE TABLE `" + dir + "/r` (id Int64 NOT NULL, PRIMARY KEY (id));\n"
			broken := map[string]string{
				"0000000001_r.up.sql":   head + "CREATE TABLE `" + dir + "/bad` (id NoSuchType NOT NULL, PRIMARY KEY (id));\n",
				"0000000001_r.down.sql": "DROP TABLE `" + dir + "/r`;\n",
			}
			delimited := map[string]string{
				"0000000001_r.up.sql": head + "DELIMITER //\n" +
					"CREATE TABLE `" + dir + "/s` (id Int64 NOT NULL, PRIMARY KEY (id))//\n",
				"0000000001_r.down.sql": broken["0000000001_r.down.sql"],
			}
			c.Assert(newMigrator(c, conn, broken, migrator.RevisionTableFormatPtah, dir).MigrateUp(c.Context()), qt.IsNotNil)

			repaired := newMigrator(c, conn, delimited, migrator.RevisionTableFormatPtah, dir)
			err := repaired.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2})

			c.Assert(err, qt.ErrorMatches,
				`(?s)migration 1 cannot run up on ydb: client delimiter directive in YQL: "DELIMITER //".*`)
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|r"})
			c.Assert(revisionProgress(c, repaired), qt.DeepEquals, []progress{{Version: 1, State: "failed", Applied: 1, Total: 2}})
		})
	}
}

// A block or an action that runs a scheme statement, and a BATCH statement,
// run outside a transaction, as YDB requires of each: inside the transaction a
// data query runs in, YDB refuses the first with `Scheme operations cannot be
// executed inside transaction` and the second with `BATCH operation can be
// executed only in the implicit transaction mode`.
func TestYDBMigrator_RunsBlocksAndBatchStatementsOutsideATransaction(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_blocks"
			dropDirectory(c, conn, dir, "a", "b", "c", "d")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "a", "b", "c", "d") })
			m := newMigrator(c, conn, map[string]string{
				"0000000001_blocks.up.sql": "DO BEGIN\n  CREATE TABLE `" + dir + "/a` (id Int64 NOT NULL, n Int64, PRIMARY KEY (id));\nEND DO;\n" +
					"DEFINE ACTION $make($name) AS\n  CREATE TABLE $name (id Int64 NOT NULL, PRIMARY KEY (id));\nEND DEFINE;\n" +
					"EVALUATE FOR $table IN AsList('" + dir + "/b', '" + dir + "/c') DO $make($table);\n" +
					"UPSERT INTO `" + dir + "/a` (id, n) VALUES (1l, 1l), (2l, 2l), (3l, 3l);\n" +
					"BATCH DELETE FROM `" + dir + "/a` WHERE id = 2l;\n" +
					"BATCH UPDATE `" + dir + "/a` SET n = 0l WHERE id > 0l;\n" +
					"DO $make('" + dir + "/d');\n",
				"0000000001_blocks.down.sql": "DROP TABLE `" + dir + "/d`;\nDROP TABLE `" + dir + "/c`;\nDROP TABLE `" + dir +
					"/b`;\nDROP TABLE `" + dir + "/a`;\n",
			}, migrator.RevisionTableFormatPtah, dir)

			c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

			c.Assert(revisionProgress(c, m), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 6, Total: 6}})
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals,
				[]string{dir + "|a", dir + "|b", dir + "|c", dir + "|d"})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/a` WHERE n = 0l"), qt.Equals, int64(2))
		})
	}
}

// A dry run executes nothing, a data query included: the data query that
// commits with its checkpoint is not run in a transaction of its own when the
// writer only logs.
func TestYDBMigrator_DryRunRunsNoDataQuery(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_dry"
			dropDirectory(c, conn, dir, "q", "r")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "q", "r") })
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE TABLE `"+dir+"/q` (id Int64 NOT NULL, PRIMARY KEY (id))"), qt.IsNil)
			dry := openYDB(c, line)
			dry.SchemaWriter().SetDryRun(true)
			m := newMigrator(c, dry, map[string]string{
				"0000000001_q.up.sql": "INSERT INTO `" + dir + "/q` (id) VALUES (1l);\n" +
					"CREATE TABLE `" + dir + "/r` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
				"0000000001_q.down.sql": "DROP TABLE `" + dir + "/r`;\n",
			}, migrator.RevisionTableFormatPtah, dir)

			c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/q`"), qt.Equals, int64(0))
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|q"})
		})
	}
}
