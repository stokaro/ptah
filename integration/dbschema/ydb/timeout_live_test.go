//go:build integration

package ydb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// fillRows creates table with rows rows of about 80 bytes each. The rows are
// generated on the server, a batch per query, so a test can build a table
// large enough that a statement over it outlives a short statement timeout.
func fillRows(c *qt.C, conn *dbschema.DatabaseConnection, table string, rows int) {
	c.Helper()
	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"CREATE TABLE `"+table+"` (id Int64 NOT NULL, v Utf8, w Utf8, PRIMARY KEY (id))"), qt.IsNil)
	const batch = 20000
	for start := 0; start < rows; start += batch {
		end := min(start+batch, rows)
		_, err := conn.ExecContext(c.Context(), fmt.Sprintf(
			"UPSERT INTO `%s` (id, v, w) SELECT CAST(x AS Int64) AS id, "+
				"CAST(x AS Utf8) || 'value-padding-padding-padding'u AS v, 'more-padding-more-padding-more-padding'u AS w "+
				"FROM AS_TABLE(ListMap(ListFromRange(%d, %d), ($x) -> (AsStruct($x AS x))))", table, start, end))
		c.Assert(err, qt.IsNil)
	}
}

// revisionOf reads the one revision a test's migrator recorded.
func revisionOf(c *qt.C, m *migrator.Migrator) migrator.MigrationRevision {
	c.Helper()
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	return revisions[0]
}

func statementTimeout(d time.Duration) migrationfile.Timeouts {
	return migrationfile.Timeouts{StatementTimeout: d, HasStatementTimeout: true}
}

// An index build outlives the statement timeout, and YDB keeps building after
// the client stops waiting. The migrator cancels the build: the run fails
// saying nothing was applied, the revision records the query as not run rather
// than as an outcome nobody knows, and the table reads back with no index. The
// same file run again without the timeout builds the index.
func TestYDBMigrator_StatementTimeoutCancelsAnIndexBuild(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_timeout_index"
			dropDirectory(c, conn, dir, "big")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "big") })
			fillRows(c, conn, dir+"/big", 1000000)
			files := map[string]string{
				"0000000001_index.up.sql":   "ALTER TABLE `" + dir + "/big` ADD INDEX big_v GLOBAL SYNC ON (v);\n",
				"0000000001_index.down.sql": "ALTER TABLE `" + dir + "/big` DROP INDEX big_v;\n",
			}

			failed := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir).
				WithDefaultTimeouts(statementTimeout(300 * time.Millisecond)).MigrateUp(c.Context())

			c.Assert(failed, qt.ErrorMatches, `(?s)failed to apply migration 1: .*the statement timeout of 300ms ran out, `+
				`and YDB canceled the build it started on `+dir+`/big, so nothing of the query was applied.*`)
			revision := revisionOf(c, newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir))
			c.Assert(revision.State, qt.Equals, "failed")
			c.Assert(revision.Applied, qt.Equals, 0)
			c.Assert(revision.StatementOutcomeUnknown(), qt.IsFalse)
			c.Assert(indexNamesOf(readScoped(c, conn, []string{dir})), qt.HasLen, 0)

			resumed := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir)
			c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
			c.Assert(revisionProgress(c, resumed), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 1, Total: 1}})
			c.Assert(indexNamesOf(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{"big_v"})
		})
	}
}

// A data query outlives the statement timeout. YDB cancels it, and its
// transaction, which holds the checkpoint too, never commits: the target stays
// empty, the revision records the query as not run, and the same file run
// again without the timeout copies every row once.
func TestYDBMigrator_StatementTimeoutCancelsADataQuery(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_timeout_data"
			dropDirectory(c, conn, dir, "src", "dst")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "src", "dst") })
			fillRows(c, conn, dir+"/src", 300000)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE TABLE `"+dir+"/dst` (id Int64 NOT NULL, v Utf8, w Utf8, PRIMARY KEY (id))"), qt.IsNil)
			files := map[string]string{
				"0000000001_copy.up.sql":   "INSERT INTO `" + dir + "/dst` (id, v, w) SELECT id, v, w FROM `" + dir + "/src`;\n",
				"0000000001_copy.down.sql": "DELETE FROM `" + dir + "/dst`;\n",
			}

			failed := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir).
				WithDefaultTimeouts(statementTimeout(200 * time.Millisecond)).MigrateUp(c.Context())

			c.Assert(failed, qt.ErrorMatches, `(?s)failed to apply migration 1: .*the statement timeout of 200ms ran out, `+
				`and YDB canceled the query, whose transaction never committed, so nothing of the query was applied.*`)
			revision := revisionOf(c, newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir))
			c.Assert(revision.State, qt.Equals, "failed")
			c.Assert(revision.Applied, qt.Equals, 0)
			c.Assert(revision.StatementOutcomeUnknown(), qt.IsFalse)
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/dst`"), qt.Equals, int64(0))

			resumed := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir)
			c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+dir+"/dst`"), qt.Equals, int64(300000))
		})
	}
}

// A schema statement YDB runs as a quick change rather than a build cannot be
// canceled, and keeps running when the client stops waiting. A statement
// timeout that stops waiting for one cannot say whether it was applied, and
// the revision says so, which makes a resume refuse to guess.
func TestYDBMigrator_StatementTimeoutLeavesAQuickSchemaChangeUnknown(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_timeout_unknown"
			dropDirectory(c, conn, dir, "t")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "t") })
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE TABLE `"+dir+"/t` (id Int64 NOT NULL, PRIMARY KEY (id))"), qt.IsNil)
			files := map[string]string{
				"0000000001_column.up.sql":   "ALTER TABLE `" + dir + "/t` ADD COLUMN note Utf8;\n",
				"0000000001_column.down.sql": "ALTER TABLE `" + dir + "/t` DROP COLUMN note;\n",
			}

			failed := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir).
				WithDefaultTimeouts(statementTimeout(time.Millisecond)).MigrateUp(c.Context())
			settleSchemaChange(c, conn, dir+"/t", "note")

			c.Assert(failed, qt.ErrorMatches, `(?s)failed to apply migration 1: .*the statement timeout of 1ms ran out, `+
				`and YDB keeps running a schema statement after the client stops waiting, so whether the query was applied `+
				`is unknown.*`)
			c.Assert(revisionOf(c, newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir)).StatementOutcomeUnknown(),
				qt.IsTrue)
		})
	}
}

// settleSchemaChange waits until column exists in table, or until a schema
// change sent before its client gave up has had time to land, so the test's
// cleanup does not race it.
func settleSchemaChange(c *qt.C, conn *dbschema.DatabaseConnection, table, column string) {
	c.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var present int64
		if err := conn.QueryRowContext(c.Context(), "SELECT COUNT("+column+") FROM `"+table+"`").Scan(&present); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// YDB has no lock wait, so a lock timeout is refused before the run writes
// anything: no revision is recorded and no statement runs.
func TestYDBMigrator_RefusesALockTimeout(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const dir = "ptah_ydb_mig_timeout_lock"
			dropDirectory(c, conn, dir, "t")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "t") })
			files := map[string]string{
				"0000000001_t.up.sql":   "CREATE TABLE `" + dir + "/t` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
				"0000000001_t.down.sql": "DROP TABLE `" + dir + "/t`;\n",
			}
			m := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir)

			refused := m.WithDefaultTimeouts(migrationfile.Timeouts{LockTimeout: time.Second, HasLockTimeout: true}).
				MigrateUp(context.Background())

			c.Assert(refused, qt.ErrorMatches, `(?s).*migration 1 declares timeouts: migration timeouts are not supported for `+
				`dialect "ydb": this target has no lock wait Ptah bounds around a migration \(capability `+
				`migration_lock_timeout\), so a lock timeout cannot be honored; remove it`)
			c.Assert(revisionProgress(c, m), qt.HasLen, 0)
			c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.HasLen, 0)
		})
	}
}
