package migrator_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A MySQL-family connection whose URL does not say parseTime=true hands back a
// time column as the server's text, and no driver here does that for a column
// declared as a time. SQLite hands back what a column of another type holds, so
// these tests store the text in a BLOB column for the bytes that driver delivers
// and in a TEXT column for a string, and read it through the public readers. The
// zone the text reads in depends on the URL, and that half needs a server:
// TestMetadataTimesReadTheSameWithoutParseTimeE2E in integration/migrateup.

// storedTimeFixture is a SQLite database one migration has been applied to and
// tagged, which leaves a row in each table that keeps a time.
func storedTimeFixture(c *qt.C) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "stored_time.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "create_notes",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n", "DROP TABLE notes;\n")))
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(m.RecordMigrationTag(ctx, "v1", 1), qt.IsNil)
	return conn, m
}

// storeTimeAs replaces a time column with one of columnType holding text.
func storeTimeAs(c *qt.C, conn *dbschema.DatabaseConnection, table, column, columnType, text string) {
	c.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, column),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, columnType),
		fmt.Sprintf("UPDATE %s SET %s = CAST('%s' AS %s)", table, column, text, columnType),
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// storedTimeShapes are the spellings the text of a time column arrives in. The
// value in `want` is the one the same row reads as under parseTime=true, which
// reads all of these shapes, and a zero date as the zero time.
var storedTimeShapes = []struct {
	name string
	text string
	want string
}{
	{name: "a TIMESTAMP", text: "2026-11-06 21:17:34", want: "2026-11-06T21:17:34Z"},
	{name: "fractional seconds", text: "2026-11-06 21:17:34.123456", want: "2026-11-06T21:17:34.123456Z"},
	{name: "fewer fractional digits", text: "2026-11-06 21:17:34.120", want: "2026-11-06T21:17:34.12Z"},
	{name: "a DATE", text: "2026-11-06", want: "2026-11-06T00:00:00Z"},
	{name: "a zero date", text: "0000-00-00", want: "0001-01-01T00:00:00Z"},
	{name: "a zero TIMESTAMP", text: "0000-00-00 00:00:00", want: "0001-01-01T00:00:00Z"},
	{name: "a zero TIMESTAMP(6)", text: "0000-00-00 00:00:00.000000", want: "0001-01-01T00:00:00Z"},
	{name: "text naming its own zone", text: "2026-11-06T21:17:34+09:00", want: "2026-11-06T21:17:34+09:00"},
}

// storedTimeColumnTypes are the declarations that make the driver deliver the
// text as bytes, as go-sql-driver does, and as a string.
var storedTimeColumnTypes = []string{"BLOB", "TEXT"}

func TestMigrationLog_ReadsATimeStoredAsText(t *testing.T) {
	for _, columnType := range storedTimeColumnTypes {
		for _, shape := range storedTimeShapes {
			t.Run(columnType+"/"+shape.name, func(t *testing.T) {
				c := qt.New(t)
				conn, m := storedTimeFixture(c)
				storeTimeAs(c, conn, "schema_migrations_log", "logged_at", columnType, shape.text)

				attempts, err := m.MigrationLog(context.Background(), 0)

				c.Assert(err, qt.IsNil)
				c.Assert(attempts, qt.HasLen, 1)
				c.Assert(attempts[0].Start.At.Format(time.RFC3339Nano), qt.Equals, shape.want)
				c.Assert(attempts[0].Outcome.At.Format(time.RFC3339Nano), qt.Equals, shape.want)
			})
		}
	}
}

func TestMigrationTags_ReadATimeStoredAsText(t *testing.T) {
	for _, columnType := range storedTimeColumnTypes {
		for _, shape := range storedTimeShapes {
			t.Run(columnType+"/"+shape.name, func(t *testing.T) {
				c := qt.New(t)
				conn, m := storedTimeFixture(c)
				storeTimeAs(c, conn, "ptah_migration_tags", "recorded_at", columnType, shape.text)

				tags, err := m.MigrationTags(context.Background())

				c.Assert(err, qt.IsNil)
				c.Assert(tags, qt.HasLen, 1)
				c.Assert(tags[0].RecordedAt.Format(time.RFC3339Nano), qt.Equals, shape.want)
			})
		}
	}
}

func TestGetRevisions_ReadATimeStoredAsText(t *testing.T) {
	for _, columnType := range storedTimeColumnTypes {
		for _, shape := range storedTimeShapes {
			t.Run(columnType+"/"+shape.name, func(t *testing.T) {
				c := qt.New(t)
				conn, m := storedTimeFixture(c)
				storeTimeAs(c, conn, "schema_migrations", "applied_at", columnType, shape.text)

				revisions, err := m.GetRevisions(context.Background())

				c.Assert(err, qt.IsNil)
				c.Assert(revisions, qt.HasLen, 1)
				c.Assert(revisions[0].AppliedAt.Format(time.RFC3339Nano), qt.Equals, shape.want)
			})
		}
	}
}

// Text that names no moment is refused by name rather than read as the zero
// time, which is what a zero date reads as.
func TestStoredTimeText_FailurePath(t *testing.T) {
	const garbage = "the day before yesterday"

	t.Run("the log", func(t *testing.T) {
		c := qt.New(t)
		conn, m := storedTimeFixture(c)
		storeTimeAs(c, conn, "schema_migrations_log", "logged_at", "BLOB", garbage)

		attempts, err := m.MigrationLog(context.Background(), 0)

		c.Assert(err, qt.ErrorMatches, `read a migration log row: failed to parse logged_at "the day before yesterday"`)
		c.Assert(attempts, qt.IsNil)
	})
	t.Run("the tags", func(t *testing.T) {
		c := qt.New(t)
		conn, m := storedTimeFixture(c)
		storeTimeAs(c, conn, "ptah_migration_tags", "recorded_at", "BLOB", garbage)

		tags, err := m.MigrationTags(context.Background())

		c.Assert(err, qt.ErrorMatches, `read migration tags: failed to parse recorded_at "the day before yesterday"`)
		c.Assert(tags, qt.IsNil)
	})
	t.Run("the revisions", func(t *testing.T) {
		c := qt.New(t)
		conn, m := storedTimeFixture(c)
		storeTimeAs(c, conn, "schema_migrations", "applied_at", "BLOB", garbage)

		revisions, err := m.GetRevisions(context.Background())

		c.Assert(err, qt.ErrorMatches, `(?s).*failed to parse applied_at "the day before yesterday"`)
		c.Assert(revisions, qt.IsNil)
	})
	t.Run("a value of no time shape", func(t *testing.T) {
		c := qt.New(t)
		conn, m := storedTimeFixture(c)
		_, err := conn.ExecContext(context.Background(), "ALTER TABLE schema_migrations_log DROP COLUMN logged_at")
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), "ALTER TABLE schema_migrations_log ADD COLUMN logged_at INTEGER DEFAULT 7")
		c.Assert(err, qt.IsNil)

		attempts, err := m.MigrationLog(context.Background(), 0)

		c.Assert(err, qt.ErrorMatches, `read a migration log row: unsupported logged_at value int64`)
		c.Assert(attempts, qt.IsNil)
	})
}
