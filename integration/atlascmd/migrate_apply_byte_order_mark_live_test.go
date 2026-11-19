//go:build integration

package atlas_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// Files that start with a UTF-8 byte order mark apply through `migrate
// apply`, and the `-- atlas:txmode none` behind the mark holds: PostgreSQL
// refuses CREATE INDEX CONCURRENTLY inside a transaction. The pinned
// community binary sends the mark to the server and fails on the first
// file; the mark is the file's encoding, so Ptah does not copy that.
func TestMigrateApplyRunsFilesThatStartWithAByteOrderMarkLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := newCleanGateDatabase(c, dbtarget.URL(t, dbtarget.PostgreSQL), nil)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	files := map[string]string{
		"20260101000000_widgets.sql": "\xEF\xBB\xBFCREATE TABLE widgets (id integer PRIMARY KEY);\n",
		"20260101000001_widgets_index.sql": "\xEF\xBB\xBF-- atlas:txmode none\n\n" +
			"CREATE INDEX CONCURRENTLY widgets_id_idx ON widgets (id);\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)

	applySpelledDirectory(c, dir, dbURL)

	c.Assert(recordedVersions(c, dbURL), qt.DeepEquals, []string{"20260101000000", "20260101000001"})
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var indexes int
	c.Assert(db.QueryRowContext(context.Background(),
		"SELECT count(*) FROM pg_indexes WHERE indexname = 'widgets_id_idx'").Scan(&indexes), qt.IsNil)
	c.Assert(indexes, qt.Equals, 1)
}
