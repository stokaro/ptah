//go:build integration

package providercli_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// This SQL-backed control exercises the same checksum policy as the native
// provider, with deterministic timestamps that admit several equivalent proofs.
func TestHistoryChecksumDistinctCutoffsCanProveTheSameHistory(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "cutoffs.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	files := providerChecksumFiles()
	c.Assert(providerChecksumRunner(c, conn, files).MigrateUp(c.Context()), qt.IsNil)
	for index, version := range []int64{1, 3, 4} {
		_, err := conn.ExecContext(c.Context(), `UPDATE main._revisions SET applied_at = ? WHERE version = ?`,
			time.Date(2024, 1, index+1, 0, 0, 0, 0, time.UTC), version)
		c.Assert(err, qt.IsNil)
	}
	files["2_two.sql"] = `CREATE TABLE "chain_two" ("id" INTEGER);`
	recovering := providerChecksumRunner(c, conn, files)
	migrations := recovering.MigrationProvider().Migrations()
	c.Assert(migrations, qt.HasLen, 4)
	c.Assert(migrations[1].Version, qt.Equals, int64(2))
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), files["2_two.sql"]), qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `INSERT INTO main._revisions
 (version, description, applied_at, state, applied, total, execution_time_ms, checksum)
 VALUES (?, ?, ?, 'applied', 1, 1, 0, ?)`, 2, "Two", time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
		strings.TrimPrefix(migrations[1].Checksum, "h1:"))
	c.Assert(err, qt.IsNil)
	reconcile, err := recovering.VerifyAppliedChecksums(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(reconcile, qt.IsTrue)
	c.Assert(recovering.MigrateUp(c.Context()), qt.IsNil)
	reconcile, err = recovering.VerifyAppliedChecksums(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(reconcile, qt.IsFalse)
	rows, err := recovering.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 4)
	c.Assert(rows[2].Checksum, qt.Equals, strings.TrimPrefix(migrations[2].Checksum, "h1:"))
	c.Assert(rows[3].Checksum, qt.Equals, strings.TrimPrefix(migrations[3].Checksum, "h1:"))
}
