//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// statusTimingZone is the zone the applying process runs in: five hours and
// forty-five minutes from UTC, so a row written in local time reads back far
// outside the run on a runner whose own zone is UTC.
const statusTimingZone = "Asia/Kathmandu"

// TestMigrationsStatusReportsWhenEachMigrationRanE2E applies one migration of
// two from a process in another zone than UTC and reads `ptah migrations
// status --json` on every server engine. The applied migration carries when
// its revision row was written, in UTC and within the run, and how long it
// ran; the pending one carries neither (stokaro/ptah#4886). PostgreSQL stores
// the native row's time without a zone, and Ptah wrote the local wall clock
// into it until it wrote UTC.
func TestMigrationsStatusReportsWhenEachMigrationRanE2E(t *testing.T) {
	c := qt.New(t)
	zone, err := time.LoadLocation(statusTimingZone)
	c.Assert(err, qt.IsNil, qt.Commentf("the zone database must hold %s, or the run below proves nothing", statusTimingZone))
	_, offset := time.Now().In(zone).Zone()
	c.Assert(offset, qt.Equals, 5*3600+45*60)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, c.Context(), e2eRepoRoot(t), binary)
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			_, url := scratch.database(c, "nstiming")
			assertStatusTimesTheRun(c, binary, url)
		})
	}
	t.Run("PostgreSQL", func(t *testing.T) {
		c := qt.New(t)
		assertStatusTimesTheRun(c, binary, postgresScratchDevURL(c, "ptah_nstiming"))
	})
}

func assertStatusTimesTheRun(c *qt.C, binary, url string) {
	c.Helper()
	dir := nativeStatusMigrationDir(c)
	before := time.Now().Add(-2 * time.Minute)
	up := exec.CommandContext(c.Context(), binary, "migrations", "up", "--db-url", url, "--migrations-dir", dir)
	up.Env = append(os.Environ(), "TZ="+statusTimingZone)
	out, err := up.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	after := time.Now().Add(2 * time.Minute)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_later.up.sql"),
		[]byte("CREATE TABLE status_later (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_later.down.sql"),
		[]byte("DROP TABLE status_later;\n"), 0o600), qt.IsNil)

	status, err := runPtahNativeWithError("migrations", "status", "--db-url", url, "--migrations-dir", dir, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", status))
	var document struct {
		Migrations []struct {
			Version     int64   `json:"version"`
			State       string  `json:"state"`
			AppliedAt   *string `json:"applied_at"`
			ExecutionMs *int64  `json:"execution_time_ms"`
		} `json:"migrations"`
	}
	c.Assert(json.Unmarshal([]byte(status), &document), qt.IsNil, qt.Commentf("%s", status))
	c.Assert(document.Migrations, qt.HasLen, 2)
	applied, pending := document.Migrations[0], document.Migrations[1]
	c.Assert(applied.State, qt.Equals, "applied")
	c.Assert(applied.AppliedAt, qt.IsNotNil)
	at, err := time.Parse(time.RFC3339Nano, *applied.AppliedAt)
	c.Assert(err, qt.IsNil)
	c.Assert(at.Location(), qt.Equals, time.UTC, qt.Commentf("applied_at %s", *applied.AppliedAt))
	c.Assert(at.After(before) && at.Before(after), qt.IsTrue,
		qt.Commentf("applied_at %s outside the run %s to %s", at, before, after))
	c.Assert(applied.ExecutionMs, qt.IsNotNil)
	c.Assert(*applied.ExecutionMs >= 0, qt.IsTrue)
	c.Assert(pending.State, qt.Equals, "pending")
	c.Assert(pending.AppliedAt, qt.IsNil)
	c.Assert(pending.ExecutionMs, qt.IsNil)
}
