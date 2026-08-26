package seeder_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/seeder"
)

const (
	seedPath    = "010_countries.dev.sql"
	firstSeed   = "INSERT INTO countries (code) VALUES ('cz');"
	editedSeed  = "INSERT INTO countries (code) VALUES ('sk');"
	countryRows = "SELECT COUNT(*) FROM countries"
)

// openSeedTarget returns a SQLite connection holding the one table the seeds
// below write to.
func openSeedTarget(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "seeds.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(
		c.Context(),
		"CREATE TABLE countries (code TEXT NOT NULL)",
	)
	c.Assert(err, qt.IsNil)
	return conn
}

func seedsWith(sql string) fstest.MapFS {
	return fstest.MapFS{seedPath: {Data: []byte(sql)}}
}

func devOptions() seeder.Options {
	return seeder.Options{Env: "dev"}
}

func countCountries(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), countryRows).Scan(&count), qt.IsNil)
	return count
}

// TestApplyRefusesASeedEditedAfterItWasApplied is the defect the checksum
// column existed for and did not answer: the value was written on every run and
// never read, so the second run consulted the path alone and reported the
// edited file as already applied.
func TestApplyRefusesASeedEditedAfterItWasApplied(t *testing.T) {
	c := qt.New(t)
	conn := openSeedTarget(c)

	first, err := seeder.Apply(c.Context(), conn, seedsWith(firstSeed), devOptions())
	c.Assert(err, qt.IsNil)
	c.Assert(first.Applied, qt.HasLen, 1)

	_, err = seeder.Apply(c.Context(), conn, seedsWith(editedSeed), devOptions())
	c.Assert(err, qt.IsNotNil)

	var mismatch *seeder.ChecksumMismatchError
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Path, qt.Equals, seedPath)
	c.Assert(mismatch.Stored, qt.Not(qt.Equals), mismatch.Computed)
	c.Assert(err.Error(), qt.Contains, "pass --force to re-apply")

	// The refusal is a refusal: the edited statement did not run.
	c.Assert(countCountries(c, conn), qt.Equals, 1)
}

// TestApplySkipsAnUnchangedSeed is the control for the refusal above. The same
// second run over the same bytes is a no-op, so what the mismatch reports is the
// edit and not the fact that the seed ran before.
func TestApplySkipsAnUnchangedSeed(t *testing.T) {
	c := qt.New(t)
	conn := openSeedTarget(c)

	_, err := seeder.Apply(c.Context(), conn, seedsWith(firstSeed), devOptions())
	c.Assert(err, qt.IsNil)

	second, err := seeder.Apply(c.Context(), conn, seedsWith(firstSeed), devOptions())
	c.Assert(err, qt.IsNil)
	c.Assert(second.Applied, qt.HasLen, 0)
	c.Assert(second.Skipped, qt.HasLen, 1)
	c.Assert(countCountries(c, conn), qt.Equals, 1)
}

// TestApplyForceReAppliesAnEditedSeed is the documented way past the refusal,
// and the second half of the contract: --force runs the edited file and records
// the new checksum, so the run after it is a clean skip rather than a second
// refusal.
func TestApplyForceReAppliesAnEditedSeed(t *testing.T) {
	c := qt.New(t)
	conn := openSeedTarget(c)

	_, err := seeder.Apply(c.Context(), conn, seedsWith(firstSeed), devOptions())
	c.Assert(err, qt.IsNil)

	forced := devOptions()
	forced.Force = true
	result, err := seeder.Apply(c.Context(), conn, seedsWith(editedSeed), forced)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.HasLen, 1)
	c.Assert(countCountries(c, conn), qt.Equals, 2)

	settled, err := seeder.Apply(c.Context(), conn, seedsWith(editedSeed), devOptions())
	c.Assert(err, qt.IsNil)
	c.Assert(settled.Skipped, qt.HasLen, 1)
	c.Assert(countCountries(c, conn), qt.Equals, 2)
}
