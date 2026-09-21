package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// verifyFixture opens a SQLite database holding one table whose rows are the
// subject of the assertions below: three users, one of them without a tier.
func verifyFixture(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "verify.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	_, err = conn.ExecContext(c.Context(), `CREATE TABLE users (id INTEGER PRIMARY KEY, tier TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `INSERT INTO users (id, tier) VALUES (1, 'pro'), (2, NULL), (3, 'free')`)
	c.Assert(err, qt.IsNil)
	return conn
}

func TestVerifyChecks_HappyPath(t *testing.T) {
	// Every requirement gets an answer, including the ones written after a
	// failure. A migration stops at its first unsatisfied precondition because
	// it has a body to protect; a verification run has none, and an operator
	// reading a release report wants the whole list.
	t.Run("every assertion is evaluated and reported in order", func(t *testing.T) {
		c := qt.New(t)
		conn := verifyFixture(c)

		report, err := migrator.VerifyChecks(c.Context(), conn, []migrator.Check{
			{Name: "rows exist", Assert: "SELECT COUNT(*) = 3 FROM users"},
			{Name: "every user has a tier", Assert: "SELECT COUNT(*) = 0 FROM users WHERE tier IS NULL"},
			{Name: "no table was dropped", Assert: "SELECT COUNT(*) >= 0 FROM users"},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(report.Results, qt.HasLen, 3)
		c.Assert(report.Results[0].Name, qt.Equals, "rows exist")
		c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusVerified)
		c.Assert(report.Results[1].Status, qt.Equals, migrator.VerifyStatusFailed)
		c.Assert(report.Results[1].Err, qt.IsNil)
		c.Assert(report.Results[2].Status, qt.Equals, migrator.VerifyStatusVerified)
		c.Assert(report.Verdict(), qt.Equals, migrator.VerifyVerdictFailed)
		c.Assert(report.Verified(), qt.IsFalse)
	})

	t.Run("a run whose assertions all hold is verified", func(t *testing.T) {
		c := qt.New(t)
		conn := verifyFixture(c)

		report, err := migrator.VerifyChecks(c.Context(), conn, []migrator.Check{
			{Name: "rows exist", Assert: "SELECT COUNT(*) = 3 FROM users"},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(report.Verdict(), qt.Equals, migrator.VerifyVerdictVerified)
		c.Assert(report.Verified(), qt.IsTrue)
		c.Assert(report.Count(migrator.VerifyStatusVerified), qt.Equals, 1)
	})

	// Nothing asked is nothing established, so the empty run is not a pass. A
	// caller that read it as one would gate a release on a checks file someone
	// emptied.
	t.Run("a run with no assertions is not verified", func(t *testing.T) {
		c := qt.New(t)
		conn := verifyFixture(c)

		report, err := migrator.VerifyChecks(c.Context(), conn, nil)

		c.Assert(err, qt.IsNil)
		c.Assert(report.Results, qt.HasLen, 0)
		c.Assert(report.Verdict(), qt.Equals, migrator.VerifyVerdictNotVerified)
		c.Assert(report.Verified(), qt.IsFalse)
	})

	// An assertion that could not run outranks one that failed: the run cannot
	// claim the failures are the only violations when a requirement was never
	// evaluated.
	t.Run("an assertion that could not run outranks one that failed", func(t *testing.T) {
		c := qt.New(t)
		conn := verifyFixture(c)

		report, err := migrator.VerifyChecks(c.Context(), conn, []migrator.Check{
			{Name: "every user has a tier", Assert: "SELECT COUNT(*) = 0 FROM users WHERE tier IS NULL"},
			{Name: "missing table", Assert: "SELECT COUNT(*) FROM no_such_table"},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusFailed)
		c.Assert(report.Results[1].Status, qt.Equals, migrator.VerifyStatusErrored)
		c.Assert(report.Results[1].Err, qt.IsNotNil)
		c.Assert(report.Verdict(), qt.Equals, migrator.VerifyVerdictErrored)
		c.Assert(report.Count(migrator.VerifyStatusFailed), qt.Equals, 1)
		c.Assert(report.Count(migrator.VerifyStatusErrored), qt.Equals, 1)
	})

	// The static rule that decides whether an assertion may be sent is the one
	// a pre-migration check uses, so a write reaches the database from neither
	// path. Here it is reported rather than raised, because a verification run
	// answers for every requirement it was given.
	t.Run("a write-shaped assertion is errored, not run", func(t *testing.T) {
		c := qt.New(t)
		conn := verifyFixture(c)

		report, err := migrator.VerifyChecks(c.Context(), conn, []migrator.Check{
			{Name: "writes a row", Assert: "INSERT INTO users (id) VALUES (99)"},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusErrored)
		c.Assert(report.Results[0].Err, qt.ErrorMatches, `check assertion must be a read-only SELECT statement`)

		var rows int
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM users").Scan(&rows), qt.IsNil)
		c.Assert(rows, qt.Equals, 3)
	})
}

func TestVerifyChecks_FailurePath(t *testing.T) {
	t.Run("no connection", func(t *testing.T) {
		c := qt.New(t)

		report, err := migrator.VerifyChecks(c.Context(), nil, []migrator.Check{
			{Name: "anything", Assert: "SELECT 1"},
		})

		c.Assert(err, qt.ErrorMatches, `verification requires an open database connection`)
		c.Assert(report.Results, qt.HasLen, 0)
	})
}
