package dbverify_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite" // registers the SQLite driver for database/sql

	"ptah.run/dbschema"
	"ptah.run/internal/cli/dbverify"
	"ptah.run/internal/cli/internal/exitcode"
)

// fixture writes a SQLite database with one table and returns its URL. SQLite
// is the one engine the verb can be driven against without a server, and it is
// a real connection: the outcomes asserted below are what the engine answered.
// tier is the second user's tier: nil writes a NULL, which is what the
// backfill assertion below is about. A "" here would be a non-null empty
// string and the assertion would hold, so the test would pass whatever the
// verb did.
func fixture(c *qt.C, tier any) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "app.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	_, err = conn.ExecContext(c.Context(), `CREATE TABLE users (id INTEGER PRIMARY KEY, tier TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(),
		`INSERT INTO users (id, tier) VALUES (1, 'pro'), (2, ?), (3, 'free')`, tier)
	c.Assert(err, qt.IsNil)
	return "sqlite://" + path
}

func writeChecks(c *qt.C, directory, name, body string) string {
	c.Helper()
	c.Assert(os.MkdirAll(directory, 0o750), qt.IsNil)
	path := filepath.Join(directory, name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

func runVerify(c *qt.C, args ...string) (stdout string, err error) {
	c.Helper()
	cmd := dbverify.NewVerifyCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), err
}

const tierBackfill = `-- +ptah check name="every user has a tier" ` +
	`assert="SELECT COUNT(*) = 0 FROM users WHERE tier IS NULL"` + "\n"

func TestVerify_HappyPath(t *testing.T) {
	t.Run("a satisfied requirement exits zero", func(t *testing.T) {
		c := qt.New(t)
		url := fixture(c, "free")
		checks := writeChecks(c, filepath.Join(c.TempDir(), "checks"), "010_backfill.sql", tierBackfill)

		stdout, err := runVerify(c, "--db-url", url, "--checks", checks)

		c.Assert(err, qt.IsNil)
		c.Assert(stdout, qt.Contains, "Verdict: verified (1 verified, 0 failed, 0 errored of 1)")
	})

	// A directory is read in sorted file-name order so the report is a list two
	// runs can be diffed against each other, and a file that is not .sql is not
	// parsed as one: a README beside the checks is not a requirement.
	t.Run("a directory contributes its sql files in name order", func(t *testing.T) {
		c := qt.New(t)
		url := fixture(c, "free")
		directory := filepath.Join(c.TempDir(), "checks")
		writeChecks(c, directory, "020_second.sql",
			`-- +ptah check name="second" assert="SELECT 1"`+"\n")
		writeChecks(c, directory, "010_first.sql",
			`-- +ptah check name="first" assert="SELECT 1"`+"\n")
		writeChecks(c, directory, "README.md",
			`-- +ptah check name="ignored" assert="SELECT 0"`+"\n")

		stdout, err := runVerify(c, "--db-url", url, "--checks", directory, "--format", "json")

		c.Assert(err, qt.IsNil)
		var report struct {
			Assertions int `json:"assertions"`
			Results    []struct {
				Name   string `json:"name"`
				Source string `json:"source"`
			} `json:"results"`
		}
		c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
		c.Assert(report.Assertions, qt.Equals, 2)
		c.Assert(report.Results[0].Name, qt.Equals, "first")
		c.Assert(report.Results[1].Name, qt.Equals, "second")
		c.Assert(filepath.Base(report.Results[0].Source), qt.Equals, "010_first.sql")
		c.Assert(filepath.Base(report.Results[1].Source), qt.Equals, "020_second.sql")
	})
}

func TestVerify_FailurePath(t *testing.T) {
	// An assertion that ran and did not hold is the expected negative result
	// this verb exists to produce, so it exits 1 beside drift and pending
	// migrations rather than 2 with the usage mistakes.
	t.Run("a violated requirement exits one", func(t *testing.T) {
		c := qt.New(t)
		url := fixture(c, nil)
		checks := writeChecks(c, filepath.Join(c.TempDir(), "checks"), "010_backfill.sql", tierBackfill)

		stdout, err := runVerify(c, "--db-url", url, "--checks", checks)

		c.Assert(err, qt.ErrorMatches, `verification failed: 1 of 1 assertions did not hold`)
		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		c.Assert(stdout, qt.Contains, "Verdict: failed")
	})

	// An assertion that could not run is a fault in the input or the server,
	// not a finding about the release, so it takes the command-error code.
	t.Run("an assertion that could not run exits two", func(t *testing.T) {
		c := qt.New(t)
		url := fixture(c, "free")
		checks := writeChecks(c, filepath.Join(c.TempDir(), "checks"), "010_missing.sql",
			`-- +ptah check name="missing table" assert="SELECT COUNT(*) FROM no_such_table"`+"\n")

		stdout, err := runVerify(c, "--db-url", url, "--checks", checks)

		c.Assert(err, qt.ErrorMatches, `verification could not complete: 1 of 1 assertions could not run`)
		c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
		c.Assert(stdout, qt.Contains, "Verdict: errored")
	})

	// An empty checks path must not read as a clean release: a gate that
	// exited zero here would pass a pipeline whose requirements someone
	// deleted.
	t.Run("a checks path with no directives exits one", func(t *testing.T) {
		c := qt.New(t)
		url := fixture(c, "free")
		directory := filepath.Join(c.TempDir(), "checks")
		writeChecks(c, directory, "010_empty.sql", "-- nothing to assert here\n")

		stdout, err := runVerify(c, "--db-url", url, "--checks", directory)

		c.Assert(err, qt.ErrorMatches,
			"nothing was verified: the checks path holds no `-- \\+ptah check` directives")
		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		c.Assert(stdout, qt.Contains, "Verdict: not verified")
	})

	t.Run("a malformed directive is reported with its file", func(t *testing.T) {
		c := qt.New(t)
		url := fixture(c, "free")
		checks := writeChecks(c, filepath.Join(c.TempDir(), "checks"), "010_bad.sql",
			`-- +ptah check name="no assert"`+"\n")

		_, err := runVerify(c, "--db-url", url, "--checks", checks)

		c.Assert(err, qt.ErrorMatches,
			`checks file .*010_bad\.sql: \+ptah check requires a non-empty assert predicate`)
	})

	t.Run("a missing checks path is reported", func(t *testing.T) {
		c := qt.New(t)
		url := fixture(c, "free")

		_, err := runVerify(c, "--db-url", url, "--checks", filepath.Join(c.TempDir(), "absent"))

		c.Assert(err, qt.ErrorMatches, `read checks from .*absent: .*`)
	})

	t.Run("no checks flag", func(t *testing.T) {
		c := qt.New(t)

		_, err := runVerify(c, "--db-url", fixture(c, "free"))

		c.Assert(err, qt.ErrorMatches, `--checks is required`)
	})

	t.Run("no database url", func(t *testing.T) {
		c := qt.New(t)

		_, err := runVerify(c, "--checks", filepath.Join(c.TempDir(), "checks"))

		c.Assert(err, qt.ErrorMatches, `database URL is required`)
	})

	t.Run("unsupported format", func(t *testing.T) {
		c := qt.New(t)

		_, err := runVerify(c, "--db-url", fixture(c, "free"), "--checks", c.TempDir(), "--format", "yaml")

		c.Assert(err, qt.ErrorMatches, `unsupported --format "yaml" \(want text or json\)`)
	})
}
