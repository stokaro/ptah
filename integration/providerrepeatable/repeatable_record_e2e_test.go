//go:build integration

package providerrepeatable_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrator"
)

// These tests drive the shipped binary against the fixture provider, whose
// engine is a SQLite file and whose SQL is table DDL only. The directory's
// versioned migration creates a table; its repeatable migration creates a table
// named after the content. A body that ran twice fails on the table its first
// run created, so a run that succeeds says the body did not run again, and the
// tables and the provider's record are read back from the file.

const recordVersioned = `CREATE TABLE "users" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT NOT NULL);
`

// recordBody is the repeatable body that creates the table run_<tag>.
func recordBody(tag string) string {
	return `CREATE TABLE "run_` + tag + `" ("id" INTEGER NOT NULL PRIMARY KEY);
`
}

// recordFailing creates a table and then fails on the one the versioned
// migration created, so a transaction that rolled the body back leaves no
// run_failed table.
const recordFailing = `CREATE TABLE "run_failed" ("id" INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE "users" ("id" INTEGER NOT NULL PRIMARY KEY);
`

// providerRefusal is the refusal of a directory with a repeatable migration
// by a provider, or an engine, that does not list the record's feature.
const providerRefusal = `error getting migration status: this directory holds repeatable migrations, which this run cannot execute on fixture: ` +
	`unsupported feature: provider example.org/fixture keeps no record of repeatable migrations for engine fixture, ` +
	`because it does not list sessions.repeatable-record (contract 25.44); install a provider release that lists it`

// recordTarget is a fixture database, the provider configuration that reaches
// it, and a migration directory.
type recordTarget struct {
	manifest, address, database, dir string
}

// newRecordTarget builds fixture command with environment, and a directory
// that holds the versioned migration and the repeatable view.
func newRecordTarget(c *qt.C, command string, environment ...string) *recordTarget {
	c.Helper()
	binary := testutils.BuildFixtureProviderRelease(c, command, testutils.FixtureRelease{})
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	spec := providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture", Environment: environment}
	manifest, err := json.Marshal(providerconfig.Configuration{
		FormatVersion: providerconfig.FormatVersion,
		Providers:     []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: "fixture"}}}},
	})
	c.Assert(err, qt.IsNil)
	root := c.TempDir()
	r := &recordTarget{manifest: filepath.Join(root, "providers.json"), database: filepath.Join(root, "database.db"), dir: filepath.Join(root, "migrations")}
	r.address = (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(r.database), "/")}).String()
	c.Assert(os.WriteFile(r.manifest, manifest, 0o600), qt.IsNil)
	c.Assert(os.Mkdir(r.dir, 0o700), qt.IsNil)
	r.write(c, "0000000001_users.up.sql", recordVersioned)
	r.write(c, "0000000001_users.down.sql", `DROP TABLE "users";`+"\n")
	r.write(c, "R__user_names.sql", recordBody("v1"))
	return r
}

func (r *recordTarget) write(c *qt.C, name, body string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(r.dir, name), []byte(body), 0o600), qt.IsNil)
}

// ptah runs verb of `ptah migrations` against the target, in a directory of
// its own.
func (r *recordTarget) ptah(c *qt.C, verb string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"migrations", verb, "--provider-config", r.manifest, "--db-url", r.address}, args...)...)
}

// up runs `migrations up --json` and reads the run document.
func (r *recordTarget) up(c *qt.C, extra ...string) (clirun.Result, migrator.RunResult) {
	c.Helper()
	result := r.ptah(c, "up", append([]string{"--migrations-dir", r.dir, "--json"}, extra...)...)
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document
}

// status runs `migrations status --json` and reads the document.
func (r *recordTarget) status(c *qt.C) migrator.MigrationStatus {
	c.Helper()
	result := r.ptah(c, "status", "--migrations-dir", r.dir, "--json")
	var status migrator.MigrationStatus
	c.Assert(json.Unmarshal([]byte(result.Stdout), &status), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return status
}

// query reads one text column of every row query returns, from the fixture
// file itself.
func (r *recordTarget) query(c *qt.C, query string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(r.database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	rows, err := conn.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	values := make([]string, 0)
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// dropByHand drops table outside a migration, from the fixture file itself.
func (r *recordTarget) dropByHand(c *qt.C, table string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(r.database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), `DROP TABLE "`+table+`"`)
	c.Assert(err, qt.IsNil)
}

// record reads the provider's record: each row's identity, the digest that
// last applied and the content it keeps.
func (r *recordTarget) record(c *qt.C) []string {
	c.Helper()
	return r.query(c, `SELECT "identity" || ' ' || "checksum" || ' ' || CAST(COALESCE("body", '') AS TEXT) `+
		`FROM "schema_migrations_repeatable" ORDER BY "identity"`)
}

// tables lists the tables of the fixture file the migrations created.
func (r *recordTarget) tables(c *qt.C) []string {
	c.Helper()
	return r.query(c, `SELECT name FROM sqlite_master WHERE type = 'table' AND (name = 'users' OR name LIKE 'run\_%' ESCAPE '\') ORDER BY name`)
}

// attempts reads the attempts `migrations log --json` printed, newest first,
// as the version, the repeatable identity and the outcome of each.
func attempts(c *qt.C, stdout string) []string {
	c.Helper()
	var records []struct {
		Version    int64  `json:"version"`
		Repeatable string `json:"repeatable"`
		Outcome    string `json:"outcome"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &records), qt.IsNil, qt.Commentf("%s", stdout))
	described := make([]string, 0, len(records))
	for _, record := range records {
		described = append(described, strconv.FormatInt(record.Version, 10)+" "+record.Repeatable+" "+record.Outcome)
	}
	return described
}

func h1(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "h1:" + base64.StdEncoding.EncodeToString(sum[:])
}

func TestProviderRepeatableRecordE2E_HappyPath(t *testing.T) {
	t.Run("a repeatable migration applies once in its transaction, and an unchanged rerun runs nothing", func(t *testing.T) {
		c := qt.New(t)
		r := newRecordTarget(c, "repeatable-record")

		result, document := r.up(c)
		rerun, unchanged := r.up(c)

		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
		c.Assert(document.Applied, qt.DeepEquals, []int64{1})
		c.Assert(document.AppliedRepeatables, qt.DeepEquals, []string{"user_names"})
		c.Assert(rerun.ExitCode, qt.Equals, 0, qt.Commentf("%s", rerun.Stderr))
		c.Assert(unchanged.Outcome, qt.Equals, migrator.RunOutcomeUpToDate)
		c.Assert(unchanged.PlannedRepeatables, qt.HasLen, 0)
		c.Assert(r.tables(c), qt.DeepEquals, []string{"run_v1", "users"})
		c.Assert(r.record(c), qt.DeepEquals, []string{"user_names " + h1(recordBody("v1")) + " " + recordBody("v1")})
		records := r.status(c).Repeatables
		c.Assert(records, qt.HasLen, 1)
		c.Assert(records[0].State, qt.Equals, migrator.RepeatableStateApplied)
		c.Assert(records[0].AppliedChecksum, qt.Equals, h1(recordBody("v1")))
		c.Assert(records[0].NoTransaction, qt.IsFalse)
	})

	t.Run("a changed file runs again, and the log names it", func(t *testing.T) {
		c := qt.New(t)
		r := newRecordTarget(c, "repeatable-record")
		first, _ := r.up(c)
		r.write(c, "R__user_names.sql", recordBody("v2"))

		result, document := r.up(c)
		log := r.ptah(c, "log", "--json")

		c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("%s", first.Stderr))
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.PlannedRepeatables, qt.DeepEquals, []migrator.RepeatableWork{{
			Identity: "user_names", Path: "R__user_names.sql", Checksum: h1(recordBody("v2")),
			AppliedChecksum: h1(recordBody("v1")), Reason: migrator.RepeatableReasonChanged,
		}})
		c.Assert(r.tables(c), qt.DeepEquals, []string{"run_v1", "run_v2", "users"})
		c.Assert(r.record(c), qt.DeepEquals, []string{"user_names " + h1(recordBody("v2")) + " " + recordBody("v2")})
		c.Assert(log.ExitCode, qt.Equals, 0, qt.Commentf("%s", log.Stderr))
		c.Assert(attempts(c, log.Stdout), qt.DeepEquals, []string{"0 user_names applied", "0 user_names applied", "1  applied"})
	})

	t.Run("a lost acknowledgment of the claim is settled by the row, and the body runs once", func(t *testing.T) {
		c := qt.New(t)
		r := newRecordTarget(c, "repeatable-record")
		clean := r.address
		r.address += "?fault=lost-repeatable-claim"

		result, document := r.up(c)
		r.address = clean

		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.AppliedRepeatables, qt.DeepEquals, []string{"user_names"})
		c.Assert(r.tables(c), qt.DeepEquals, []string{"run_v1", "users"})
		c.Assert(r.record(c), qt.DeepEquals, []string{"user_names " + h1(recordBody("v1")) + " " + recordBody("v1")})
	})

	t.Run("a failed body is retried once it is fixed", func(t *testing.T) {
		c := qt.New(t)
		r := newRecordTarget(c, "repeatable-record")
		r.write(c, "R__user_names.sql", recordFailing)
		failed, _ := r.up(c)
		r.write(c, "R__user_names.sql", recordBody("v1"))

		result, document := r.up(c, "--allow-dirty")

		c.Assert(failed.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.PlannedRepeatables[0].Reason, qt.Equals, migrator.RepeatableReasonRetry)
		c.Assert(r.tables(c), qt.DeepEquals, []string{"run_v1", "users"})
		c.Assert(r.status(c).Repeatables[0].State, qt.Equals, migrator.RepeatableStateApplied)
	})

	t.Run("a table dropped by hand comes back once repair removes the row", func(t *testing.T) {
		c := qt.New(t)
		r := newRecordTarget(c, "repeatable-record")
		first, _ := r.up(c)
		r.dropByHand(c, "run_v1")

		kept, unchanged := r.up(c)
		dropped := r.tables(c)
		repair := r.ptah(c, "repair", "--migrations-dir", r.dir, "--repeatable", "user_names", "--force")
		emptied := r.record(c)
		result, document := r.up(c)

		c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("%s", first.Stderr))
		c.Assert(kept.ExitCode, qt.Equals, 0, qt.Commentf("%s", kept.Stderr))
		c.Assert(unchanged.Outcome, qt.Equals, migrator.RunOutcomeUpToDate)
		c.Assert(dropped, qt.DeepEquals, []string{"users"})
		c.Assert(repair.ExitCode, qt.Equals, 0, qt.Commentf("%s", repair.Stderr))
		c.Assert(emptied, qt.HasLen, 0)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.PlannedRepeatables[0].Reason, qt.Equals, migrator.RepeatableReasonNew)
		c.Assert(r.tables(c), qt.DeepEquals, []string{"run_v1", "users"})
	})
}

func TestProviderRepeatableRecordE2E_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, command string
		environment   []string
	}{
		{name: "a provider that does not list the record", command: "native-migrations"},
		{name: "an engine that does not list the record", command: "repeatable-record", environment: []string{"FIXTURE_REPEATABLE_ENGINE=off"}},
	} {
		t.Run(test.name+" refuses the directory before anything changes", func(t *testing.T) {
			c := qt.New(t)
			r := newRecordTarget(c, test.command, test.environment...)

			result, document := r.up(c)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(document.Error.Message, qt.Equals, providerRefusal)
			c.Assert(document.Applied, qt.HasLen, 0)
			c.Assert(r.query(c, `SELECT name FROM sqlite_master WHERE type = 'table'`), qt.HasLen, 0)
		})
	}

	t.Run("a body that fails in its transaction leaves no trace and blocks the next run", func(t *testing.T) {
		c := qt.New(t)
		r := newRecordTarget(c, "repeatable-record")
		r.write(c, "R__user_names.sql", recordFailing)

		result, document := r.up(c)
		r.write(c, "R__user_names.sql", recordBody("v1"))
		blocked, refusal := r.up(c)

		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomePartial)
		c.Assert(document.Applied, qt.DeepEquals, []int64{1})
		c.Assert(document.Error.Message, qt.Matches, `(?s)repeatable migration user_names failed and took no effect: .*users.*`)
		c.Assert(blocked.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(refusal.Error.Message, qt.Matches, `(?s)repeatable migrations did not finish: user_names .*failed: .*`)
		c.Assert(r.tables(c), qt.DeepEquals, []string{"users"})
		record := r.status(c).Repeatables[0]
		c.Assert(record.State, qt.Equals, migrator.RepeatableStateFailed)
		c.Assert(record.NoTransaction, qt.IsFalse)
	})

	t.Run("a file longer than the record keeps is refused before anything changes", func(t *testing.T) {
		c := qt.New(t)
		r := newRecordTarget(c, "repeatable-record")
		body := recordBody("v1") + "-- " + strings.Repeat("x", 2<<20) + "\n"
		r.write(c, "R__user_names.sql", body)

		result, document := r.up(c)

		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Error.Message, qt.Equals, "repeatable migration R__user_names.sql is "+strconv.Itoa(len(body))+
			" bytes, and the record of repeatable migrations on this engine keeps at most 2097152; split the file into "+
			"smaller repeatable migrations")
		c.Assert(document.Applied, qt.HasLen, 0)
		c.Assert(r.tables(c), qt.HasLen, 0)
	})
}
