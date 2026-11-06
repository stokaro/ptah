//go:build integration

package migrateup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// metadataTimeDatabase creates a database of the test's own on a MySQL-family
// server and returns its address. The tag table has a fixed name, so these tests
// do not write it into the database the rest of the contour shares.
func metadataTimeDatabase(c *qt.C, admin dbtarget.Engine) string {
	c.Helper()
	address := dbtarget.URL(c, admin)
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_metadata_time_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP DATABASE "+name)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(conn)
	})
	renamed, err := atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	return renamed
}

// parseTimeAddresses returns address carrying parameters, once with no parseTime
// and once with parseTime=true, and holds each to that through the driver's own
// reading of it. The variable CI sets may carry parameters of its own, and a
// parseTime among them would leave these tests reading what no defect can reach.
func parseTimeAddresses(c *qt.C, address string, parameters url.Values) (without, with string) {
	c.Helper()
	base, rawQuery, _ := strings.Cut(address, "?")
	query, err := url.ParseQuery(rawQuery)
	c.Assert(err, qt.IsNil)
	query.Del("parseTime")
	maps.Copy(query, parameters)
	without = strings.TrimSuffix(base+"?"+query.Encode(), "?")
	query.Set("parseTime", "true")
	with = base + "?" + query.Encode()
	c.Assert(driverParsesTime(c, without), qt.IsFalse)
	c.Assert(driverParsesTime(c, with), qt.IsTrue)
	return without, with
}

func driverParsesTime(c *qt.C, address string) bool {
	c.Helper()
	parsed, err := atlasurl.ParseMySQLURL(address)
	c.Assert(err, qt.IsNil)
	config, err := mysqldriver.ParseDSN(parsed.DSN())
	c.Assert(err, qt.IsNil)
	return config.ParseTime
}

// logAttempt is the part of `ptah migrations log --json` these tests read, in the
// shape it is published.
type logAttempt struct {
	StartedAt  *string `json:"started_at"`
	FinishedAt *string `json:"finished_at"`
	Version    int64   `json:"version"`
	Outcome    string  `json:"outcome"`
	StartLost  bool    `json:"start_lost"`
}

// readLogDocument runs `ptah migrations log --json`.
func readLogDocument(c *qt.C, args ...string) []logAttempt {
	c.Helper()
	stdout := ptah(c, slices.Concat([]string{"migrations", "log", "--json"}, args)...)
	var attempts []logAttempt
	c.Assert(json.Unmarshal([]byte(stdout), &attempts), qt.IsNil, qt.Commentf("%s", stdout))
	return attempts
}

// ptah runs the shipped binary and holds it to exiting clean.
func ptah(c *qt.C, args ...string) string {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
	c.Assert(result.ExitCode, qt.Equals, 0,
		qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, result.Stdout, result.Stderr))
	return result.Stdout
}

// A MySQL or MariaDB URL that does not say parseTime=true hands back a time column
// as the server's text, with no zone in it. Every reader of Ptah's own tables takes
// that URL, and a row reads the same as it does with the parameter on: the second
// URL is the control, whose values the driver parses itself. The driver writes in
// the zone `loc` names, so that is the zone the text has to be read in, and a read
// in any other zone lands outside the run.
func TestMetadataTimesReadTheSameWithoutParseTimeE2E(t *testing.T) {
	engines := []struct {
		name  string
		admin dbtarget.Engine
	}{
		{name: "mysql", admin: dbtarget.MySQLAdmin},
		{name: "mariadb", admin: dbtarget.MariaDBAdmin},
	}
	zones := []struct {
		name       string
		parameters url.Values
		offset     string
	}{
		{name: "no loc", offset: "Z"},
		{name: "a loc", parameters: url.Values{"loc": {"Asia/Tokyo"}}, offset: "+09:00"},
	}
	for _, engine := range engines {
		for _, zone := range zones {
			t.Run(engine.name+"/"+zone.name, func(t *testing.T) {
				c := qt.New(t)
				database := metadataTimeDatabase(c, engine.admin)
				without, with := parseTimeAddresses(c, database, zone.parameters)
				before := time.Now().Add(-2 * time.Minute)
				run := newLostLogRun(c, without, "", "")

				result, _ := run.up(c)

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
				tagged := ptah(c, "migrations", "tag", "v1", "--db-url", without, "--migrations-table", run.table)
				c.Assert(tagged, qt.Contains, "as v1")
				after := time.Now().Add(2 * time.Minute)

				// Every reader, byte for byte, in the table and in the document.
				for _, verb := range [][]string{
					{"migrations", "log", "--migrations-table", run.table},
					{"migrations", "log", "--json", "--migrations-table", run.table},
					{"migrations", "tag", "--migrations-table", run.table},
					{"migrations", "status", "--json", "--migrations-table", run.table, "--migrations-dir", run.dir},
				} {
					got := ptah(c, slices.Concat(verb, []string{"--db-url", without})...)
					control := ptah(c, slices.Concat(verb, []string{"--db-url", with})...)
					c.Assert(got, qt.Equals, control, qt.Commentf("%v", verb))
				}

				// The document says what the server stored, in the URL's zone.
				var stored string
				c.Assert(run.conn.QueryRowContext(c.Context(),
					"SELECT DATE_FORMAT(logged_at, '%Y-%m-%dT%H:%i:%s') FROM "+run.table+"_log "+
						"WHERE version = 1 AND state = 'started'",
				).Scan(&stored), qt.IsNil)
				attempts := readLogDocument(c, "--db-url", without, "--migrations-table", run.table)
				c.Assert(attempts, qt.HasLen, 2)
				c.Assert(attempts[1].Version, qt.Equals, int64(1))
				c.Assert(attempts[1].StartedAt, qt.IsNotNil)
				c.Assert(*attempts[1].StartedAt, qt.Equals, stored+zone.offset)

				// And what it says is the moment the run happened.
				started, err := time.Parse(time.RFC3339, *attempts[1].StartedAt)
				c.Assert(err, qt.IsNil)
				c.Assert(started.After(before) && started.Before(after), qt.IsTrue,
					qt.Commentf("started_at %s outside the run %s to %s", started, before, after))
				var status struct {
					Migrations []struct {
						AppliedAt time.Time `json:"applied_at"`
					} `json:"migrations"`
				}
				document := ptah(c, "migrations", "status", "--json", "--db-url", without,
					"--migrations-table", run.table, "--migrations-dir", run.dir)
				c.Assert(json.Unmarshal([]byte(document), &status), qt.IsNil, qt.Commentf("%s", document))
				c.Assert(status.Migrations, qt.HasLen, 2)
				c.Assert(status.Migrations[0].AppliedAt.After(before) && status.Migrations[0].AppliedAt.Before(after), qt.IsTrue,
					qt.Commentf("applied_at %s outside the run %s to %s", status.Migrations[0].AppliedAt, before, after))
			})
		}
	}
}

// A check constraint the first migration adds refuses every later start and no
// outcome, so the log holds the second migration's outcome and not its start. NOT
// VALID leaves the first migration's own start in place, which is the control: the
// whole attempt reads as it always did, beside the one that has no start.
func TestMigrationsLogShowsAnOutcomeWhoseStartWasLostE2E(t *testing.T) {
	c := qt.New(t)
	run := newLostLogRun(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"ALTER TABLE {log} ADD CONSTRAINT {log}_no_start CHECK (state <> 'started') NOT VALID;\n", "")

	result, document := run.up(c)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
	c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(run.logRows(c), qt.DeepEquals, []string{"1:up:started", "1:up:applied", "2:up:applied"})

	table := []string{"migrations", "log", "--db-url", run.url, "--migrations-table", run.table}
	c.Assert(ptah(c, table...), qt.Matches,
		`STARTED +VERSION +OPERATION +OUTCOME +ACTOR +SOURCE\n`+
			`- +2 +up +applied +\S+ +\S+\n`+
			`\S+Z +1 +up +applied +\S+ +\S+\n`)
	attempts := readLogDocument(c, "--db-url", run.url, "--migrations-table", run.table)
	c.Assert(attempts, qt.HasLen, 2)
	c.Assert(attempts[0].Version, qt.Equals, int64(2))
	c.Assert(attempts[0].Outcome, qt.Equals, "applied")
	c.Assert(attempts[0].StartLost, qt.IsTrue)
	c.Assert(attempts[0].StartedAt, qt.IsNil)
	c.Assert(attempts[0].FinishedAt, qt.IsNotNil)
	c.Assert(attempts[1].Version, qt.Equals, int64(1))
	c.Assert(attempts[1].StartLost, qt.IsFalse)
	c.Assert(attempts[1].StartedAt, qt.IsNotNil)

	// With only the outcome left, the log has still recorded an operation.
	_, err := run.conn.ExecContext(c.Context(), "DELETE FROM "+run.table+"_log WHERE version = 1")
	c.Assert(err, qt.IsNil)

	c.Assert(ptah(c, table...), qt.Matches,
		`STARTED +VERSION +OPERATION +OUTCOME +ACTOR +SOURCE\n- +2 +up +applied +\S+ +\S+\n`)
}
