//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// A migration may set a parameter for its own transaction, with SET LOCAL or
// set_config(name, value, true), and PostgreSQL ends the setting with that
// transaction. Replay runs every migration of a directory on one session, so
// what these tests pin is the pair of facts the replay guard relies on: the
// setting is in effect for the rest of its migration, and it is gone before
// the next one. Each migration asserts what it reads with a CHECK constraint,
// so a replay that ran the file outside a transaction, or let the setting
// through to the next file, fails the insert.
//
// The same directory is then applied, and the rows read back, so replay and
// apply are held to one answer.

// setLocalProbeFiles is an Atlas directory. Migration 4 opts out of the
// transaction, where PostgreSQL ignores a SET LOCAL with a warning and ends a
// set_config at the statement, on replay and on apply alike.
var setLocalProbeFiles = map[string]string{
	"1_set_local.sql": `SET LOCAL lock_timeout = '5s';
CREATE TABLE probe_set_local (v text CHECK (v = '5s'));
INSERT INTO probe_set_local SELECT current_setting('lock_timeout');
`,
	"2_set_config.sql": `SELECT set_config('lock_timeout', '7s', true);
CREATE TABLE probe_set_config (v text CHECK (v = '7s'));
INSERT INTO probe_set_config SELECT current_setting('lock_timeout');
`,
	"3_next.sql": `CREATE TABLE probe_next (v text CHECK (v = '0'));
INSERT INTO probe_next SELECT current_setting('lock_timeout');
`,
	"4_no_transaction.sql": `-- atlas:txmode none

SET LOCAL lock_timeout = '5s';
SELECT set_config('statement_timeout', '9s', true);
CREATE TABLE probe_no_transaction (v text CHECK (v = '0|0'));
INSERT INTO probe_no_transaction SELECT current_setting('lock_timeout') || '|' || current_setting('statement_timeout');
`,
	"5_after.sql": `CREATE TABLE probe_after (v text CHECK (v = '0|0'));
INSERT INTO probe_after SELECT current_setting('lock_timeout') || '|' || current_setting('statement_timeout');
`,
}

// setLocalProbeReadings is what the probe tables hold after the directory
// applies, one reading per migration.
var setLocalProbeReadings = map[string]string{
	"probe_set_local":      "5s",
	"probe_set_config":     "7s",
	"probe_next":           "0",
	"probe_no_transaction": "0|0",
	"probe_after":          "0|0",
}

func writeReplayProbeDir(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)
	return dir
}

// newSetLocalScratchDatabase creates a database of its own on the PostgreSQL
// test server and returns its URL and a connection to it. A replay empties the
// dev database it is given, so it is never handed the shared one.
func newSetLocalScratchDatabase(c *qt.C) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_set_local_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), `CREATE DATABASE "`+name+`"`)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	dbURL := parsed.String()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	return dbURL, conn
}

func readSetLocalProbes(c *qt.C, conn *dbschema.DatabaseConnection) map[string]string {
	c.Helper()
	readings := make(map[string]string, len(setLocalProbeReadings))
	for table := range setLocalProbeReadings {
		var value string
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT v FROM "+table).Scan(&value), qt.IsNil)
		readings[table] = value
	}
	return readings
}

func TestReplayConfinesATransactionScopedSettingToItsMigration(t *testing.T) {
	c := qt.New(t)
	dir := writeReplayProbeDir(c, setLocalProbeFiles)
	compatDev, _ := newSetLocalScratchDatabase(c)
	nativeDev, _ := newSetLocalScratchDatabase(c)

	out, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", compatDev)
	c.Assert(err, qt.IsNil, qt.Commentf("migrate validate output:\n%s", out))

	native := runPtahNative(c, "migrations", "validate", "--dir", dir, "--dir-format", "atlas", "--dev-url", nativeDev)
	c.Assert(native, qt.Contains, "migration SQL validated on dev database")
}

// TestApplyConfinesATransactionScopedSettingToItsMigration applies the
// directory the replay above accepted, through both surfaces, and reads each
// migration's reading back.
func TestApplyConfinesATransactionScopedSettingToItsMigration(t *testing.T) {
	c := qt.New(t)
	dir := writeReplayProbeDir(c, setLocalProbeFiles)
	compatURL, compatConn := newSetLocalScratchDatabase(c)
	nativeURL, nativeConn := newSetLocalScratchDatabase(c)

	out, err := runCompatVerb("migrate", "apply", "--dir", "file://"+dir, "--url", compatURL)
	c.Assert(err, qt.IsNil, qt.Commentf("migrate apply output:\n%s", out))
	c.Assert(readSetLocalProbes(c, compatConn), qt.DeepEquals, setLocalProbeReadings)

	runPtahNative(c,
		"migrations", "up",
		"--db-url", nativeURL,
		"--migrations-dir", dir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
	)
	c.Assert(readSetLocalProbes(c, nativeConn), qt.DeepEquals, setLocalProbeReadings)
}

// TestReplayRefusesASessionSetting is the control: a plain SET outlives its
// transaction and would reach every later migration of the replay.
func TestReplayRefusesASessionSetting(t *testing.T) {
	c := qt.New(t)
	dir := writeReplayProbeDir(c, map[string]string{
		"1_session.sql": "SET lock_timeout = '5s';\nCREATE TABLE probe_session (v text);\n",
	})
	devURL, _ := newSetLocalScratchDatabase(c)

	_, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", devURL)

	c.Assert(err, qt.ErrorMatches, `(?s).*replay migration 1 on dev database: .*`+
		`postgres migration replay rejects SET session or transaction state because its effects cannot be confined to the disposable database realm.*`)
}
