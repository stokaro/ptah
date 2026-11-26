//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A history a dump tool wrote, followed by one Ptah generated, replays on a
// dev database with no trust. pg_dump and ysql_dump set session parameters
// before their first object and comment on the schema and the extension they
// create; Ptah comments on the schema and the extension its migration creates.
// The replay guard accepts each, since none of them reaches past the dev
// database or changes how the guard reads the statements after it, and every
// cleanup drops what the comments describe.
//
// The dumps are in testdata/pgdump, each with the statements it was taken
// from. Fixture 023 is where the schema, the extension and their comments
// come from; the entities below keep that part and leave out the roles,
// routines and policies the guard refuses without trust.

// pgDumpEntitiesBase declares a schema, an extension and the public schema,
// each with a comment, and objects in public, as fixture 023 does. The
// sequence names no cache, which YugabyteDB raises to 100.
const pgDumpEntitiesBase = `package entities

//ptah:schema:schema name="public" comment="Fixture public schema"
type SchemaMarker struct{}

//ptah:schema:schema name="app" comment="Fixture application schema"
type AppSchemaMarker struct{}

//ptah:schema:extension name="pg_trgm" if_not_exists="true" comment="Fixture extension"
type ExtensionsMarker struct{}

//ptah:schema:table name="users"
//ptah:schema:constraint name="users_email_check" type="CHECK" check="email <> ''" comment="Email must not be empty"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//ptah:schema:field name="status" type="ENUM" enum="active,disabled" default="active"
	Status string

	//ptah:schema:index name="idx_users_email" fields="email"
	_ int
}

//ptah:schema:sequence name="fixture_order_seq" as="bigint" start="1000" increment="1" comment="Fixture standalone sequence"
type OrderSeqMarker struct{}

//ptah:schema:domain name="fixture_email" type="TEXT" not_null="true" check="VALUE ~ '@'" comment="Fixture domain"
type EmailDomainMarker struct{}

//ptah:schema:composite name="fixture_address" fields="street:TEXT,city:TEXT,zip:VARCHAR(10)" comment="Fixture composite type"
type AddressCompositeMarker struct{}

//ptah:schema:view name="active_users" body="SELECT id, email FROM users" comment="Active users view"
type ActiveUsersView struct{}
`

// pgDumpEntitiesNext adds a table in the schema app.
const pgDumpEntitiesNext = pgDumpEntitiesBase + `
//ptah:schema:table name="audit_log" schema="app"
type AuditLog struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="note" type="TEXT"
	Note string
}
`

// pgDumpFirstObject starts the entry of the first object in each dump, so
// what comes before it is the header that sets session parameters.
const pgDumpFirstObject = "--\n-- Name: reports; Type: SCHEMA"

// pgDumpReplayCreated counts the schemas and the extensions the history
// creates, which a replay's cleanup drops from the dev database.
const pgDumpReplayCreated = `SELECT (SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname IN ('app', 'reports'))
	+ (SELECT count(*) FROM pg_catalog.pg_extension WHERE extname IN ('citext', 'pg_trgm'))`

// pgDumpEngines are the servers whose dump tool wrote a dump in
// testdata/pgdump.
var pgDumpEngines = []struct {
	name   string
	engine dbtarget.Engine
	dump   string
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL, dump: "postgres-18.sql"},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB, dump: "yugabytedb-2026.1.sql"},
}

// readPgDump returns the dump named file in testdata/pgdump.
func readPgDump(c *qt.C, file string) string {
	c.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "pgdump", file))
	c.Assert(err, qt.IsNil)
	return string(data)
}

// pgDumpHeader is the header of the dump named file: the statements it sends
// before its first object.
func pgDumpHeader(c *qt.C, file string) string {
	c.Helper()
	header, _, found := strings.Cut(readPgDump(c, file), pgDumpFirstObject)
	c.Assert(found, qt.IsTrue, qt.Commentf("%s holds no %q", file, pgDumpFirstObject))
	return header
}

// newPgDumpHistory is an empty migration directory and a new target, shadow
// and dev database on engine.
func newPgDumpHistory(c *qt.C, engine dbtarget.Engine) (postgresHistory, string) {
	c.Helper()
	h := postgresHistory{
		root:   c.TempDir(),
		target: newHistoryReplayDatabase(c, engine, "target"),
		shadow: newHistoryReplayDatabase(c, engine, "shadow"),
	}
	h.dir = filepath.Join(h.root, "migrations")
	c.Assert(os.MkdirAll(h.dir, 0o750), qt.IsNil)
	return h, newHistoryReplayDatabase(c, engine, "dev")
}

// writePgDumpEntities writes source as the only entity file of the directory
// name under the history's root, and returns the directory.
func writePgDumpEntities(c *qt.C, h postgresHistory, name, source string) string {
	c.Helper()
	dir := filepath.Join(h.root, name)
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "entities.go"), []byte(source), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	return dir
}

// writePgDumpMigration writes up and down as migration version of the
// history, named name, and hashes the directory.
func writePgDumpMigration(c *qt.C, h postgresHistory, version int, name, up, down string) {
	c.Helper()
	stem := filepath.Join(h.dir, fmt.Sprintf("%010d_%s", version, name))
	c.Assert(os.WriteFile(stem+".up.sql", []byte(up), 0o600), qt.IsNil)     // #nosec G703 -- the path is c.TempDir() plus a constant name
	c.Assert(os.WriteFile(stem+".down.sql", []byte(down), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	pgDumpRun(c, h, "migrations", "hash", "--dir", h.dir)
}

// takeGeneratedPgDumpMigration removes the one migration generate wrote under
// name and returns its up and down bodies. Generate names a migration by the
// time it ran, so the test writes it back under a version of its own, which
// keeps the dump after it.
func takeGeneratedPgDumpMigration(c *qt.C, h postgresHistory, name string) (up, down string) {
	c.Helper()
	ups, err := filepath.Glob(filepath.Join(h.dir, "*_"+name+".up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
	stem := strings.TrimSuffix(ups[0], ".up.sql")
	upBody, err := os.ReadFile(stem + ".up.sql")
	c.Assert(err, qt.IsNil)
	downBody, err := os.ReadFile(stem + ".down.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(os.Remove(stem+".up.sql"), qt.IsNil)
	c.Assert(os.Remove(stem+".down.sql"), qt.IsNil)
	return string(upBody), string(downBody)
}

// pgDumpRun runs ptah in the history's root with no PTAH_ variable, and
// asserts that it succeeded.
func pgDumpRun(c *qt.C, h postgresHistory, args ...string) string {
	c.Helper()
	got := h.run(c, nil, args...)
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("ptah %s\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), got.Stdout, got.Stderr))
	return got.Stdout + got.Stderr
}

// pgDumpQuery runs query, which reads one value, on the database at rawURL.
func pgDumpQuery[T any](c *qt.C, rawURL, query string, args ...any) T {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, rawURL))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var value T
	c.Assert(db.QueryRowContext(c.Context(), query, args...).Scan(&value), qt.IsNil)
	return value
}

// applyPgDumpHistory generates a migration from base on the history's empty
// target, with generateArgs, and applies it and then the dump named dump
// after it. The generated migration carries a comment on the extension and on
// the schema it creates.
func applyPgDumpHistory(c *qt.C, h postgresHistory, base, dump string, generateArgs ...string) {
	c.Helper()
	pgDumpRun(c, h, append([]string{"migrations", "generate", "--db-url", h.target, "--migrations-dir", h.dir,
		"--root-dir", base, "--name", "fixture"}, generateArgs...)...)
	up, down := takeGeneratedPgDumpMigration(c, h, "fixture")
	c.Assert(up, qt.Contains, `COMMENT ON EXTENSION "pg_trgm" IS 'Fixture extension';`)
	c.Assert(up, qt.Contains, `COMMENT ON SCHEMA "app" IS 'Fixture application schema';`)
	writePgDumpMigration(c, h, 1, "fixture", up, down)
	writePgDumpMigration(c, h, 2, "pg_dump", readPgDump(c, dump),
		"DROP TABLE reports.daily;\nDROP EXTENSION citext;\nDROP SCHEMA reports;\n")
	pgDumpRun(c, h, "migrations", "up", "--db-url", h.target, "--migrations-dir", h.dir)
}

// TestReplayPgDumpHistoryE2E_HappyPath generates a migration from the
// entities above with a shadow database, which replays the comments it
// carries, applies it and pg_dump's dump after it, and replays the history
// through migrations validate, migrations drift and a second migrations
// generate, none of them trusted. The dev and the shadow database end as they
// started.
func TestReplayPgDumpHistoryE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.PostgreSQL)
	base := writePgDumpEntities(c, h, "base", pgDumpEntitiesBase)
	next := writePgDumpEntities(c, h, "next", pgDumpEntitiesNext)
	applyPgDumpHistory(c, h, base, "postgres-18.sql", "--shadow-db", h.shadow)

	validated := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)
	drift := pgDumpRun(c, h, "migrations", "drift", "--db-url", h.target, "--migrations-dir", h.dir,
		"--shadow-db", h.shadow)
	pgDumpRun(c, h, "migrations", "generate", "--db-url", h.target, "--migrations-dir", h.dir,
		"--root-dir", next, "--shadow-db", h.shadow, "--name", "audit", "--include", "app.audit_log")
	audit, _ := takeGeneratedPgDumpMigration(c, h, "audit")

	c.Assert(validated, qt.Contains, "migration SQL validated on dev database")
	c.Assert(drift, qt.Contains, "No drift")
	c.Assert(audit, qt.Contains, `CREATE TABLE "app"."audit_log"`)
	c.Assert(pgDumpQuery[int](c, h.target, pgDumpReplayCreated), qt.Equals, 4)
	c.Assert(pgDumpQuery[int](c, dev, pgDumpReplayCreated), qt.Equals, 0)
	c.Assert(pgDumpQuery[int](c, h.shadow, pgDumpReplayCreated), qt.Equals, 0)
}

// TestReplayPgDumpHistoryE2E_HappyPathYugabyteDB is the test above on
// YugabyteDB with ysql_dump's dump, replayed through migrations validate and
// migrations drift. The migration is generated without a shadow database: on
// YugabyteDB 2026.1.2 the cleanup that follows generate's up, down and up
// rehearsal on the shadow refused DROP TYPE with SQLSTATE 2BP01 in one run of
// five, which is not the replay guard's to answer.
func TestReplayPgDumpHistoryE2E_HappyPathYugabyteDB(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.YugabyteDB)
	applyPgDumpHistory(c, h, writePgDumpEntities(c, h, "base", pgDumpEntitiesBase), "yugabytedb-2026.1.sql")

	validated := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)
	drift := pgDumpRun(c, h, "migrations", "drift", "--db-url", h.target, "--migrations-dir", h.dir,
		"--shadow-db", h.shadow)

	c.Assert(validated, qt.Contains, "migration SQL validated on dev database")
	c.Assert(drift, qt.Contains, "No drift")
	c.Assert(pgDumpQuery[int](c, h.target, pgDumpReplayCreated), qt.Equals, 4)
	c.Assert(pgDumpQuery[int](c, dev, pgDumpReplayCreated), qt.Equals, 0)
	c.Assert(pgDumpQuery[int](c, h.shadow, pgDumpReplayCreated), qt.Equals, 0)
}

// pgDumpCockroachBody is the part of the dumps CockroachDB runs: it has no
// citext extension to create, and refuses a comment on an extension as
// unimplemented.
const pgDumpCockroachBody = `CREATE SCHEMA reports;
COMMENT ON SCHEMA reports IS 'Dumped reports schema';
CREATE TABLE reports.daily (
    day date NOT NULL PRIMARY KEY,
    total integer DEFAULT 0 NOT NULL,
    note text DEFAULT 'it''s a \ backslash'::text
);
`

// TestReplayPgDumpHeaderE2E_HappyPathCockroachDB replays pg_dump's header, on
// CockroachDB, before a schema the history creates and comments on, through
// migrations validate and migrations drift. CockroachDB accepts every
// statement of the header.
func TestReplayPgDumpHeaderE2E_HappyPathCockroachDB(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.CockroachDB)
	writePgDumpMigration(c, h, 1, "pg_dump", pgDumpHeader(c, "postgres-18.sql")+pgDumpCockroachBody,
		"DROP TABLE reports.daily;\nDROP SCHEMA reports;\n")
	pgDumpRun(c, h, "migrations", "up", "--db-url", h.target, "--migrations-dir", h.dir)

	validated := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)
	drift := pgDumpRun(c, h, "migrations", "drift", "--db-url", h.target, "--migrations-dir", h.dir,
		"--shadow-db", h.shadow)

	c.Assert(validated, qt.Contains, "migration SQL validated on dev database")
	c.Assert(drift, qt.Contains, "No drift")
	c.Assert(pgDumpQuery[int](c, h.target, pgDumpReplayCreated), qt.Equals, 1)
	c.Assert(pgDumpQuery[int](c, dev, pgDumpReplayCreated), qt.Equals, 0)
}

// pgDumpRefused runs migrations validate on a history whose one migration is
// up, against dev, and returns what it wrote. The replay refuses the
// migration, which exits 2.
func pgDumpRefused(c *qt.C, h postgresHistory, dev, up string) string {
	c.Helper()
	writePgDumpMigration(c, h, 1, "refused", up, "SELECT 1;\n")
	got := h.run(c, nil, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)
	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	return got.Stdout + got.Stderr
}

// TestReplayPgDumpHeaderE2E_FailurePath is the control for the header: after
// it, a setting that changes where an unqualified name lands or who the rest
// runs as is still refused, on every engine, with no trust.
func TestReplayPgDumpHeaderE2E_FailurePath(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
		dump   string
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL, dump: "postgres-18.sql"},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB, dump: "yugabytedb-2026.1.sql"},
		{name: "CockroachDB", engine: dbtarget.CockroachDB, dump: "postgres-18.sql"},
	}
	settings := []struct {
		statement string
		wantErr   string
	}{
		{statement: "SET search_path = other;\n", wantErr: "rejects SET search_path because"},
		{statement: "SET ROLE ptah_dump_owner;\n", wantErr: "rejects SET ROLE because"},
		{statement: "SET session_authorization = ptah_dump_owner;\n", wantErr: "rejects SET session or transaction state because"},
	}
	for _, engine := range engines {
		for _, setting := range settings {
			t.Run(engine.name+"/"+strings.TrimSpace(setting.statement), func(t *testing.T) {
				c := qt.New(t)
				h, dev := newPgDumpHistory(c, engine.engine)

				got := pgDumpRefused(c, h, dev, pgDumpHeader(c, engine.dump)+setting.statement)

				c.Assert(got, qt.Contains, setting.wantErr)
				c.Assert(got, qt.Contains, "SQL: "+strings.TrimSuffix(setting.statement, ";\n"))
			})
		}
	}
}

// pgDumpEscapes are settings under which the server reads a later statement's
// bytes by other rules than the guard, which reads the SELECT after each as
// one statement. Each body is a format whose verb is a role name: on a server
// that runs the setting, the SELECT runs a CREATE ROLE. PostgreSQL 18 and
// YugabyteDB run both settings; PostgreSQL 19 refuses
// standard_conforming_strings off itself, and the guard refuses it first.
var pgDumpEscapes = []struct {
	name    string
	body    string
	wantErr string
}{
	{
		name:    "backslash escapes",
		body:    "SET LOCAL standard_conforming_strings = off;\nSELECT 'x\\' AS a, ' ; CREATE ROLE %s; -- ';\n",
		wantErr: "rejects SET standard_conforming_strings because",
	},
	{
		name:    "GBK",
		body:    "SET LOCAL client_encoding = 'GBK';\nSELECT E'\x81\\' ; CREATE ROLE %s; -- ';\n",
		wantErr: "rejects SET client_encoding because",
	},
}

// TestReplayLexingSettingE2E_FailurePath pins the refusal of the settings
// above, and reads the server's roles back: the role the SELECT would create
// does not exist.
func TestReplayLexingSettingE2E_FailurePath(t *testing.T) {
	for _, engine := range pgDumpEngines {
		for _, escape := range pgDumpEscapes {
			t.Run(engine.name+"/"+escape.name, func(t *testing.T) {
				c := qt.New(t)
				h, dev := newPgDumpHistory(c, engine.engine)
				role := fmt.Sprintf("ptah_dump_escape_%d", time.Now().UnixNano())
				c.Cleanup(func() { dropPgDumpRole(c, engine.engine, role) })

				got := pgDumpRefused(c, h, dev, fmt.Sprintf(escape.body, role))

				c.Assert(got, qt.Contains, escape.wantErr)
				c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.Equals, 0)
			})
		}
	}
}

// dropPgDumpRole drops role from engine's server, where a replay that ran an
// escape created it.
func dropPgDumpRole(c *qt.C, engine dbtarget.Engine, role string) {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbtarget.URL(c, engine)))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
	c.Check(err, qt.IsNil)
}

// pgDumpExtensionComment reads the comment on the extension pg_trgm.
const pgDumpExtensionComment = `SELECT coalesce(pg_catalog.obj_description(oid, 'pg_extension'), '')
	FROM pg_catalog.pg_extension WHERE extname = 'pg_trgm'`

// TestReplayExtensionCommentE2E_FailurePath is the control for the comments
// the happy path accepts. On a dev database that held pg_trgm when the replay
// claimed it, a comment on pg_trgm outlives the run, since the cleanup keeps
// the extension; the replay refuses it, and the comment reads as before.
func TestReplayExtensionCommentE2E_FailurePath(t *testing.T) {
	for _, engine := range pgDumpEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			compatDriftExec(c, dev, "CREATE EXTENSION pg_trgm")
			before := pgDumpQuery[string](c, dev, pgDumpExtensionComment)

			got := pgDumpRefused(c, h, dev, "COMMENT ON EXTENSION pg_trgm IS 'changed by a migration';\n")

			c.Assert(got, qt.Contains, "rejects COMMENT ON global metadata because")
			c.Assert(pgDumpQuery[string](c, dev, pgDumpExtensionComment), qt.Equals, before)
		})
	}
}

// withPgDumpParameter is rawURL with the query parameter key added, set to
// value. A space in value is written as %20: the URL Ptah reads hands a plus
// sign to the server as it is, and PostgreSQL then reads no parameter name in
// options.
func withPgDumpParameter(c *qt.C, rawURL, key, value string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.RawQuery = strings.TrimPrefix(parsed.RawQuery+"&"+key+"="+strings.ReplaceAll(url.QueryEscape(value), "+", "%20"), "&")
	return parsed.String()
}

// TestReplayDevSessionE2E_FailurePath is the control for a session that
// starts reading statements by other rules than the guard: the dev URL sets
// them, no migration does, and the replay refuses the session before it runs
// the header. PostgreSQL 19 refuses a session that starts with
// standard_conforming_strings off itself, with SQLSTATE 0A000, before Ptah
// reads it, so that row runs on YugabyteDB, which accepts the value.
func TestReplayDevSessionE2E_FailurePath(t *testing.T) {
	sessions := []struct {
		name       string
		engine     dbtarget.Engine
		dump       string
		key, value string
		wantErr    string
	}{
		{name: "PostgreSQL/GBK", engine: dbtarget.PostgreSQL, dump: "postgres-18.sql",
			key: "client_encoding", value: "GBK",
			wantErr: `client_encoding "GBK" and standard_conforming_strings "on"`},
		{name: "YugabyteDB/GBK", engine: dbtarget.YugabyteDB, dump: "yugabytedb-2026.1.sql",
			key: "client_encoding", value: "GBK",
			wantErr: `client_encoding "GBK" and standard_conforming_strings "on"`},
		{name: "YugabyteDB/backslash escapes", engine: dbtarget.YugabyteDB, dump: "yugabytedb-2026.1.sql",
			key: "options", value: "-c standard_conforming_strings=off",
			wantErr: `client_encoding "UTF8" and standard_conforming_strings "off"`},
	}
	for _, session := range sessions {
		t.Run(session.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, session.engine)

			got := pgDumpRefused(c, h, withPgDumpParameter(c, dev, session.key, session.value), pgDumpHeader(c, session.dump))

			c.Assert(got, qt.Contains, "the dev session reads statements with "+session.wantErr)
		})
	}
}

// TestReplayExtensionCommentE2E_FailurePathPinnedSchema is the control for a
// dev URL that pins search_path to one schema. The reset of such a claim is
// scoped to that schema and does not drop an extension the claim did not
// find, so the replay refuses a comment even on an extension it creates.
// Without the pin, the same two statements replay, as the happy path shows.
func TestReplayExtensionCommentE2E_FailurePathPinnedSchema(t *testing.T) {
	for _, engine := range pgDumpEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)

			got := pgDumpRefused(c, h, withPgDumpParameter(c, dev, "search_path", "public"),
				"CREATE EXTENSION IF NOT EXISTS \"pg_trgm\";\nCOMMENT ON EXTENSION \"pg_trgm\" IS 'Fixture extension';\n")

			c.Assert(got, qt.Contains, "rejects COMMENT ON global metadata because")
			c.Assert(got, qt.Contains, `SQL: COMMENT ON EXTENSION "pg_trgm" IS 'Fixture extension'`)
		})
	}
}
