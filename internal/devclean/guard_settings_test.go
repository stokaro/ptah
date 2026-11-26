package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

// postgresDumpHeader is what `pg_dump --schema-only` 18.6 sets before and
// among the objects it dumps, one statement per row, in the order it writes
// them. The \restrict line pg_dump 18 writes first is a psql command, which
// the parser leaves out of a schema file.
var postgresDumpHeader = []string{
	`SET statement_timeout = 0`,
	`SET lock_timeout = 0`,
	`SET idle_in_transaction_session_timeout = 0`,
	`SET transaction_timeout = 0`,
	`SET client_encoding = 'UTF8'`,
	`SET standard_conforming_strings = on`,
	`SELECT pg_catalog.set_config('search_path', '', false)`,
	`SET check_function_bodies = false`,
	`SET xmloption = content`,
	`SET client_min_messages = warning`,
	`SET row_security = off`,
	`SET default_tablespace = ''`,
	`SET default_table_access_method = heap`,
}

// TestReplayGuardSetting_HappyPath pins the settings a replay accepts for the
// rest of its session: the pg_dump header, a timeout, and a value of
// search_path, standard_conforming_strings or client_encoding that leaves
// the guard's reading of later statements as it is. The empty search_path
// gives an unqualified CREATE no schema to land in, and RESET gives back the
// path the session started with. Every dialect of the family shares the one
// reading.
func TestReplayGuardSetting_HappyPath(t *testing.T) {
	statements := append([]string{
		`SET search_path = ''`,
		`SET search_path TO ''`,
		`SET SESSION search_path = ''`,
		`SET LOCAL search_path = ''`,
		`SET search_path TO DEFAULT`,
		`RESET search_path`,
		`SELECT set_config('search_path', '', false)`,
		`SELECT set_config('search_path', '', true)`,
		`SET standard_conforming_strings TO 'on'`,
		`SET standard_conforming_strings = true`,
		`SET LOCAL standard_conforming_strings = on`,
		`SELECT set_config('standard_conforming_strings', 'on', false)`,
		`SET client_encoding TO utf8`,
		`SET client_encoding = 'utf-8'`,
		`SET client_encoding = 'Unicode'`,
		`SET NAMES 'UTF8'`,
		`SET LOCAL client_encoding = 'UTF8'`,
		`SELECT set_config('client_encoding', 'UTF8', false)`,
		`SET lock_timeout = '5s'`,
		`SET SESSION statement_timeout TO '30s'`,
		`SET "lock_timeout" = '5s'`,
		`RESET lock_timeout`,
		`SET idle_session_timeout = 0`,
		`SET XML OPTION DOCUMENT`,
		`SET default_tablespace = pg_default`,
		`SELECT set_config('lock_timeout', '5s', false)`,
	}, postgresDumpHeader...)
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: dialect, Schema: "public"}, devclean.ReplayRealmDatabase)
		for _, statement := range statements {
			t.Run(dialect+"/"+statement, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(statement), qt.IsNil)
			})
		}
	}
}

// TestReplayGuardSetting_FailurePath is the control for the test above, in
// every scope a setting can have. A search_path that names a schema decides
// where a later unqualified name lands, and the guard reads one by the
// catalog of the path the session started with. A standard_conforming_strings or client_encoding other than on and
// UTF-8 makes the server read a later statement's bytes by rules the guard
// does not, and DEFAULT or RESET gives back a value the guard did not read.
// role and session_authorization change who the rest runs as. A set_config
// value the guard cannot read as one plain string literal is refused for
// those parameters, since a column named utf8 or "on" is a value too.
func TestReplayGuardSetting_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "search_path naming a schema", statement: `SET search_path = other`, wantErr: `.*rejects SET search_path .*`},
		{name: "search_path for a transaction", statement: `SET LOCAL search_path = other`, wantErr: `.*rejects SET search_path .*`},
		{name: "search_path naming pg_catalog", statement: `SET search_path TO pg_catalog`, wantErr: `.*rejects SET search_path .*`},
		{name: "search_path list", statement: `SET search_path = '', pg_catalog`, wantErr: `.*rejects SET search_path .*`},
		{name: "search_path escape string", statement: `SET search_path = E''`, wantErr: `.*rejects SET search_path .*`},
		{name: "search_path from current", statement: `SET search_path FROM CURRENT`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "SET SCHEMA", statement: `SET SCHEMA ''`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "set_config of a schema path", statement: `SELECT set_config('search_path', 'other', false)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of a computed path", statement: `SELECT set_config('search_path', '' || 'pg_catalog', false)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of a NULL path", statement: `SELECT set_config('search_path', NULL, false)`, wantErr: `.*rejects cluster control function .*`},
		{name: "standard_conforming_strings off", statement: `SET standard_conforming_strings = off`, wantErr: `.*rejects SET standard_conforming_strings .*`},
		{name: "standard_conforming_strings off for a transaction", statement: `SET LOCAL standard_conforming_strings = off`, wantErr: `.*rejects SET standard_conforming_strings .*`},
		{name: "standard_conforming_strings default", statement: `SET standard_conforming_strings TO DEFAULT`, wantErr: `.*rejects SET standard_conforming_strings .*`},
		{name: "standard_conforming_strings reset", statement: `RESET standard_conforming_strings`, wantErr: `.*rejects RESET standard_conforming_strings .*`},
		{name: "set_config of standard_conforming_strings off", statement: `SELECT set_config('standard_conforming_strings', 'off', true)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of standard_conforming_strings from a column", statement: `SELECT set_config('standard_conforming_strings', "on", false) FROM (SELECT 'off' AS "on") AS s`, wantErr: `.*rejects cluster control function .*`},
		{name: "client_encoding GBK", statement: `SET client_encoding = 'GBK'`, wantErr: `.*rejects SET client_encoding .*`},
		{name: "client_encoding GBK for a transaction", statement: `SET LOCAL client_encoding = 'GBK'`, wantErr: `.*rejects SET client_encoding .*`},
		{name: "SET NAMES SJIS", statement: `SET NAMES 'SJIS'`, wantErr: `.*rejects SET client_encoding .*`},
		{name: "SET LOCAL NAMES GBK", statement: `SET LOCAL NAMES 'GBK'`, wantErr: `.*rejects SET client_encoding .*`},
		{name: "client_encoding reset", statement: `RESET client_encoding`, wantErr: `.*rejects RESET client_encoding .*`},
		{name: "client_encoding escape string", statement: `SET client_encoding = E'UTF8'`, wantErr: `.*rejects SET client_encoding .*`},
		{name: "set_config of client_encoding GBK", statement: `SELECT set_config('client_encoding', 'GBK', true)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of client_encoding from a column", statement: `SELECT set_config('client_encoding', utf8, false) FROM (SELECT 'GBK' AS utf8) AS s`, wantErr: `.*rejects cluster control function .*`},
		{name: "SET ROLE", statement: `SET ROLE app_owner`, wantErr: `.*rejects SET ROLE .*`},
		{name: "SET SESSION AUTHORIZATION", statement: `SET SESSION AUTHORIZATION app_owner`, wantErr: `.*rejects SET SESSION AUTHORIZATION .*`},
		{name: "session_authorization parameter", statement: `SET session_authorization = app_owner`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "RESET ROLE", statement: `RESET ROLE`, wantErr: `.*rejects RESET session or transaction state .*`},
		{name: "RESET SESSION AUTHORIZATION", statement: `RESET SESSION AUTHORIZATION`, wantErr: `.*rejects RESET session or transaction state .*`},
		{name: "set_config of role", statement: `SELECT set_config('role', 'app_owner', false)`, wantErr: `.*rejects cluster control function .*`},
		{name: "allow_system_table_mods", statement: `SET LOCAL allow_system_table_mods = on`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "database", statement: `SET database = other`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "custom parameter for the session", statement: `SET app.tenant_id = '42'`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "time zone for the session", statement: `SET TIME ZONE 'UTC'`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "time zone reset", statement: `RESET TIME ZONE`, wantErr: `.*rejects RESET session or transaction state .*`},
		{name: "set_config of a custom parameter for the session", statement: `SELECT set_config('app.tenant_id', '42', false)`, wantErr: `.*rejects cluster control function .*`},
	}
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: dialect, Schema: "public"}, devclean.ReplayRealmDatabase)
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
			})
		}
	}
}

// TestReplayGuardSetting_FailurePathTrusted pins that trust lifts none of the
// refusals above: each would mislead the guard about the statements after it,
// which trust leaves the guard to read. ErrorMatches anchors the whole
// message, so a trust remedy appended to one fails the row.
func TestReplayGuardSetting_FailurePathTrusted(t *testing.T) {
	const refused = ` because its effects cannot be confined to the disposable database realm`
	tests := []struct {
		statement string
		wantErr   string
	}{
		{statement: `SET search_path = other`, wantErr: `postgres migration replay rejects SET search_path` + refused},
		{statement: `SET standard_conforming_strings = off`, wantErr: `postgres migration replay rejects SET standard_conforming_strings` + refused},
		{statement: `SET LOCAL client_encoding = 'GBK'`, wantErr: `postgres migration replay rejects SET client_encoding` + refused},
		{statement: `SET session_authorization = app_owner`, wantErr: `postgres migration replay rejects SET session or transaction state` + refused},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			trustReplaySQL(c, trustedPostgresDev.URL)
			c.Assert(devclean.NewDevReplayGuard(trustedPostgresDev).ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}
