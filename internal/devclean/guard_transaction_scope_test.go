package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

// TestReplayGuardTransactionScopedSetting_HappyPath pins the settings a replay
// accepts because their effect ends with the transaction they run in. Replay
// runs every migration on one session, and these cannot reach the next one.
func TestReplayGuardTransactionScopedSetting_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "SET LOCAL with =", statement: `SET LOCAL lock_timeout = '5s'`},
		{name: "SET LOCAL with TO", statement: `SET LOCAL statement_timeout TO '30s'`},
		{name: "SET LOCAL of a quoted parameter", statement: `SET LOCAL "lock_timeout" = '5s'`},
		{name: "SET LOCAL of a custom parameter", statement: `SET LOCAL app.tenant_id = '42'`},
		{name: "SET LOCAL TIME ZONE", statement: `SET LOCAL TIME ZONE 'UTC'`},
		{name: "SET LOCAL to the default", statement: `SET LOCAL check_function_bodies TO DEFAULT`},
		{name: "SET CONSTRAINTS", statement: `SET CONSTRAINTS ALL DEFERRED`},
		{name: "set_config is_local", statement: `SELECT set_config('lock_timeout', '5s', true)`},
		{name: "set_config of a custom parameter", statement: `SELECT set_config('app.tenant_id', lower('ACME'), true)`},
		{name: "set_config in a larger query", statement: `SELECT set_config('app.tenant_id', id::text, true) FROM tenants WHERE slug = 'acme'`},
	}
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: dialect, Schema: "public"})
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
			})
		}
	}
}

// TestReplayGuardTransactionScopedSetting_FailurePath is the control for the
// test above. A plain SET, RESET or set_config with is_local false outlives
// its transaction and reaches the migrations after it. search_path decides
// where an unqualified name lands, and role and session_authorization decide
// who the rest of the migration runs as, so those stay refused even when set
// for one transaction. A set_config whose name or is_local is not a literal
// cannot be read, and is refused.
func TestReplayGuardTransactionScopedSetting_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "plain SET", statement: `SET lock_timeout = '5s'`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "SET SESSION", statement: `SET SESSION lock_timeout = '5s'`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "RESET", statement: `RESET lock_timeout`, wantErr: `.*rejects RESET session or transaction state .*`},
		{name: "RESET ALL", statement: `RESET ALL`, wantErr: `.*rejects RESET session or transaction state .*`},
		{name: "SET LOCAL search_path", statement: `SET LOCAL search_path = pg_catalog`, wantErr: `.*rejects SET search_path .*`},
		{name: "SET LOCAL SCHEMA", statement: `SET LOCAL SCHEMA 'pg_catalog'`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "SET LOCAL ROLE", statement: `SET LOCAL ROLE app_owner`, wantErr: `.*rejects SET ROLE .*`},
		{name: "SET LOCAL role parameter", statement: `SET LOCAL role = app_owner`, wantErr: `.*rejects SET ROLE .*`},
		{name: "SET LOCAL session_authorization", statement: `SET LOCAL session_authorization = app_owner`, wantErr: `.*rejects SET session or transaction state .*`},
		{name: "SET LOCAL SESSION AUTHORIZATION", statement: `SET LOCAL SESSION AUTHORIZATION app_owner`, wantErr: `.*rejects SET SESSION AUTHORIZATION .*`},
		{name: "set_config session-wide", statement: `SELECT set_config('lock_timeout', '5s', false)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of search_path", statement: `SELECT set_config('search_path', 'pg_catalog', true)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of role", statement: `SELECT set_config('role', 'app_owner', true)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of a computed name", statement: `SELECT set_config(current_setting('app.target'), 'x', true)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config of a column", statement: `SELECT set_config(name, 'x', true) FROM settings`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config with is_local as a string", statement: `SELECT set_config('lock_timeout', '5s', 'true')`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config with a computed is_local", statement: `SELECT set_config('lock_timeout', '5s', 1 = 1)`, wantErr: `.*rejects cluster control function .*`},
		{name: "set_config beside a control function", statement: `SELECT set_config('lock_timeout', '5s', true), pg_terminate_backend(42)`, wantErr: `.*rejects cluster control function .*`},
	}
	guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}
