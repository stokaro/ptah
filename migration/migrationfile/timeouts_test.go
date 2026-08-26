package migrationfile_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrationfile"
)

func TestParseTimeouts(t *testing.T) {
	tests := []struct {
		name                    string
		sql                     string
		wantLockTimeout         time.Duration
		wantStatementTimeout    time.Duration
		wantHasLockTimeout      bool
		wantHasStatementTimeout bool
		wantErr                 string
	}{
		{
			name: "directives at top of file",
			sql: `-- Migration header
-- +ptah lock_timeout=3s
-- +ptah statement_timeout=30s

ALTER TABLE users ADD COLUMN email TEXT;`,
			wantLockTimeout:         3 * time.Second,
			wantStatementTimeout:    30 * time.Second,
			wantHasLockTimeout:      true,
			wantHasStatementTimeout: true,
		},
		{
			name: "multiple directives on one line",
			sql: `-- +ptah lock_timeout=500ms statement_timeout=2m
ALTER TABLE users ADD COLUMN email TEXT;`,
			wantLockTimeout:         500 * time.Millisecond,
			wantStatementTimeout:    2 * time.Minute,
			wantHasLockTimeout:      true,
			wantHasStatementTimeout: true,
		},
		{
			name: "directive after SQL is ignored",
			sql: `ALTER TABLE users ADD COLUMN email TEXT;
-- +ptah lock_timeout=3s`,
		},
		{
			name: "other ptah directive is ignored",
			sql:  "-- +ptah unknown_timeout=3s\nALTER TABLE users ADD COLUMN email TEXT;",
		},
		{
			name: "online ddl directive is ignored by timeout parser",
			sql:  "-- +ptah online_ddl_tool=ghost\nALTER TABLE users ADD COLUMN email TEXT;",
		},
		{
			name: "no transaction directive is ignored by timeout parser",
			sql:  "-- +ptah no_transaction\nALTER TABLE users ADD COLUMN email TEXT;",
		},
		{
			name: "check directive is ignored by timeout parser",
			sql:  `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort` + "\nDROP TABLE users;",
		},
		{
			name: "check directive alongside timeout directives",
			sql: `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
-- +ptah lock_timeout=3s statement_timeout=30s
DROP TABLE users;`,
			wantLockTimeout:         3 * time.Second,
			wantStatementTimeout:    30 * time.Second,
			wantHasLockTimeout:      true,
			wantHasStatementTimeout: true,
		},
		{
			name:    "bare unknown directive fails",
			sql:     "-- +ptah unknown_directive\nALTER TABLE users ADD COLUMN email TEXT;",
			wantErr: `invalid +ptah directive "unknown_directive"`,
		},
		{
			name:    "invalid duration fails",
			sql:     "-- +ptah lock_timeout=soon\nALTER TABLE users ADD COLUMN email TEXT;",
			wantErr: "invalid +ptah lock_timeout value",
		},
		{
			name:    "zero duration fails",
			sql:     "-- +ptah statement_timeout=0s\nALTER TABLE users ADD COLUMN email TEXT;",
			wantErr: "must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := migrationfile.ParseTimeouts(tt.sql)
			if tt.wantErr != "" {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, tt.wantErr)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(got.HasLockTimeout, qt.Equals, tt.wantHasLockTimeout)
			c.Assert(got.HasStatementTimeout, qt.Equals, tt.wantHasStatementTimeout)
			c.Assert(got.LockTimeout, qt.Equals, tt.wantLockTimeout)
			c.Assert(got.StatementTimeout, qt.Equals, tt.wantStatementTimeout)
		})
	}
}

// TestParseTimeouts_ToleratesTheFileDirectiveFamilies pins
// the timeout scanner against the `-- +ptah` directive vocabulary that
// migrationfile.ParseDirectives owns. The scanner runs on every migration file load, so
// any directive family owned by another parser must pass through it without
// error — otherwise files carrying that directive fail to load entirely (this
// regressed once for `check`). When adding a new directive family, add a row
// here, or a test beside the migrator's
// TestParseTimeouts_ToleratesThePreMigrationCheckDirective if another parser
// owns it.
//
// Each row asserts the whole directive map its line produces, so the family is
// proven real by its own parser rather than assumed: a line the owning parser
// silently ignores would leave the tolerance assertion below measuring nothing.

func TestParseTimeouts_ToleratesTheFileDirectiveFamilies(t *testing.T) {
	tests := []struct {
		name           string
		line           string
		wantDirectives map[string]string
	}{
		{
			name:           "no_transaction",
			line:           "-- +ptah " + migrationfile.DirectiveNoTransaction,
			wantDirectives: map[string]string{migrationfile.DirectiveNoTransaction: "true"},
		},
		{
			name: "timeouts",
			line: "-- +ptah lock_timeout=3s statement_timeout=30s",
			wantDirectives: map[string]string{
				"lock_timeout":      "3s",
				"statement_timeout": "30s",
			},
		},
		{
			name: "online DDL routing",
			line: "-- +ptah online_ddl_tool=ghost online_ddl_fallback=error",
			wantDirectives: map[string]string{
				"online_ddl_tool":     "ghost",
				"online_ddl_fallback": "error",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql := tt.line + "\nALTER TABLE users ADD COLUMN email TEXT;"
			c.Assert(migrationfile.ParseDirectives(sql), qt.DeepEquals, tt.wantDirectives)

			_, err := migrationfile.ParseTimeouts(sql)
			c.Assert(err, qt.IsNil,
				qt.Commentf("timeout scanner must tolerate the %s directive family or files carrying it cannot load", tt.name))
		})
	}
}
