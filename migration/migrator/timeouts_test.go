package migrator

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestParseMigrationTimeouts(t *testing.T) {
	c := qt.New(t)

	timeouts, err := ParseMigrationTimeouts("3s", "30s")
	c.Assert(err, qt.IsNil)
	c.Assert(timeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(timeouts.LockTimeout, qt.Equals, 3*time.Second)
	c.Assert(timeouts.HasStatementTimeout, qt.IsTrue)
	c.Assert(timeouts.StatementTimeout, qt.Equals, 30*time.Second)
}

func TestParseMigrationTimeouts_Invalid(t *testing.T) {
	c := qt.New(t)

	_, err := ParseMigrationTimeouts("0s", "")
	c.Assert(err, qt.ErrorMatches, "invalid lock timeout: must be greater than zero")
}

func TestMergeMigrationTimeouts(t *testing.T) {
	c := qt.New(t)

	defaults := MigrationTimeouts{
		LockTimeout:         3 * time.Second,
		StatementTimeout:    30 * time.Second,
		HasLockTimeout:      true,
		HasStatementTimeout: true,
	}
	overrides := MigrationTimeouts{
		LockTimeout:    500 * time.Millisecond,
		HasLockTimeout: true,
	}

	got := mergeMigrationTimeouts(defaults, overrides)
	c.Assert(got.LockTimeout, qt.Equals, 500*time.Millisecond)
	c.Assert(got.StatementTimeout, qt.Equals, 30*time.Second)
	c.Assert(got.HasLockTimeout, qt.IsTrue)
	c.Assert(got.HasStatementTimeout, qt.IsTrue)
}

func TestTimeoutStatements(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		timeouts    MigrationTimeouts
		wantSetup   []string
		wantRestore []string
		wantErr     string
	}{
		{
			name:    "postgres",
			dialect: "postgres",
			timeouts: MigrationTimeouts{
				LockTimeout:         3 * time.Second,
				StatementTimeout:    30 * time.Second,
				HasLockTimeout:      true,
				HasStatementTimeout: true,
			},
			wantSetup: []string{
				"SET LOCAL lock_timeout = '3000ms'",
				"SET LOCAL statement_timeout = '30000ms'",
			},
		},
		{
			name:    "mysql",
			dialect: "mysql",
			timeouts: MigrationTimeouts{
				LockTimeout:         1500 * time.Millisecond,
				StatementTimeout:    2500 * time.Millisecond,
				HasLockTimeout:      true,
				HasStatementTimeout: true,
			},
			wantSetup: []string{
				"SET @ptah_prev_innodb_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout",
				"SET SESSION innodb_lock_wait_timeout = 2",
				"SET @ptah_prev_max_execution_time = @@SESSION.max_execution_time",
				"SET SESSION max_execution_time = 2500",
			},
			wantRestore: []string{
				"SET SESSION max_execution_time = @ptah_prev_max_execution_time",
				"SET SESSION innodb_lock_wait_timeout = @ptah_prev_innodb_lock_wait_timeout",
			},
		},
		{
			name:    "mariadb",
			dialect: "mariadb",
			timeouts: MigrationTimeouts{
				LockTimeout:         time.Second,
				StatementTimeout:    1500 * time.Millisecond,
				HasLockTimeout:      true,
				HasStatementTimeout: true,
			},
			wantSetup: []string{
				"SET @ptah_prev_innodb_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout",
				"SET SESSION innodb_lock_wait_timeout = 1",
				"SET @ptah_prev_max_statement_time = @@SESSION.max_statement_time",
				"SET SESSION max_statement_time = 1.5",
			},
			wantRestore: []string{
				"SET SESSION max_statement_time = @ptah_prev_max_statement_time",
				"SET SESSION innodb_lock_wait_timeout = @ptah_prev_innodb_lock_wait_timeout",
			},
		},
		{
			name:    "clickhouse lock timeout",
			dialect: "clickhouse",
			timeouts: MigrationTimeouts{
				LockTimeout:    time.Second,
				HasLockTimeout: true,
			},
			wantErr: `migration timeouts are not supported for dialect "clickhouse"`,
		},
		{
			name:    "spanner unsupported",
			dialect: "spanner",
			timeouts: MigrationTimeouts{
				LockTimeout:    time.Second,
				HasLockTimeout: true,
			},
			wantErr: `migration timeouts are not supported for dialect "spanner"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			gotSetup, gotRestore, err := timeoutStatements(tt.dialect, tt.timeouts)
			if tt.wantErr != "" {
				c.Assert(err, qt.ErrorMatches, tt.wantErr)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(gotSetup, qt.DeepEquals, tt.wantSetup)
			c.Assert(gotRestore, qt.DeepEquals, tt.wantRestore)
		})
	}
}

// TestParseMigrationTimeoutDirectives_ToleratesEveryDirectiveFamily pins the
// timeout scanner against the full `-- +ptah` directive vocabulary. The scanner
// runs on every migration file load, so any directive family owned by another
// parser must pass through it without error — otherwise files carrying that
// directive fail to load entirely (this regressed once for `check`). When
// adding a new directive family, add a row here.
//
// This is a white-box test because parseMigrationTimeoutDirectives is the
// unexported scanner on the file-load path; the cross-check must target it
// directly to guard the two parsers against drifting.
func TestParseMigrationTimeoutDirectives_ToleratesEveryDirectiveFamily(t *testing.T) {
	tests := []struct {
		name string
		line string
		// recognize proves the directive family is real: its owning parser
		// accepts the exact line the timeout scanner is asked to tolerate.
		recognize func(c *qt.C, sql string)
	}{
		{
			name: "no_transaction",
			line: "-- +ptah " + DirectiveNoTransaction,
			recognize: func(c *qt.C, sql string) {
				c.Assert(ParseFileDirectives(sql)[DirectiveNoTransaction], qt.Equals, "true")
			},
		},
		{
			name: "timeouts",
			line: "-- +ptah lock_timeout=3s statement_timeout=30s",
			recognize: func(c *qt.C, sql string) {
				directives := ParseFileDirectives(sql)
				c.Assert(directives["lock_timeout"], qt.Equals, "3s")
				c.Assert(directives["statement_timeout"], qt.Equals, "30s")
			},
		},
		{
			name: "online DDL routing",
			line: "-- +ptah online_ddl_tool=ghost online_ddl_fallback=error",
			recognize: func(c *qt.C, sql string) {
				directives := ParseFileDirectives(sql)
				c.Assert(directives["online_ddl_tool"], qt.Equals, "ghost")
				c.Assert(directives["online_ddl_fallback"], qt.Equals, "error")
			},
		},
		{
			name: "pre-migration check",
			line: `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort`,
			recognize: func(c *qt.C, sql string) {
				checks, err := ParseChecks(sql, "")
				c.Assert(err, qt.IsNil)
				c.Assert(checks, qt.HasLen, 1)
				c.Assert(checks[0].Name, qt.Equals, "users_empty")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql := tt.line + "\nALTER TABLE users ADD COLUMN email TEXT;"
			tt.recognize(c, sql)

			_, err := parseMigrationTimeoutDirectives(sql, directiveScopeHeader)
			c.Assert(err, qt.IsNil,
				qt.Commentf("timeout scanner must tolerate the %s directive family or files carrying it cannot load", tt.name))
		})
	}
}

func TestDurationUnitCeilUsesIntegerMath(t *testing.T) {
	c := qt.New(t)

	c.Assert(durationMillis(1500*time.Microsecond), qt.Equals, int64(2))
	c.Assert(durationSeconds(1500*time.Millisecond), qt.Equals, int64(2))

	maxDuration := time.Duration(1<<63 - 1)
	c.Assert(durationMillis(maxDuration), qt.Equals, int64(maxDuration/time.Millisecond)+1)
	c.Assert(durationSeconds(maxDuration), qt.Equals, int64(maxDuration/time.Second)+1)
}
