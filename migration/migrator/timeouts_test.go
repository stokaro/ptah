package migrator

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
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

// TestParseMigrationTimeoutDirectives_ToleratesTheFileDirectiveFamilies pins
// the timeout scanner against the `-- +ptah` directive vocabulary that
// ParseFileDirectives owns. The scanner runs on every migration file load, so
// any directive family owned by another parser must pass through it without
// error — otherwise files carrying that directive fail to load entirely (this
// regressed once for `check`). When adding a new directive family, add a row
// here, or a test beside
// TestParseMigrationTimeoutDirectives_ToleratesThePreMigrationCheckDirective if
// another parser owns it.
//
// Each row asserts the whole directive map its line produces, so the family is
// proven real by its own parser rather than assumed: a line the owning parser
// silently ignores would leave the tolerance assertion below measuring nothing.
//
// This is a white-box test because parseMigrationTimeoutDirectives is the
// unexported scanner on the file-load path; the cross-check must target it
// directly to guard the two parsers against drifting.
func TestParseMigrationTimeoutDirectives_ToleratesTheFileDirectiveFamilies(t *testing.T) {
	tests := []struct {
		name           string
		line           string
		wantDirectives map[string]string
	}{
		{
			name:           "no_transaction",
			line:           "-- +ptah " + DirectiveNoTransaction,
			wantDirectives: map[string]string{DirectiveNoTransaction: "true"},
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
			c.Assert(ParseFileDirectives(sql), qt.DeepEquals, tt.wantDirectives)

			_, err := parseMigrationTimeoutDirectives(sql)
			c.Assert(err, qt.IsNil,
				qt.Commentf("timeout scanner must tolerate the %s directive family or files carrying it cannot load", tt.name))
		})
	}
}

// TestParseMigrationTimeoutDirectives_ToleratesThePreMigrationCheckDirective is
// the same cross-check for the one family ParseChecks owns rather than
// ParseFileDirectives — and the family the scanner actually regressed on.
func TestParseMigrationTimeoutDirectives_ToleratesThePreMigrationCheckDirective(t *testing.T) {
	c := qt.New(t)

	sql := `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort` +
		"\nALTER TABLE users ADD COLUMN email TEXT;"

	checks, err := ParseChecks(sql, "")
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.HasLen, 1)
	c.Assert(checks[0].Name, qt.Equals, "users_empty")

	_, err = parseMigrationTimeoutDirectives(sql)
	c.Assert(err, qt.IsNil,
		qt.Commentf("timeout scanner must tolerate the check directive family or files carrying it cannot load"))
}

// TestMigrationTimeoutsUseTheExecutionDialect pins the recomputation, and it
// needs a dialect that reads the file NARROWLY to be worth anything.
//
// SQL Server is that dialect: its lexer options are the only ones that disable
// hash comments, so `# ...` is not a comment there and the timeout line below it
// is outside the header. Every other target -- and the unresolved dialect load
// time uses, which must never read a shorter header than the target will --
// keeps the timeouts. Asserting the load-time snapshot were EMPTY instead would
// pass for the wrong reason: it would also pass with the header rule broken for
// MySQL, which is the file this fixture is written for.
func TestMigrationTimeoutsUseTheExecutionDialect(t *testing.T) {
	c := qt.New(t)
	sql := "# generated by deploy tooling\n-- +ptah lock_timeout=3s statement_timeout=30s\nALTER TABLE users ADD COLUMN email TEXT;"
	header := "# generated by deploy tooling\n-- +ptah lock_timeout=3s statement_timeout=30s\n"
	c.Assert(directiveRegion(sql, platform.MySQL), qt.Equals, header)
	c.Assert(directiveRegion(sql, ""), qt.Equals, header)
	c.Assert(directiveRegion(sql, platform.SQLServer), qt.Equals, "")

	loaded, err := parseMigrationTimeoutDirectives(sql)
	c.Assert(err, qt.IsNil)
	c.Assert(loaded.HasLockTimeout, qt.IsTrue)

	migration := &Migration{
		UpSQL:               sql,
		DownSQL:             sql,
		UpTimeouts:          loaded,
		DownTimeouts:        loaded,
		upParsedTimeouts:    loaded,
		downParsedTimeouts:  loaded,
		upTimeoutsFromSQL:   true,
		downTimeoutsFromSQL: true,
	}

	up, err := migration.upTimeoutsForDialect(platform.MySQL)
	c.Assert(err, qt.IsNil)
	c.Assert(up.LockTimeout, qt.Equals, 3*time.Second)
	c.Assert(up.StatementTimeout, qt.Equals, 30*time.Second)
	c.Assert(up.HasLockTimeout, qt.IsTrue)
	c.Assert(up.HasStatementTimeout, qt.IsTrue)

	down, err := migration.downTimeoutsForDialect(platform.MariaDB)
	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.DeepEquals, up)

	clickhouse, err := migration.upTimeoutsForDialect(platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	c.Assert(clickhouse, qt.DeepEquals, up)

	sqlserver, err := migration.upTimeoutsForDialect(platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(sqlserver.IsZero(), qt.IsTrue)
}

func TestMigrationTimeoutsKeepAnExplicitPublicOverride(t *testing.T) {
	c := qt.New(t)
	sql := "# generated by deploy tooling\n-- +ptah lock_timeout=3s\nSELECT 1;"
	loaded, err := parseMigrationTimeoutDirectives(sql)
	c.Assert(err, qt.IsNil)
	override := MigrationTimeouts{
		LockTimeout:    9 * time.Second,
		HasLockTimeout: true,
	}
	migration := &Migration{
		UpSQL:             sql,
		UpTimeouts:        override,
		upParsedTimeouts:  loaded,
		upTimeoutsFromSQL: true,
	}

	got, err := migration.upTimeoutsForDialect(platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, override)
}

func TestDurationUnitCeilUsesIntegerMath(t *testing.T) {
	c := qt.New(t)

	c.Assert(durationMillis(1500*time.Microsecond), qt.Equals, int64(2))
	c.Assert(durationSeconds(1500*time.Millisecond), qt.Equals, int64(2))

	maxDuration := time.Duration(1<<63 - 1)
	c.Assert(durationMillis(maxDuration), qt.Equals, int64(maxDuration/time.Millisecond)+1)
	c.Assert(durationSeconds(maxDuration), qt.Equals, int64(maxDuration/time.Second)+1)
}
