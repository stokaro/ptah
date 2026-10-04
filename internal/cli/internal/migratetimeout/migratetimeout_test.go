package migratetimeout_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/cli/internal/migratetimeout"
	"ptah.run/migration/migrationfile"
)

var both = migrationfile.Timeouts{
	LockTimeout: 2 * time.Second, HasLockTimeout: true,
	StatementTimeout: 30 * time.Second, HasStatementTimeout: true,
}

// TestRequestDecide_FailurePath names each spelling the operator addressed to
// the command, and only the timeouts the target has no key for.
// PTAH_LOCK_TIMEOUT and PTAH_STATEMENT_TIMEOUT need the binding the root
// installs and are measured through the shipped command tree.
func TestRequestDecide_FailurePath(t *testing.T) {
	tests := []struct {
		name                string
		dialect             string
		caps                capability.Capabilities
		args                []string
		lockFromConfig      bool
		statementFromConfig bool
		timeouts            migrationfile.Timeouts
		wantErr             string
		wantMissing         []capability.Capability
	}{
		{
			name:     "both flags typed at a target with neither timeout",
			dialect:  "sqlite",
			caps:     capability.SQLite3(),
			args:     []string{"--lock-timeout", "2s", "--statement-timeout", "30s"},
			timeouts: both,
			wantErr: `--lock-timeout and --statement-timeout set timeouts for every migration, and dialect "sqlite" has ` +
				`no lock or statement timeout Ptah can set and restore around a migration. ` +
				`Remove --lock-timeout and --statement-timeout to run without one`,
			wantMissing: []capability.Capability{capability.MigrationLockTimeout, capability.MigrationStatementTimeout},
		},
		{
			// YDB bounds each statement, so only the lock timeout is refused.
			name:     "both flags typed at a target with a statement timeout only",
			dialect:  "ydb",
			caps:     capability.YDB262(),
			args:     []string{"--lock-timeout", "2s", "--statement-timeout", "30s"},
			timeouts: both,
			wantErr: `--lock-timeout sets a timeout for every migration, and dialect "ydb" has ` +
				`no lock wait Ptah can bound around a migration \(target capability migration_lock_timeout\). ` +
				`Remove --lock-timeout to run without one`,
			wantMissing: []capability.Capability{capability.MigrationLockTimeout},
		},
		{
			name:     "one flag typed",
			dialect:  "sqlite",
			caps:     capability.SQLite3(),
			args:     []string{"--statement-timeout", "30s"},
			timeouts: migrationfile.Timeouts{StatementTimeout: 30 * time.Second, HasStatementTimeout: true},
			wantErr: `--statement-timeout sets a timeout for every migration, and dialect "sqlite" has ` +
				`no lock or statement timeout Ptah can set and restore around a migration. ` +
				`Remove --statement-timeout to run without one`,
			wantMissing: []capability.Capability{capability.MigrationLockTimeout, capability.MigrationStatementTimeout},
		},
		{
			name:           "project config",
			dialect:        "ydb",
			caps:           capability.YDB262(),
			lockFromConfig: true,
			timeouts:       migrationfile.Timeouts{LockTimeout: 2 * time.Second, HasLockTimeout: true},
			wantErr:        `migration.lock_timeout sets a timeout for every migration, and dialect "ydb" has no lock wait .*`,
			wantMissing:    []capability.Capability{capability.MigrationLockTimeout},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratetimeout.Request{
				Cmd:                 commandWithTimeoutFlags(c, test.args),
				LockFlag:            "lock-timeout",
				StatementFlag:       "statement-timeout",
				LockFromConfig:      test.lockFromConfig,
				StatementFromConfig: test.statementFromConfig,
			}

			err := request.Decide(test.dialect, test.caps, test.timeouts)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var unsupported *migratetimeout.UnsupportedError
			c.Assert(err, qt.ErrorAs, &unsupported)
			c.Assert(unsupported.Dialect, qt.Equals, test.dialect)
			c.Assert(unsupported.Missing, qt.DeepEquals, test.wantMissing)
		})
	}
}

// Each row is a reason to stay quiet.
func TestRequestDecide_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		caps     capability.Capabilities
		timeouts migrationfile.Timeouts
	}{
		{name: "nobody asked", caps: capability.YDB262()},
		{name: "the target takes timeouts", args: []string{"--lock-timeout", "2s"}, caps: capability.Postgres18(),
			timeouts: migrationfile.Timeouts{LockTimeout: 2 * time.Second, HasLockTimeout: true}},
		{name: "the target takes the statement timeout it was given", args: []string{"--statement-timeout", "30s"},
			caps:     capability.YDB262(),
			timeouts: migrationfile.Timeouts{StatementTimeout: 30 * time.Second, HasStatementTimeout: true}},
		{name: "a value nobody typed or configured", caps: capability.SQLite3(), timeouts: both},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratetimeout.Request{
				Cmd:           commandWithTimeoutFlags(c, test.args),
				LockFlag:      "lock-timeout",
				StatementFlag: "statement-timeout",
			}

			c.Assert(request.Decide("ydb", test.caps, test.timeouts), qt.IsNil)
		})
	}
}

func commandWithTimeoutFlags(c *qt.C, args []string) *cobra.Command {
	c.Helper()
	cmd := &cobra.Command{Use: "probe", RunE: func(*cobra.Command, []string) error { return nil }}
	var lock, statement string
	cmd.Flags().StringVar(&lock, "lock-timeout", "", "lock timeout")
	cmd.Flags().StringVar(&statement, "statement-timeout", "", "statement timeout")
	c.Assert(cmd.Flags().Parse(args), qt.IsNil)
	return cmd
}

// A URL whose dialect lacks the timeout's key refuses before anything
// connects.
func TestRequestDecideFromURL_FailurePath(t *testing.T) {
	statement := migrationfile.Timeouts{StatementTimeout: 30 * time.Second, HasStatementTimeout: true}
	lock := migrationfile.Timeouts{LockTimeout: 2 * time.Second, HasLockTimeout: true}
	tests := []struct {
		name     string
		url      string
		args     []string
		timeouts migrationfile.Timeouts
		wantErr  string
	}{
		{name: "sqlite", url: "sqlite://versioned.db", args: []string{"--statement-timeout", "30s"}, timeouts: statement,
			wantErr: `--statement-timeout sets a timeout .* dialect "sqlite" .*`},
		{name: "ydb lock timeout", url: "ydb://localhost:2136/local", args: []string{"--lock-timeout", "2s"},
			timeouts: lock, wantErr: `--lock-timeout sets a timeout .* dialect "ydb" has no lock wait .*`},
		{name: "clickhouse", url: "clickhouse://localhost:9000/db", args: []string{"--statement-timeout", "30s"},
			timeouts: statement, wantErr: `--statement-timeout sets a timeout .* dialect "clickhouse" .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratetimeout.Request{
				Cmd:           commandWithTimeoutFlags(c, test.args),
				LockFlag:      "lock-timeout",
				StatementFlag: "statement-timeout",
				DBURL:         test.url,
			}

			err := request.DecideFromURL(test.timeouts)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// Each row is a URL that does not settle the answer before connecting, a
// target that takes the timeout, or a run that asked for nothing.
func TestRequestDecideFromURL_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		args []string
	}{
		{name: "a PostgreSQL-wire URL may reach Spanner", url: "postgres://localhost/db",
			args: []string{"--statement-timeout", "30s"}},
		{name: "a MySQL URL", url: "mysql://localhost/db", args: []string{"--statement-timeout", "30s"}},
		{name: "a YDB URL takes a statement timeout", url: "ydb://localhost:2136/local",
			args: []string{"--statement-timeout", "30s"}},
		{name: "a URL Ptah cannot classify", url: "nosuch://x", args: []string{"--statement-timeout", "30s"}},
		{name: "nothing typed", url: "sqlite://versioned.db"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratetimeout.Request{
				Cmd:           commandWithTimeoutFlags(c, test.args),
				LockFlag:      "lock-timeout",
				StatementFlag: "statement-timeout",
				DBURL:         test.url,
			}

			c.Assert(request.DecideFromURL(migrationfile.Timeouts{
				StatementTimeout: 30 * time.Second, HasStatementTimeout: true,
			}), qt.IsNil)
		})
	}
}
