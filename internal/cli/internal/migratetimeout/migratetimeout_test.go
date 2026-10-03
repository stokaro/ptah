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
// the command. PTAH_LOCK_TIMEOUT and PTAH_STATEMENT_TIMEOUT need the binding the
// root installs and are measured through the shipped command tree.
func TestRequestDecide_FailurePath(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		lockFromConfig      bool
		statementFromConfig bool
		timeouts            migrationfile.Timeouts
		wantErr             string
	}{
		{
			name:     "both flags typed",
			args:     []string{"--lock-timeout", "2s", "--statement-timeout", "30s"},
			timeouts: both,
			wantErr: `--lock-timeout and --statement-timeout set timeouts for every migration, and dialect "ydb" has ` +
				`no lock or statement timeout Ptah can set and restore around a migration. ` +
				`Remove --lock-timeout and --statement-timeout to run without one`,
		},
		{
			name:     "one flag typed",
			args:     []string{"--statement-timeout", "30s"},
			timeouts: migrationfile.Timeouts{StatementTimeout: 30 * time.Second, HasStatementTimeout: true},
			wantErr: `--statement-timeout sets a timeout for every migration, and dialect "ydb" has ` +
				`no lock or statement timeout Ptah can set and restore around a migration. ` +
				`Remove --statement-timeout to run without one`,
		},
		{
			name:           "project config",
			lockFromConfig: true,
			timeouts:       migrationfile.Timeouts{LockTimeout: 2 * time.Second, HasLockTimeout: true},
			wantErr:        `migration.lock_timeout sets a timeout for every migration, and dialect "ydb" has .*`,
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

			err := request.Decide("ydb", capability.YDB262(), test.timeouts)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var unsupported *migratetimeout.UnsupportedError
			c.Assert(err, qt.ErrorAs, &unsupported)
			c.Assert(unsupported.Dialect, qt.Equals, "ydb")
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
		{name: "a value nobody typed or configured", caps: capability.YDB262(), timeouts: both},
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
