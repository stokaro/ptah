package migratelock_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/migratelock"
)

const flagName = "migration-lock-timeout"

func TestEnsure_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "yugabytedb", dialect: "yugabytedb"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlserver", dialect: "sqlserver"},
		{name: "alias resolves before the lookup", dialect: "postgresql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(migratelock.Ensure("--"+flagName, test.dialect), qt.IsNil)
		})
	}
}

func TestEnsure_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantErr string
	}{
		{
			name:    "sqlite",
			dialect: "sqlite",
			wantErr: `--migration-lock-timeout requested the migration advisory lock, and dialect "sqlite" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --migration-lock-timeout to run without a lock`,
		},
		{
			name:    "clickhouse",
			dialect: "clickhouse",
			wantErr: `--migration-lock-timeout requested the migration advisory lock, and dialect "clickhouse" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --migration-lock-timeout to run without a lock`,
		},
		{
			name:    "cockroachdb",
			dialect: "cockroachdb",
			wantErr: `--migration-lock-timeout requested the migration advisory lock, and dialect "cockroachdb" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --migration-lock-timeout to run without a lock`,
		},
		{
			name:    "spanner",
			dialect: "spanner",
			wantErr: `--migration-lock-timeout requested the migration advisory lock, and dialect "spanner" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --migration-lock-timeout to run without a lock`,
		},
		{
			name:    "oracle",
			dialect: "oracle",
			wantErr: `--migration-lock-timeout requested the migration advisory lock, and dialect "oracle" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --migration-lock-timeout to run without a lock`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := migratelock.Ensure("--"+flagName, test.dialect)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			var unsupported *migratelock.UnsupportedError
			c.Assert(err, qt.ErrorAs, &unsupported)
			c.Assert(unsupported.Dialect, qt.Equals, test.dialect)
			c.Assert(unsupported.Request, qt.Equals, "--"+flagName)
		})
	}
}

// TestRequestDecide_FailurePath covers the spellings a plain command can carry.
// PTAH_MIGRATION_LOCK_TIMEOUT needs the binding the root installs and is
// measured through the shipped command tree instead.
func TestRequestDecide_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		fromConfig bool
		wantErr    string
	}{
		{
			name:    "typed value",
			args:    []string{"--migration-lock-timeout", "10s"},
			wantErr: `--migration-lock-timeout requested the migration advisory lock`,
		},
		{
			name:    "typed empty value asks for an unbounded wait",
			args:    []string{"--migration-lock-timeout", ""},
			wantErr: `--migration-lock-timeout requested the migration advisory lock`,
		},
		{
			name:       "project config",
			args:       nil,
			fromConfig: true,
			wantErr:    `migration.migration_lock_timeout requested the migration advisory lock`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := commandWithLockFlag(c, test.args)

			request := migratelock.Request{
				Cmd:        cmd,
				FlagName:   flagName,
				FromConfig: test.fromConfig,
				DBURL:      "sqlite://target.db",
			}

			c.Assert(request.Decide("sqlite"), qt.ErrorMatches, regexp.QuoteMeta(test.wantErr)+`.*`)
		})
	}
}

// TestRequestDecide_HappyPath holds the decision to a request the operator made
// against a target that cannot lock. Each row is a reason to stay quiet.
func TestRequestDecide_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		fromConfig bool
		dbURL      string
		dialect    string
	}{
		{name: "nobody asked", args: nil, dbURL: "sqlite://target.db", dialect: "sqlite"},
		{
			name:    "the dialect locks",
			args:    []string{"--migration-lock-timeout", "10s"},
			dbURL:   "postgres://ptah@127.0.0.1:1/db",
			dialect: "postgres",
		},
		{
			name:    "an unclassified URL makes no claim",
			args:    []string{"--migration-lock-timeout", "10s"},
			dbURL:   "sqlite://target.db",
			dialect: "",
		},
		{
			name:    "a docker URL names a dev engine, not a target",
			args:    []string{"--migration-lock-timeout", "10s"},
			dbURL:   "docker://sqlite/dev",
			dialect: "sqlite",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := commandWithLockFlag(c, test.args)

			request := migratelock.Request{
				Cmd:        cmd,
				FlagName:   flagName,
				FromConfig: test.fromConfig,
				DBURL:      test.dbURL,
			}

			c.Assert(request.Decide(test.dialect), qt.IsNil)
		})
	}
}

// TestRequestDecideConnected_FailurePath is the wire-dialect half offline: a
// URL that resolves to a dialect which locks, in front of a server that does
// not. Only the connected dialect can answer, so only this decision fires.
func TestRequestDecideConnected_FailurePath(t *testing.T) {
	c := qt.New(t)
	cmd := commandWithLockFlag(c, []string{"--migration-lock-timeout", "10s"})

	request := migratelock.Request{
		Cmd:      cmd,
		FlagName: flagName,
		DBURL:    "postgres://root@127.0.0.1:1/db",
	}

	c.Assert(request.Decide("postgres"), qt.IsNil,
		qt.Commentf("the URL alone says PostgreSQL, which locks"))
	c.Assert(request.DecideConnected("cockroachdb"), qt.ErrorMatches,
		regexp.QuoteMeta(`--migration-lock-timeout requested the migration advisory lock, and dialect "cockroachdb" has none`)+`.*`)
}

// TestRequestDecideConnected_HappyPath keeps the second decision off a target
// the URL already named. Where the two agree there is nothing new to weigh, and
// deciding twice would double whatever the first decision wrote.
func TestRequestDecideConnected_HappyPath(t *testing.T) {
	c := qt.New(t)
	cmd := commandWithLockFlag(c, []string{"--migration-lock-timeout", "10s"})

	request := migratelock.Request{
		Cmd:      cmd,
		FlagName: flagName,
		DBURL:    "postgres://ptah@127.0.0.1:1/db",
	}

	c.Assert(request.DecideConnected("postgres"), qt.IsNil)
}

// commandWithLockFlag builds a command registering the lock timeout flag and
// parses args against it, so pflag's own bookkeeping decides what was set
// rather than the test asserting it.
func commandWithLockFlag(c *qt.C, args []string) *cobra.Command {
	c.Helper()

	cmd := &cobra.Command{
		Use:  "probe",
		RunE: func(*cobra.Command, []string) error { return nil },
	}
	var value string
	cmd.Flags().StringVar(&value, flagName, "", "lock timeout")
	c.Assert(cmd.Flags().Parse(args), qt.IsNil)
	return cmd
}
