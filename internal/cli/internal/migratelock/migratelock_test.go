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
		{name: "oracle", dialect: "oracle"},
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
				`only postgres, mysql, mariadb, sqlserver, yugabytedb, oracle take a session advisory lock. ` +
				`Remove --migration-lock-timeout to run without a lock`,
		},
		{
			name:    "clickhouse",
			dialect: "clickhouse",
			wantErr: `--migration-lock-timeout requested the migration advisory lock, and dialect "clickhouse" has none: ` +
				`only postgres, mysql, mariadb, sqlserver, yugabytedb, oracle take a session advisory lock. ` +
				`Remove --migration-lock-timeout to run without a lock`,
		},
		{
			name:    "cockroachdb",
			dialect: "cockroachdb",
			wantErr: `--migration-lock-timeout requested the migration advisory lock, and dialect "cockroachdb" has none: ` +
				`only postgres, mysql, mariadb, sqlserver, yugabytedb, oracle take a session advisory lock. ` +
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
	cmd.SetContext(c.Context())
	return cmd
}

const nameFlagName = "migration-lock-name"

// commandWithNameFlag registers the lock name flag the way the commands do and
// parses args against it.
func commandWithNameFlag(c *qt.C, args []string) *cobra.Command {
	c.Helper()

	cmd := &cobra.Command{
		Use:  "probe",
		RunE: func(*cobra.Command, []string) error { return nil },
	}
	var value string
	migratelock.RegisterNameFlag(cmd.Flags(), &value)
	c.Assert(cmd.Flags().Parse(args), qt.IsNil)
	cmd.SetContext(c.Context())
	return cmd
}

func TestResolveName_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		fromConfig bool
		value      string
		want       migratelock.Name
	}{
		{name: "nobody chose a name", value: "", want: migratelock.Name{}},
		{
			name:  "typed flag",
			args:  []string{"--" + nameFlagName, "svc_a"},
			value: "svc_a",
			want:  migratelock.Name{Value: "svc_a", Spelling: "--" + nameFlagName},
		},
		{
			name:  "surrounding space is dropped, as the migrator drops it",
			args:  []string{"--" + nameFlagName, "  svc_a  "},
			value: "  svc_a  ",
			want:  migratelock.Name{Value: "svc_a", Spelling: "--" + nameFlagName},
		},
		{
			name:       "project config",
			fromConfig: true,
			value:      "svc_b",
			want:       migratelock.Name{Value: "svc_b", Spelling: migratelock.NameConfigKey},
		},
		{
			name:       "the typed flag is the spelling when config carries a key too",
			args:       []string{"--" + nameFlagName, "svc_a"},
			fromConfig: true,
			value:      "svc_a",
			want:       migratelock.Name{Value: "svc_a", Spelling: "--" + nameFlagName},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratelock.NameRequest(commandWithNameFlag(c, test.args), test.fromConfig, "postgres://ptah@127.0.0.1:1/db")

			got, err := migratelock.ResolveName(request, test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(got.Explicit(), qt.Equals, test.want.Spelling != "")
		})
	}
}

func TestResolveName_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		fromConfig bool
		value      string
		wantErr    string
	}{
		{
			name:    "typed empty value",
			args:    []string{"--" + nameFlagName, ""},
			value:   "",
			wantErr: `--migration-lock-name must name the migration lock: .*`,
		},
		{
			name:    "typed whitespace-only value",
			args:    []string{"--" + nameFlagName, " \t "},
			value:   " \t ",
			wantErr: `--migration-lock-name must name the migration lock: .*`,
		},
		{
			name:       "project config written empty",
			fromConfig: true,
			value:      "",
			wantErr:    `migration\.migration_lock_name must name the migration lock: .*`,
		},
		{
			name:       "project config whitespace-only",
			fromConfig: true,
			value:      "   ",
			wantErr:    `migration\.migration_lock_name must name the migration lock: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratelock.NameRequest(commandWithNameFlag(c, test.args), test.fromConfig, "postgres://ptah@127.0.0.1:1/db")

			got, err := migratelock.ResolveName(request, test.value)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, migratelock.Name{})
		})
	}
}

// TestNameRequestDecide_FailurePath is the lock name's half of the dialect
// decision. A name is a request for the migration advisory lock, and a dialect
// with none cannot honor it, so the refusal reuses the timeout's wording and
// names the spelling the operator used.
func TestNameRequestDecide_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		fromConfig bool
		wantSpell  string
	}{
		{name: "typed flag", args: []string{"--" + nameFlagName, "svc_a"}, wantSpell: "--" + nameFlagName},
		{name: "project config", fromConfig: true, wantSpell: migratelock.NameConfigKey},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratelock.NameRequest(commandWithNameFlag(c, test.args), test.fromConfig, "sqlite://target.db")

			err := request.Decide("sqlite")

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantSpell+` requested the migration advisory lock, `+
				`and dialect "sqlite" has none: only postgres, mysql, mariadb, sqlserver, yugabytedb, oracle take a session advisory lock. `+
				`Remove `+test.wantSpell+` to run without a lock`))
			var unsupported *migratelock.UnsupportedError
			c.Assert(err, qt.ErrorAs, &unsupported)
			c.Assert(unsupported.Request, qt.Equals, test.wantSpell)
		})
	}
}

func TestNameRequestDecide_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		dbURL   string
		dialect string
	}{
		{name: "nobody chose a name", dbURL: "sqlite://target.db", dialect: "sqlite"},
		{name: "the dialect locks", args: []string{"--" + nameFlagName, "svc_a"}, dbURL: "postgres://ptah@127.0.0.1:1/db", dialect: "postgres"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := migratelock.NameRequest(commandWithNameFlag(c, test.args), false, test.dbURL)

			c.Assert(request.Decide(test.dialect), qt.IsNil)
		})
	}
}

// TestRequestsDecideFromURL_FailurePath holds the pair to both halves: either
// request alone refuses an unlocked dialect, and the zero value of the other
// half asks nothing.
func TestRequestsDecideFromURL_FailurePath(t *testing.T) {
	t.Run("name only", func(t *testing.T) {
		c := qt.New(t)
		requests := migratelock.Requests{
			Name: migratelock.NameRequest(
				commandWithNameFlag(c, []string{"--" + nameFlagName, "svc_a"}), false, "sqlite://target.db"),
		}

		c.Assert(requests.DecideFromURL(), qt.ErrorMatches, `--migration-lock-name requested the migration advisory lock.*`)
	})

	t.Run("timeout only", func(t *testing.T) {
		c := qt.New(t)
		requests := migratelock.Requests{
			Timeout: migratelock.Request{
				Cmd:      commandWithLockFlag(c, []string{"--migration-lock-timeout", "10s"}),
				FlagName: flagName,
				DBURL:    "sqlite://target.db",
			},
		}

		c.Assert(requests.DecideFromURL(), qt.ErrorMatches, `--migration-lock-timeout requested the migration advisory lock.*`)
	})
}

func TestRequestsDecideFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(migratelock.Requests{}.DecideFromURL(), qt.IsNil)
	c.Assert(migratelock.Requests{}.DecideConnected("sqlite"), qt.IsNil)
}
