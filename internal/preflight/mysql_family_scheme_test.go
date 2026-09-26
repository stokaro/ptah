package preflight_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/preflight"
)

// failingCommandRunner answers every hook with the output it holds and a
// failure, so the output reaches the caller inside the hook's error.
type failingCommandRunner struct {
	output string
}

func (r failingCommandRunner) Run(_ context.Context, _ string, _, _ []string) (string, error) {
	return r.output, errors.New("exit status 42")
}

// TestCommandHookOutputRedactsEveryMySQLFamilyScheme holds the hook output
// redaction to the schemes the connector opens.
//
// net/url refuses the tcp() form, so the password is found only by the
// MySQL-family parser, and that parser is reached only for a scheme it
// recognizes. With `mysql://` and `mariadb://` written out there, a `maria://`
// URL the migration connects with is printed with its password, twice: in the
// echoed URL and as MYSQL_PWD (stokaro/ptah#3744).
func TestCommandHookOutputRedactsEveryMySQLFamilyScheme(t *testing.T) {
	tests := []struct {
		name        string
		databaseURL string
		wantURL     string
	}{
		{
			name:        "maria",
			databaseURL: "maria://app:pw-value@tcp(db.internal:3307)/shop",
			wantURL:     "PTAH_DB_URL=maria://app@db.internal:3307/shop",
		},
		{
			name:        "upper-case mariadb",
			databaseURL: "MARIADB://app:pw-value@tcp(db.internal:3307)/shop",
			wantURL:     "PTAH_DB_URL=mariadb://app@db.internal:3307/shop",
		},
		{
			name:        "mysql",
			databaseURL: "mysql://app:pw-value@tcp(db.internal:3307)/shop",
			wantURL:     "PTAH_DB_URL=mysql://app@db.internal:3307/shop",
		},
		{
			name:        "a socket URL",
			databaseURL: "mysql+unix://app:pw-value@/run/mysqld/mysqld.sock?database=shop",
			wantURL:     "PTAH_DB_URL=mysql+unix://app@/run/mysqld/mysqld.sock?database=shop",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runner := failingCommandRunner{
				output: "PTAH_DB_URL=" + test.databaseURL + "\nMYSQL_PWD=pw-value\n",
			}

			_, err := preflight.Runner{CommandRunner: runner}.Execute(context.Background(), preflight.Options{
				Direction:   preflight.DirectionUp,
				DatabaseURL: test.databaseURL,
				Command:     "backup",
			})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), "pw-value")
			c.Assert(err.Error(), qt.Contains, test.wantURL)
			c.Assert(err.Error(), qt.Contains, "MYSQL_PWD=redacted")
		})
	}
}

// recordingCommandRunner keeps the one command a hook ran.
type recordingCommandRunner struct {
	name *string
	args *[]string
	env  *[]string
}

func (r recordingCommandRunner) Run(_ context.Context, name string, args, env []string) (string, error) {
	*r.name, *r.args, *r.env = name, args, env
	return "", nil
}

// TestMySQLDumpReachesTheServerTheURLNames points mysqldump at the server and
// the database the migration connects to, in each form of the URL. The socket
// URL names its database in the `database` parameter; read from the path, it
// would dump a database called run/mysqld/mysqld.sock from the default server.
func TestMySQLDumpReachesTheServerTheURLNames(t *testing.T) {
	tests := []struct {
		name        string
		databaseURL string
		wantAddress []string
	}{
		{
			name:        "a socket URL",
			databaseURL: "mariadb+unix://app:pw-value@/run/mysqld/mysqld.sock?database=shop",
			wantAddress: []string{"--protocol=SOCKET", "--socket", "/run/mysqld/mysqld.sock"},
		},
		{
			name:        "the driver's socket form",
			databaseURL: "mariadb://app:pw-value@unix(/run/mysqld/mysqld.sock)/shop",
			wantAddress: []string{"--protocol=SOCKET", "--socket", "/run/mysqld/mysqld.sock"},
		},
		{
			name:        "the driver's TCP form",
			databaseURL: "maria://app:pw-value@tcp(db.internal:3307)/shop",
			wantAddress: []string{"--protocol=TCP", "--host", "db.internal", "--port", "3307"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var name string
			var args, env []string
			dumpDir := c.TempDir()

			results, err := preflight.Runner{
				CommandRunner: recordingCommandRunner{name: &name, args: &args, env: &env},
				Now:           func() time.Time { return time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC) },
			}.Execute(context.Background(), preflight.Options{
				Direction:      preflight.DirectionUp,
				DatabaseURL:    test.databaseURL,
				Dialect:        "mariadb",
				CurrentVersion: 1,
				TargetVersion:  2,
				MySQLDumpDir:   dumpDir,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(results, qt.HasLen, 1)
			c.Assert(name, qt.Equals, "mysqldump")
			wantArgs := append(append([]string{"--result-file", results[0].Artifact}, test.wantAddress...), "--user", "app", "shop")
			c.Assert(args, qt.DeepEquals, wantArgs)
			c.Assert(env, qt.DeepEquals, []string{"MYSQL_PWD=pw-value"})
			c.Assert(filepath.Dir(results[0].Artifact), qt.Equals, dumpDir)
		})
	}
}
