//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// A MySQL or MariaDB history that drops or strips the account the run
// connects as is refused on a server the run owns and under trust, and the
// account is still on the server afterwards. The same history naming another
// account replays there, so the refusal is about the account and not about
// account statements.

// connectingAccountEngines are the servers the tests below run on, each with
// the scheme its dev URL is spelled with.
var connectingAccountEngines = []struct {
	name   string
	engine dbtarget.Engine
	scheme string
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin, scheme: "mysql"},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin, scheme: "mariadb"},
	{name: "MariaDB 11.8", engine: dbtarget.MariaDB118, scheme: "mariadb"},
}

// connectingAccountFixture is a dev database on engine reached with an
// account of its own that holds every privilege, and another account, both
// dropped when the test ends.
type connectingAccountFixture struct {
	family  mysqlFamilyServer
	account string
	other   string
	devURL  string
}

func newConnectingAccountFixture(c *qt.C, engine dbtarget.Engine, scheme string) connectingAccountFixture {
	c.Helper()
	family := newMySQLFamilyServer(c, engine)
	suffix := time.Now().UnixNano() % 1_000_000_000
	account := fmt.Sprintf("ptah_acct_%d", suffix)
	other := fmt.Sprintf("ptah_other_%d", suffix)
	password := account + "_pw"
	createMySQLUser(c, c.Context(), family.admin, account, password)
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), family.admin, account) })
	createMySQLUser(c, c.Context(), family.admin, other, other+"_pw")
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), family.admin, other) })
	_, err := family.admin.ExecContext(c.Context(), fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION", account))
	c.Assert(err, qt.IsNil)
	database := family.database(c, "acct")
	devURL := (&url.URL{Scheme: scheme, User: url.UserPassword(account, password), Host: family.config.Addr, Path: "/" + database}).String()
	return connectingAccountFixture{family: family, account: account, other: other, devURL: devURL}
}

// accounts counts the accounts named user on the host % that may still
// create users, which an account dropped or stripped of its privileges is
// not.
func (fx connectingAccountFixture) accounts(c *qt.C, user string) int {
	c.Helper()
	var count int
	c.Assert(fx.family.admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = '%' AND Create_user_priv = 'Y'", user).Scan(&count), qt.IsNil)
	return count
}

// validate replays a directory holding the statements of history, under trust and on a server
// declared disposable, with ptah migrations validate.
func (fx connectingAccountFixture) validate(c *qt.C, history string) (string, error) {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	body := "CREATE TABLE items (id bigint PRIMARY KEY);\n" + history + ";\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_account.sql"), []byte(body), 0o600), qt.IsNil)
	_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	return runPtahNativeWithError("migrations", "validate", "--dir", dir, "--dir-format", "atlas",
		"--dev-url", fx.devURL, "--trust-replay-sql")
}

// TestMigrationsValidateRefusesTheConnectingAccountE2E replays a history that
// reaches the account the dev URL connects as, by a spelling the server reads
// as that account and by the name only the session read tells the guard. The
// refusal offers no remedy, since neither owning the server nor trust lifts
// it, and the account keeps its privileges.
func TestMigrationsValidateRefusesTheConnectingAccountE2E(t *testing.T) {
	histories := []struct {
		name, history, operation string
	}{
		{name: "DROP USER CURRENT_USER", history: "DROP USER CURRENT_USER", operation: "DROP USER"},
		{name: "account by name", history: "DROP USER '{account}'@'%'", operation: "DROP USER"},
		{name: "every privilege revoked", history: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM CURRENT_USER", operation: "REVOKE"},
	}
	for _, engine := range connectingAccountEngines {
		for _, test := range histories {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newConnectingAccountFixture(c, engine.engine, engine.scheme)
				history := strings.ReplaceAll(test.history, "{account}", fx.account)

				out, err := fx.validate(c, history)

				c.Assert(err, qt.ErrorMatches, `(?s).* migration replay rejects `+test.operation+
					` that reaches the account the run connects as because its effects cannot be confined to the `+
					`disposable database realm\nSQL: `+regexp.QuoteMeta(history), qt.Commentf("%s", out))
				c.Assert(fx.accounts(c, fx.account), qt.Equals, 1)
			})
		}
	}
}

// TestMigrationsValidateReplaysAnotherAccountE2E is the control for the test
// above: the same statements naming another account replay, and that account
// is gone.
func TestMigrationsValidateReplaysAnotherAccountE2E(t *testing.T) {
	for _, engine := range connectingAccountEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newConnectingAccountFixture(c, engine.engine, engine.scheme)

			out, err := fx.validate(c, fmt.Sprintf("REVOKE ALL PRIVILEGES, GRANT OPTION FROM '%[1]s'@'%%';\nDROP USER '%[1]s'@'%%'", fx.other))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(fx.accounts(c, fx.account), qt.Equals, 1)
			var others int
			c.Assert(fx.family.admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM mysql.user WHERE User = ?", fx.other).Scan(&others), qt.IsNil)
			c.Assert(others, qt.Equals, 0)
		})
	}
}
