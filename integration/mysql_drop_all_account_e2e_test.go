//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestMySQLFamilyDropAllAccountNameE2E runs `ptah db drop-all` as an account
// holding ALL PRIVILEGES ON *.* whose user name holds a character the
// privilege views spell as is: an `@`, which the account's CURRENT_USER()
// spelling also uses between the user and the host, and an apostrophe, which
// QUOTE() would escape and the views do not. The cleanup proves it may see
// every object from the account's global privileges, so it must find that
// account's rows; the server's catalog, read through the admin connection,
// says whether the table went.
func TestMySQLFamilyDropAllAccountNameE2E(t *testing.T) {
	users := []struct{ name, user string }{
		{name: "at sign", user: "dropq@x"},
		{name: "apostrophe", user: "drop'q"},
	}
	for _, engine := range roleInheritEngines {
		for _, user := range users {
			t.Run(engine.name+"/"+user.name, func(t *testing.T) {
				c := qt.New(t)
				server := newMySQLFamilyServer(c, engine.engine)
				database := server.database(c, "drop_account")
				name := fmt.Sprintf("%s%d", user.user, time.Now().UnixNano()%1_000_000_000)
				account := granteeIdentifier(name) + "@'%'"
				c.Cleanup(func() {
					_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
					c.Check(err, qt.IsNil)
				})
				for _, statement := range []string{
					"CREATE USER " + account + " IDENTIFIED BY 'drop_account_pw'",
					"GRANT ALL PRIVILEGES ON *.* TO " + account,
					"CREATE TABLE `" + database + "`.items (id int PRIMARY KEY)",
				} {
					_, err := server.admin.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
				}
				target := (&url.URL{
					Scheme: engine.scheme,
					User:   url.UserPassword(name, "drop_account_pw"),
					Host:   server.config.Addr,
					Path:   "/" + database,
				}).String()

				result := runPtahIn(c, "db", "drop-all", "--db-url", target, "--auto-approve")

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(server.tables(c, database), qt.Equals, "")
			})
		}
	}
}

// TestMySQLFamilyDropAllCaseTwinAccountE2E runs `ptah db drop-all` as an
// account holding privileges on its database and SELECT on the mysql
// database, beside an account whose user name differs only in case and holds
// ALL PRIVILEGES ON *.*. SELECT on mysql makes the privilege views list every
// account's rows, and they compare a grantee without regard to case: measured
// on MySQL 26.10.0 and MariaDB 12.3.3, `grantee = 'TWINX'@'%'` matches the
// twin's rows too, and an account without SELECT on mysql sees its own rows
// alone. So the cleanup matches the session's account byte for byte: read
// under the views' collation, the twin's global privileges would prove a
// visibility the session does not have, and the cleanup would drop what it
// cannot see in full. The refusal, and the table still in place, say it read
// its own rows.
func TestMySQLFamilyDropAllCaseTwinAccountE2E(t *testing.T) {
	for _, engine := range roleInheritEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, engine.engine)
			database := server.database(c, "drop_twin")
			suffix := time.Now().UnixNano() % 1_000_000_000
			session := granteeIdentifier(fmt.Sprintf("DROPTWIN%d", suffix)) + "@'%'"
			twin := granteeIdentifier(fmt.Sprintf("droptwin%d", suffix)) + "@'%'"
			c.Cleanup(func() {
				_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+session+", "+twin)
				c.Check(err, qt.IsNil)
			})
			for _, statement := range []string{
				"CREATE USER " + session + " IDENTIFIED BY 'drop_twin_pw'",
				"CREATE USER " + twin + " IDENTIFIED BY 'drop_twin_pw'",
				"GRANT ALL PRIVILEGES ON `" + database + "`.* TO " + session,
				"GRANT SELECT ON mysql.* TO " + session,
				"GRANT ALL PRIVILEGES ON *.* TO " + twin,
				"CREATE TABLE `" + database + "`.items (id int PRIMARY KEY)",
			} {
				_, err := server.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			target := (&url.URL{
				Scheme: engine.scheme,
				User:   url.UserPassword(fmt.Sprintf("DROPTWIN%d", suffix), "drop_twin_pw"),
				Host:   server.config.Addr,
				Path:   "/" + database,
			}).String()

			result := runPtahIn(c, "db", "drop-all", "--db-url", target, "--auto-approve")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, `refusing to clean database "`+database+`": global`)
			c.Assert(server.tables(c, database), qt.Equals, "items")
		})
	}
}
