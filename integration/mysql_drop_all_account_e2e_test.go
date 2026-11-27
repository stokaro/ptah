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
