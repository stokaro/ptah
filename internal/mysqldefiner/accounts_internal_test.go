package mysqldefiner

// White-box testing required: changesAccounts decides whether a run's account
// read still answers for its later files, and CheckSources reaches it only
// through a live server. Each statement form needs a server where the account
// it would create is a definer the deploy account may not otherwise name.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/mysqlstored"
)

func TestChangesAccounts(t *testing.T) {
	mysql := mysqlstored.Server{Dialect: platform.MySQL, VersionID: 261000}
	mariadb := mysqlstored.Server{Dialect: platform.MariaDB, VersionID: 120303}
	tests := []struct {
		server    mysqlstored.Server
		statement string
		want      bool
	}{
		{statement: "CREATE USER 'app'@'%'", want: true},
		{statement: "create user if not exists app", want: true},
		{statement: "CREATE ROLE reader", want: true},
		{statement: "RENAME USER 'a'@'%' TO 'b'@'%'", want: true},
		{statement: "/* new */ CREATE USER app", want: true},
		{statement: "DROP USER 'app'@'%'", want: false},
		{statement: "CREATE TABLE user (id int)", want: false},
		{statement: "SELECT 'CREATE USER app'", want: false},
		{statement: "RENAME TABLE a TO b", want: false},
		{statement: "CREATE", want: false},
		{server: mysql, statement: "/*!80016 CREATE USER app */", want: true},
		{server: mariadb, statement: "/*M!100100 CREATE ROLE reader */", want: true},
		{server: mariadb, statement: "/*!80016 CREATE USER app */", want: false},
		{server: mysql, statement: "/*!1000000 CREATE USER app */", want: true},
	}

	for _, test := range tests {
		t.Run(test.server.Dialect+" "+test.statement, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(changesAccounts(test.server, test.statement), qt.Equals, test.want)
		})
	}
}
