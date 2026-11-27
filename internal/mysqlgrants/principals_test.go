package mysqlgrants_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlgrants"
)

// TestSplitAccount reads CURRENT_USER() at its last @: a user name can hold
// one and a host cannot.
func TestSplitAccount(t *testing.T) {
	tests := []struct {
		name, account string
		want          mysqlgrants.Account
	}{
		{name: "user and host", account: "app@localhost", want: mysqlgrants.Account{User: "app", Host: "localhost"}},
		{name: "an @ in the user name", account: "rvq@x@%", want: mysqlgrants.Account{User: "rvq@x", Host: "%"}},
		{name: "an empty host", account: "app@", want: mysqlgrants.Account{User: "app"}},
		{name: "no host", account: "app", want: mysqlgrants.Account{User: "app"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlgrants.SplitAccount(test.account), qt.Equals, test.want)
		})
	}
}

// TestPrincipalName names an account by its user where its host is the one
// CREATE ROLE gives a role, and by user@host elsewhere. A login that shares a
// role's user name on another host must not read as the role.
func TestPrincipalName(t *testing.T) {
	tests := []struct {
		name    string
		account mysqlgrants.Account
		mariaDB bool
		want    string
	}{
		{name: "MySQL role", account: mysqlgrants.Account{User: "macct", Host: "%"}, want: "macct"},
		{name: "MySQL login on another host", account: mysqlgrants.Account{User: "macct", Host: "localhost"}, want: "macct@localhost"},
		{name: "MySQL account on the empty host", account: mysqlgrants.Account{User: "macct"}, want: "macct@"},
		{name: "MariaDB role", account: mysqlgrants.Account{User: "macct"}, mariaDB: true, want: "macct"},
		{name: "MariaDB login on any host", account: mysqlgrants.Account{User: "macct", Host: "%"}, mariaDB: true, want: "macct@%"},
		{name: "an @ in the user name", account: mysqlgrants.Account{User: "r@x", Host: "localhost"}, want: "r@x@localhost"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlgrants.Options{MariaDB: test.mariaDB}.PrincipalName(test.account), qt.Equals, test.want)
		})
	}
}

// TestNames_HappyPath names a MariaDB login on host % by its user alone, as
// `GRANT r TO app` grants it, unless a role of that name was named first.
func TestNames_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		mariaDB  bool
		accounts []mysqlgrants.Account
		want     []string
	}{
		{
			name:     "MariaDB role and a login sharing its user",
			mariaDB:  true,
			accounts: []mysqlgrants.Account{{User: "macct"}, {User: "macct", Host: "%"}, {User: "macct"}},
			want:     []string{"macct", "macct@%", "macct"},
		},
		{
			name:     "MariaDB login with no role of its name",
			mariaDB:  true,
			accounts: []mysqlgrants.Account{{User: "app", Host: "%"}, {User: "app", Host: "localhost"}, {User: "app", Host: "%"}},
			want:     []string{"app", "app@localhost", "app"},
		},
		{
			name:     "MySQL account on host %",
			accounts: []mysqlgrants.Account{{User: "app", Host: "%"}, {User: "app", Host: "localhost"}},
			want:     []string{"app", "app@localhost"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			names := &mysqlgrants.Names{MariaDB: test.mariaDB}
			got := make([]string, 0, len(test.accounts))
			for _, account := range test.accounts {
				name, err := names.Name(account)
				c.Assert(err, qt.IsNil)
				got = append(got, name)
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestNames_FailurePath refuses a role whose user name is the name a login on
// another host reads as, since the two would share their grants.
func TestNames_FailurePath(t *testing.T) {
	c := qt.New(t)
	names := &mysqlgrants.Names{}
	_, err := names.Name(mysqlgrants.Account{User: "app@localhost", Host: "%"})
	c.Assert(err, qt.IsNil)

	name, err := names.Name(mysqlgrants.Account{User: "app", Host: "localhost"})

	c.Assert(err, qt.ErrorMatches, `the accounts 'app@localhost'@'%' and 'app'@'localhost' both read as the principal "app@localhost", .*`)
	c.Assert(name, qt.Equals, "")
}

func TestRoleHost(t *testing.T) {
	c := qt.New(t)
	c.Assert([]string{mysqlgrants.Options{}.RoleHost(), mysqlgrants.Options{MariaDB: true}.RoleHost()}, qt.DeepEquals, []string{"%", ""})
}

// TestNames_FailurePathMariaDBLoginBeforeRole refuses a MariaDB role named
// after a login that took the bare name: a caller names every role first, and
// a role that arrives later would share the login's name.
func TestNames_FailurePathMariaDBLoginBeforeRole(t *testing.T) {
	c := qt.New(t)
	names := &mysqlgrants.Names{MariaDB: true}
	_, err := names.Name(mysqlgrants.Account{User: "app", Host: "%"})
	c.Assert(err, qt.IsNil)

	name, err := names.Name(mysqlgrants.Account{User: "app"})

	c.Assert(err, qt.ErrorMatches, `the accounts 'app'@'%' and 'app'@'' both read as the principal "app", .*`)
	c.Assert(name, qt.Equals, "")
}
