package mysqlreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// networkSessionStatements each create an account that takes new connections
// of the session's account from it: MySQL reads a host written with a
// netmask or a prefix length as an address, and tries an account on an
// address before one on a pattern or a name. Measured on MySQL 8.4.13 from
// 192.168.215.14, each of the first three refuses the next login of
// dev@'192.168.215.%', and the fourth does not: a pattern does not say which
// addresses the session's connections come from, so the guard refuses every
// network against one.
var networkSessionStatements = []struct {
	name, session, statement, operation string
}{
	{name: "netmask ahead of a pattern", session: "dev@192.168.215.%", statement: "CREATE USER 'dev'@'192.168.0.0/255.255.0.0'", operation: "CREATE USER"},
	{name: "prefix length ahead of a pattern", session: "dev@192.168.215.%", statement: "CREATE USER 'dev'@'192.168.0.0/16'", operation: "CREATE USER"},
	{name: "anonymous account on a netmask ahead of a pattern", session: "dev@192.168.215.%", statement: "CREATE USER ''@'192.168.0.0/255.255.0.0'", operation: "CREATE USER"},
	{name: "network the pattern does not match", session: "dev@192.168.215.%", statement: "CREATE USER 'dev'@'10.0.0.0/8'", operation: "CREATE USER"},
	{name: "netmask ahead of a name", session: "dev@db.example", statement: "GRANT USAGE ON *.* TO 'dev'@'10.0.0.0/255.0.0.0'", operation: "GRANT"},
	{name: "address ahead of a name", session: "dev@db.example", statement: "CREATE USER 'dev'@'10.1.2.3'", operation: "CREATE USER"},
	{name: "netmask holding the address", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.0.0/255.255.0.0'", operation: "CREATE USER"},
	{name: "prefix length holding the address", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.215.0/24'", operation: "CREATE USER"},
	{name: "netmask under which the address is the network's", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.215.0/255.255.255.128'", operation: "CREATE USER"},
	{name: "anonymous account on a netmask holding the address", session: "dev@192.168.215.3", statement: "CREATE USER ''@'192.168.215.0/255.255.255.0'", operation: "CREATE USER"},
	{name: "network that cannot be read", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.215.x/24'", operation: "CREATE USER"},
	{name: "netmask that cannot be read", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.215.0/255.255.x.0'", operation: "CREATE USER"},
	{name: "prefix length that cannot be read", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.0.0/40'", operation: "CREATE USER"},
	{name: "netmask holding a mapped address", session: "dev@::ffff:192.168.215.3", statement: "CREATE USER 'dev'@'192.168.0.0/255.255.0.0'", operation: "CREATE USER"},
	{name: "address on the session's network", session: "dev@10.0.0.0/255.0.0.0", statement: "CREATE USER 'dev'@'10.1.2.3'", operation: "CREATE USER"},
	{name: "network inside the session's network", session: "dev@10.0.0.0/255.0.0.0", statement: "CREATE USER 'dev'@'10.1.0.0/16'", operation: "CREATE USER"},
}

func TestGuard_ConnectingAccount_FailurePathNetwork(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range networkSessionStatements {
			for _, scope := range accountScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(test.session, "[]"))

					err := guard.Validate(test.statement, scope.scope)

					c.Assert(err, qt.ErrorMatches, refusal(dialect, test.operation+reaches))
				})
			}
		}
	}
}

// networkSessionControls create an account on a network or an address that
// the session's connections do not come from.
var networkSessionControls = []struct {
	name, session, statement string
}{
	{name: "netmask without the address", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'10.0.0.0/255.0.0.0'"},
	{name: "prefix length without the address", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.216.0/24'"},
	{name: "netmask under which the address is another network's", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.215.128/255.255.255.128'"},
	{name: "network of another address family", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'::/0'"},
	{name: "netmask against an address of another family", session: "dev@fe80::1", statement: "CREATE USER 'dev'@'10.0.0.0/255.0.0.0'"},
	{name: "network address with bits outside its netmask", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.215.77/255.255.255.0'"},
	{name: "network address with bits outside its prefix length", session: "dev@192.168.215.3", statement: "CREATE USER 'dev'@'192.168.215.77/24'"},
	{name: "pattern that reads as a zoned address", session: "dev@db.example", statement: "CREATE USER 'dev'@'fe80::%1'"},
	{name: "address the pattern does not match", session: "dev@192.168.215.%", statement: "CREATE USER 'dev'@'10.1.2.3'"},
	{name: "address off the session's network", session: "dev@10.0.0.0/255.0.0.0", statement: "CREATE USER 'dev'@'11.1.2.3'"},
	{name: "name beside the session's network", session: "dev@10.0.0.0/255.0.0.0", statement: "CREATE USER 'dev'@'db.example'"},
	{name: "another user on a network", session: "dev@192.168.215.%", statement: "CREATE USER 'other'@'192.168.0.0/16'"},
	{name: "another name", session: "dev@db.example", statement: "CREATE USER 'dev'@'other.example'"},
}

func TestGuard_AnotherAccount_HappyPathNetwork(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range networkSessionControls {
			for _, scope := range liftingScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(test.session, "[]"))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.IsNil)
				})
			}
		}
	}
}

// MariaDB keeps a space at the start of a name, so its session can run as
// ' dev'@'%', which only MariaDB's reading of a statement can name: MySQL's
// trims the space away.
func TestGuard_ConnectingAccount_FailurePathMariaDBReading(t *testing.T) {
	statements := []struct {
		name, statement, operation string
	}{
		{name: "dropped with a space at the end", statement: "DROP USER ' dev '@'%'", operation: "DROP USER" + reaches},
		{name: "revoked from", statement: "REVOKE ALL ON *.* FROM ' dev'", operation: "REVOKE" + reaches},
		{name: "dropped with a space at the end of the host", statement: "DROP USER ' dev'@'% '", operation: "DROP USER" + reaches},
		{name: "shadowed on a narrower host", statement: "CREATE USER ' dev'@'localhost'", operation: "CREATE USER" + reaches},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range statements {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				guard := boundGuard(c, dialect, utf8Session(" dev@%", "[]"))

				c.Assert(guard.Validate(test.statement, liftingScopes[0].scope), qt.ErrorMatches, refusal(dialect, test.operation))
			})
		}
	}
}

// The account without the space is another one to MariaDB, and to MySQL.
func TestGuard_AnotherAccount_HappyPathMariaDBReading(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			guard := boundGuard(c, dialect, utf8Session(" dev@%", "[]"))

			c.Assert(guard.Validate("DROP USER 'dev'@'%'", liftingScopes[0].scope), qt.IsNil)
		})
	}
}

// CURRENT_USER() answers dev@ for an account on an empty host. MariaDB reads
// an empty host as %, and MySQL keeps it as a host of its own, which matches
// every connection after %. The guard reads both as %, so a statement that
// spells the empty host names the account.
func TestGuard_ConnectingAccount_FailurePathEmptyHost(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			guard := boundGuard(c, dialect, utf8Session("dev@", "[]"))

			c.Assert(guard.Validate("DROP USER 'dev'@''", liftingScopes[0].scope), qt.ErrorMatches, refusal(dialect, "DROP USER"+reaches))
		})
	}
}
