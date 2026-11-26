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
// network against one. A pattern with a letter in it matches the name the
// server resolves an address to, which the guard cannot know either, so an
// account on an address is refused against it too; a host the guard cannot
// read as an address, such as 10.0.0.01, is a name. A pattern with a colon,
// such as fe80::%, is an IPv6 address pattern, whose letters are hexadecimal
// digits, and covers only the addresses it matches.
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
	{name: "address ahead of a name pattern", session: "dev@%.example.com", statement: "CREATE USER 'dev'@'192.168.215.1'", operation: "CREATE USER"},
	{name: "address granted ahead of a name pattern", session: "dev@%.example.com", statement: "GRANT USAGE ON *.* TO 'dev'@'10.0.0.1'", operation: "GRANT"},
	{name: "anonymous account on an address ahead of a name pattern", session: "dev@%.example.com", statement: "CREATE USER ''@'192.168.215.1'", operation: "CREATE USER"},
	{name: "netmask ahead of a name pattern", session: "dev@%.example.com", statement: "CREATE USER 'dev'@'10.0.0.0/255.0.0.0'", operation: "CREATE USER"},
	{name: "address ahead of a name with no letter", session: "dev@10.0.0.01", statement: "CREATE USER 'dev'@'10.0.0.1'", operation: "CREATE USER"},
	{name: "IPv6 address under an IPv6 pattern", session: "dev@fe80::%", statement: "CREATE USER 'dev'@'fe80::1'", operation: "CREATE USER"},
	{name: "address ahead of a host with a colon that is no address", session: "dev@fe80::1::2", statement: "CREATE USER 'dev'@'10.0.0.1'", operation: "CREATE USER"},
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
	{name: "address a numeric pattern does not match", session: "dev@192.168.215.%", statement: "CREATE USER 'dev'@'10.0.0.1'"},
	{name: "another user on an address beside a name pattern", session: "dev@%.example.com", statement: "CREATE USER 'other'@'192.168.215.1'"},
	{name: "address beside an IPv6 pattern", session: "dev@fe80::%", statement: "CREATE USER 'dev'@'192.168.215.1'"},
	{name: "IPv6 address an IPv6 pattern does not match", session: "dev@fe80::%", statement: "CREATE USER 'dev'@'2001:db8::1'"},
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

// foldedHostStatements spell the session's host with letters a server folds
// onto its own. MySQL 8.4.13 and 26.10.0 and MariaDB 10.11.19 lowercase a
// host by the Unicode simple mapping, so a dotted capital I (U+0130) is i and
// a Kelvin sign (U+212A) is k. MariaDB 11.8.9 and 12.3.3 also lowercase a
// letter's uppercase, so a long s (U+017F) is s and a dotless i (U+0131) is i.
// Measured with DROP USER on each, which drops the account of the session's
// spelling. A pattern covers a host by the same readings.
var foldedHostStatements = []struct {
	name, session, statement, operation string
}{
	{name: "dotted capital I", session: "dev@ci-host", statement: "DROP USER 'dev'@'Cİ-HOST'", operation: "DROP USER"},
	{name: "Kelvin sign", session: "dev@k-host", statement: "DROP USER 'dev'@'K-HOST'", operation: "DROP USER"},
	{name: "long s", session: "dev@ci-host", statement: "DROP USER 'dev'@'ci-hoſt'", operation: "DROP USER"},
	{name: "dotless i", session: "dev@ci-host", statement: "REVOKE ALL ON *.* FROM 'dev'@'cı-host'", operation: "REVOKE"},
	{name: "dotted capital I under a pattern", session: "dev@ci-%", statement: "CREATE USER 'dev'@'Cİ-HOST'", operation: "CREATE USER"},
	{name: "dotless i under a pattern", session: "dev@ci-%", statement: "CREATE USER 'dev'@'cı-host'", operation: "CREATE USER"},
}

func TestGuard_ConnectingAccount_FailurePathFoldedHost(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range foldedHostStatements {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				guard := boundGuard(c, dialect, utf8Session(test.session, "[]"))

				c.Assert(guard.Validate(test.statement, liftingScopes[0].scope), qt.ErrorMatches, refusal(dialect, test.operation+reaches))
			})
		}
	}
}

// foldedHostControls spell a host with letters no server folds onto the
// session's: an accented letter and a full-width letter. Measured with DROP
// USER on the same servers, which refuse each (1396).
var foldedHostControls = []struct {
	name, session, statement string
}{
	{name: "accented letter", session: "dev@ci-host", statement: "DROP USER 'dev'@'cí-host'"},
	{name: "full-width letter", session: "dev@ci-host", statement: "DROP USER 'dev'@'ｃi-host'"},
	{name: "accented letter under a pattern", session: "dev@ci-%", statement: "CREATE USER 'dev'@'cí-host'"},
}

func TestGuard_AnotherAccount_HappyPathFoldedHost(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range foldedHostControls {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				guard := boundGuard(c, dialect, utf8Session(test.session, "[]"))

				c.Assert(guard.Validate(test.statement, liftingScopes[0].scope), qt.IsNil)
			})
		}
	}
}
