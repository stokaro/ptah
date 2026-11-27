package mysqlgrants

import (
	"fmt"
	"strings"
)

// Account is one row of mysql.user, which holds roles and logins alike: the
// user and the host the server stores, as the server stores them.
type Account struct {
	User, Host string
}

// SplitAccount reads the text CURRENT_USER() or a DEFINER answers,
// `user@host`, into its two parts. A user name can hold an `@` and a host
// cannot, so the host starts after the last one. Text without an `@` is a user
// with an empty host.
func SplitAccount(account string) Account {
	at := strings.LastIndexByte(account, '@')
	if at < 0 {
		return Account{User: account}
	}
	return Account{User: account[:at], Host: account[at+1:]}
}

// RoleHost is the host of a role a statement names by its name alone: `%` on
// MySQL, where `CREATE ROLE r` is 'r'@'%', and empty on MariaDB, where a role
// has no host. A declaration names a role by its name, so only an account on
// this host is one a declaration can name.
func (o Options) RoleHost() string {
	if o.MariaDB {
		return ""
	}
	return "%"
}

// PrincipalName is the name a role read gives an account in a membership: its
// user alone where its host is [Options.RoleHost], since that is the role a
// declaration of that name means, and `user@host` otherwise. A login that
// shares a role's user name on another host then reads as a principal of its
// own, and its memberships are not the role's: measured on MySQL 8.4.13 and
// 26.10.0 and on MariaDB 11.8.9 and 12.3.3, after `GRANT mbig TO
// 'macct'@'localhost'` a session with the role macct active is refused the
// tables mbig reads, with Error 1142.
//
// The name of an account on [Options.RoleHost] is the user alone on both engines, and
// no two accounts there share a user, so only an account on another host can
// read as the name of another account: a role whose user name is itself
// `user@host`. [Names] refuses that read.
func (o Options) PrincipalName(account Account) string {
	if account.Host == o.RoleHost() {
		return account.User
	}
	return account.User + "@" + account.Host
}

// Names gives each account the name a statement naming that principal by its
// name alone means, and refuses two accounts that would read as one: a role
// named `app@localhost` and the login 'app'@'localhost' would share their
// grants and memberships under one name. The zero value is ready to use on
// MySQL.
//
// The name is [Options.PrincipalName], with one MariaDB refinement. There `GRANT r TO
// app` grants the role app where one exists and the login 'app'@'%' where none
// does, measured on MariaDB 11.8.9 and 12.3.3, so a login on host % reads as
// its user alone unless a role of that name was named first. A caller names
// every role before any other account.
type Names struct {
	// MariaDB is [Options.MariaDB].
	MariaDB bool
	named   map[string]Account
}

// Name returns the name of account, or an error where another account the
// same Names has named reads the same.
func (n *Names) Name(account Account) (string, error) {
	if n.named == nil {
		n.named = make(map[string]Account)
	}
	name := Options{MariaDB: n.MariaDB}.PrincipalName(account)
	if held, seen := n.named[account.User]; n.MariaDB && account.Host == "%" && (!seen || held == account) {
		name = account.User
	}
	if held, seen := n.named[name]; seen && held != account {
		return "", fmt.Errorf("the accounts %s and %s both read as the principal %q, "+
			"so their privileges and memberships cannot be told apart; rename one of them",
			quoteAccount(held), quoteAccount(account), name)
	}
	n.named[name] = account
	return name, nil
}

// quoteAccount spells an account as a statement names it.
func quoteAccount(account Account) string {
	return "'" + strings.ReplaceAll(account.User, "'", "''") + "'@'" + strings.ReplaceAll(account.Host, "'", "''") + "'"
}
