package mysqlgrants

import (
	"slices"
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

// Grantee is the account as the information_schema privilege views name it,
// 'user'@'host': each part between single quotes and neither one escaped.
// Measured on MySQL 8.4.13 and 26.10.0 and on MariaDB 11.8.9 and 12.3.3,
// USER_PRIVILEGES names 'rvq@x'@'%' so, and an account whose user is o'k as
// 'o'k'@'%', where QUOTE() would write 'o\'k'.
func (a Account) Grantee() string {
	return "'" + a.User + "'@'" + a.Host + "'"
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
// `user@host`. [Names] records that read as a collision.
func (o Options) PrincipalName(account Account) string {
	if account.Host == o.RoleHost() {
		return account.User
	}
	return account.User + "@" + account.Host
}

// Names gives each account the name a statement naming that principal by its
// name alone means, and records two accounts that read as one: a role named
// `app@localhost` and the login 'app'@'localhost' would share their grants
// and memberships under one name. The zero value is ready to use on MySQL.
//
// The name is [Options.PrincipalName], with one MariaDB refinement. There `GRANT r TO
// app` grants the role app where one exists and the login 'app'@'%' where none
// does, measured on MariaDB 11.8.9 and 12.3.3, so a login on host % reads as
// its user alone unless a role of that name was named first. A caller names
// every role before any other account.
type Names struct {
	// MariaDB is [Options.MariaDB].
	MariaDB   bool
	named     map[string]Account
	colliding map[string][]Account
}

// Name returns the name of account. Where another account the same Names has
// named reads the same, the name is recorded as one [Names.Collisions]
// reports, and it names both accounts.
func (n *Names) Name(account Account) string {
	if n.named == nil {
		n.named = make(map[string]Account)
	}
	name := Options{MariaDB: n.MariaDB}.PrincipalName(account)
	if held, seen := n.named[account.User]; n.MariaDB && account.Host == "%" && (!seen || held == account) {
		name = account.User
	}
	held, seen := n.named[name]
	if !seen {
		n.named[name] = account
		return name
	}
	if held != account && !slices.Contains(n.colliding[name], account) {
		if n.colliding == nil {
			n.colliding = make(map[string][]Account)
		}
		if len(n.colliding[name]) == 0 {
			n.colliding[name] = []Account{held}
		}
		n.colliding[name] = append(n.colliding[name], account)
	}
	return name
}

// Account returns the first account [Names.Name] gave name, and false where
// it gave name to none.
func (n *Names) Account(name string) (Account, bool) {
	account, named := n.named[name]
	return account, named
}

// Collision is a name two accounts or more read as, so their privileges and
// memberships cannot be told apart.
type Collision struct {
	// Name is the principal name the accounts share.
	Name string
	// Accounts are the accounts, in the order they were named.
	Accounts []Account
}

// Collisions returns each name more than one account read as, sorted by name.
func (n *Names) Collisions() []Collision {
	collisions := make([]Collision, 0, len(n.colliding))
	for name, accounts := range n.colliding {
		collisions = append(collisions, Collision{Name: name, Accounts: slices.Clone(accounts)})
	}
	slices.SortFunc(collisions, func(a, b Collision) int { return strings.Compare(a.Name, b.Name) })
	return collisions
}
