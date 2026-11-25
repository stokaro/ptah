// Package mysqldefiner finds the definer accounts MySQL and MariaDB statements
// name, and decides before the first of them runs whether the connected
// account may name them.
//
// A CREATE or ALTER that writes DEFINER= for another account fails with ERROR
// 1227 unless the account that runs it holds the privilege to name another
// definer. MySQL and MariaDB run DDL outside a transaction, so a statement
// that fails in the middle of a schema apply or a migration run leaves the
// statements before it applied and, for a migration, the revision dirty. The
// apply paths ask [Check] before they send anything, so an account without
// the privilege is refused while nothing has changed.
package mysqldefiner

import (
	"strings"

	"ptah.run/internal/definer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlstored"
)

// Named is one statement that names a definer account.
type Named struct {
	// Object is what the statement writes, as its kind and name, such as
	// "view my_docs" or "procedure app.refresh".
	Object string
	// Definer is the account the statement names, user@host, unquoted. A
	// definer written without a host is the account with host %, as MySQL
	// reads it.
	Definer string
	// Hostless reports that the statement wrote no host. MariaDB reads such a
	// definer as a role where one of that name exists, and the scan cannot
	// tell a role from an account.
	Hostless bool
}

// Scan returns the definers the statements name on server, one entry for each
// CREATE or ALTER of a view, routine, trigger, event or package that writes a
// DEFINER clause, in statement order. The statements are read with
// [mysqlstored.Statement], so a DEFINER clause inside an executable comment
// the server runs counts, such as `/*!50013 DEFINER=... */` as mysqldump
// writes it, and one the server skips does not.
//
// A clause naming CURRENT_USER names the account that runs the statement and
// is left out. CURRENT_ROLE is an ordinary account name on MySQL and the
// current role on MariaDB, and is returned host-less, as MariaDB reads a role.
// A quoted name holding a backslash, whose meaning depends on the server's
// sql_mode, is left out, as is a statement the reader cannot follow to an
// account and an object name, and one holding an executable comment it
// cannot read.
func Scan(server mysqlstored.Server, statements []string) []Named {
	var named []Named
	for _, statement := range statements {
		if found, ok := scanStatement(server, statement); ok {
			named = append(named, found)
		}
	}
	return named
}

func scanStatement(server mysqlstored.Server, statement string) (Named, bool) {
	definition, ok, err := mysqlstored.Statement(server, statement)
	if err != nil || !ok || definition.Definer == nil || definition.Name == "" {
		return Named{}, false
	}
	account, ok := readAccount(*definition.Definer)
	if !ok {
		return Named{}, false
	}
	object := strings.ToLower(definition.Kind) + " " + definition.Name
	return Named{Object: object, Definer: account.name, Hostless: account.hostless}, true
}

// account is a definer as a statement writes it.
type account struct {
	// name is user@host, unquoted, with host % where none was written.
	name string
	// hostless reports that no host was written.
	hostless bool
}

// readAccount reads the account clause names. CURRENT_USER, and an account
// it cannot read, is reported as not found.
func readAccount(clause mysqlstored.Definer) (account, bool) {
	user, ok := accountPart(clause.User)
	if !ok || clause.User.Type == lexer.TokenIdentifier && strings.EqualFold(user, "CURRENT_USER") {
		return account{}, false
	}
	host, hostless := "%", true
	if clause.Host != nil {
		host, ok = accountPart(*clause.Host)
		if !ok {
			return account{}, false
		}
		hostless = false
	}
	if user == "" || host == "" {
		return account{}, false
	}
	return account{name: user + "@" + host, hostless: hostless}, true
}

// accountPart returns the user or host a token spells, and false for a quoted
// string holding a backslash: under the default sql_mode the server reads it
// as an escape, and under NO_BACKSLASH_ESCAPES as itself.
func accountPart(token lexer.Token) (string, bool) {
	if token.Type == lexer.TokenString && strings.Contains(token.Value, `\`) {
		return "", false
	}
	return mysqlstored.Unquote(token), true
}

// sameAccount reports whether a named definer is the connected account.
func sameAccount(named, account string) bool {
	return definer.Replace(named, account) == definer.Kept
}
