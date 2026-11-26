// Package rowaccess says whether a table privilege reads a table's rows or
// changes them.
//
// The MySQL reader classifies what a grant on every database or on a database
// name pattern gives, and the security analysis classifies what a grant on a
// relation or a schema gives. Both ask [Of], so a role's read and write reach
// mean the same thing whichever grant gives it.
package rowaccess

import "strings"

// Of reports whether privilege, as a GRANT on a table names it, reads rows or
// changes them. SELECT reads; INSERT, UPDATE, DELETE and TRUNCATE change rows;
// ALL and ALL PRIVILEGES do both. Case and surrounding spaces do not matter,
// and every other privilege does neither.
func Of(privilege string) (read, write bool) {
	switch strings.ToUpper(strings.TrimSpace(privilege)) {
	case "SELECT":
		return true, false
	case "INSERT", "UPDATE", "DELETE", "TRUNCATE":
		return false, true
	case "ALL", "ALL PRIVILEGES":
		return true, true
	default:
		return false, false
	}
}
