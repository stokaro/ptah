// Package grantee says when two names in the grantee position of a GRANT,
// REVOKE or DENY name one principal. The grant comparison, the SQL Server
// DENY planner and the wide-grant cover each match a grant the database holds
// against one a schema declares, and they read one predicate, so the three
// cannot come to disagree about who a grant is to.
package grantee

import "strings"

// Public is the grantee every principal holds a privilege through.
const Public = "PUBLIC"

// Key is name as a grant is matched by it: PUBLIC in any case keys as
// [Public], and every other name keys as itself, trimmed.
//
// A statement may write PUBLIC in any case, and each catalog that has it
// reports one spelling: SQL Server's sys.database_principals names the role
// public, and MariaDB and PostgreSQL report PUBLIC. Keyed as written, a
// grant or a DENY to PUBLIC read from one side never matches the other, and
// every plan states it again. PostgreSQL refuses a role of that name as
// reserved, and SQL Server's public is a fixed role of every database, so no
// role there is a different principal under another case of it.
func Key(name string) string {
	name = strings.TrimSpace(name)
	if strings.EqualFold(name, Public) {
		return Public
	}
	return name
}
