// Package offlinenames says whether a target's names can be compared without
// a connection.
//
// Some targets compare names by a collation only their catalog knows: the
// dialect's offline rules state [identifier.ComparisonCatalogUnknown], and SQL
// Server is one. Without a connection to such a target, two names may be one
// name or two, so the checks that keep a plan from losing an object word their
// refusals for that case, and a command that holds a connection to a server of
// that dialect asks it instead of the offline rules.
package offlinenames

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
)

// Unanswered reports whether semantics leave every name to a catalog nobody
// asked: they are the offline rules of a target whose names only its catalog
// can compare. Rules a connection resolved, and rules that compare names
// without a catalog, answer false.
//
// The checks that word an offline refusal and the commands that ask a server
// in place of the offline rules read this one predicate, so that a command
// asks exactly where a check would otherwise refuse.
func Unanswered(semantics identifier.Semantics) bool {
	return semantics.TableNames == identifier.ComparisonCatalogUnknown
}

// IgnoresCatalog reports whether semantics compare names without the catalog
// of dialect, whose offline rules leave every name to that catalog: they are
// neither those offline rules nor rules a connection resolved. Rules of
// another shape for such a dialect say which names are one by a rule its
// server does not follow, and [Unanswered] answers false for them, so a check
// that refuses an offline plan would let one through.
func IgnoresCatalog(semantics identifier.Semantics, dialect string) bool {
	return Unanswered(identifier.ForDialect(dialect)) &&
		!Unanswered(semantics) &&
		semantics.TableNames != identifier.ComparisonCatalogResolved
}

// RolesByCatalog reports whether dialect compares a role name under its
// catalog's rules, as it compares a table name: its offline rules leave every
// name to the catalog ([Unanswered]) and its descriptor states no rule of its
// own for a role name (descriptor.Names.Roles). SQL Server is such a dialect:
// under a case-insensitive collation the roles App and APP are one role.
//
// The comparison asks the server about role names exactly where this holds,
// and the checks that refuse two roles the target may read as one name read
// it too, so a role name a check would refuse is a name the server was asked
// about.
func RolesByCatalog(dialect string) bool {
	described, _ := platform.Lookup(dialect)
	return Unanswered(identifier.ForDialect(dialect)) && described.Names.Roles == ""
}
