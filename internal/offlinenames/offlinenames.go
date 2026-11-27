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

import "ptah.run/core/platform/identifier"

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
