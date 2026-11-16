// Package definer decides whether a statement that writes a stored object
// again keeps the account the object runs as.
//
// On the MySQL family a routine or a view under SQL SECURITY DEFINER runs with
// its definer's privileges, and a CREATE that names no DEFINER makes the
// account that runs it the definer. Two rules ask whether that account is the
// one the object holds: the comparison's routine rule
// (migration/schemadiff/internal/compare.ValidateRoutineDefinerReplacements)
// and the MySQL planner's view rule. They ask through [Replace] and not each
// through its own comparison, because they must recognize an account the same
// way: if one rule learned a second spelling of an account and the other did
// not, one kind of object would change hands where the other is refused.
package definer

// Replacement is what writing an object again does to its definer.
//
// The zero value is [Unknown], so a verdict nobody computed refuses.
type Replacement int

const (
	// Unknown means the read recorded no definer or no connected account, so
	// nothing says whether the account changes. A caller refuses it: reading a
	// missing fact as the same account would allow the change the rule exists
	// to stop.
	Unknown Replacement = iota
	// Kept means the definer is the connected account, so the object written
	// again runs as the account the one it replaces did.
	Kept
	// Changed means the definer is another account, so the object written
	// again runs as the connected account instead.
	Changed
)

// Replace reports what writing an object again through a connection does to
// the object's definer. definer is the account the catalog records as the
// object's definer, and account is the account the connection runs as, its
// CURRENT_USER(). Either one empty is [Unknown].
//
// Both are compared as the server spells them, user@host with the host part.
// The catalog's DEFINER column and CURRENT_USER() spell one account the same
// way, and a CREATE that names no DEFINER records CURRENT_USER(), so an object
// the connected account wrote compares equal. USER() is not the same fact: it
// names the host the client connected from, not the account the server
// matched, and a role name without its host names no account.
func Replace(definer, account string) Replacement {
	switch {
	case definer == "" || account == "":
		return Unknown
	case definer == account:
		return Kept
	default:
		return Changed
	}
}
