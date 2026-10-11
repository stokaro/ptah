package lint

import (
	"fmt"
	"slices"
	"strings"
)

// The ON family is an allowlist, which is what separates it from every other
// family. Its rules are the engine owners': ON101 and ON103 the PostgreSQL
// owner's, ON102 the MySQL owner's. This file holds the mode they run in,
// which is this package's policy.
//
// The rest of this package is a blocklist: a rule fires on a hazard somebody
// wrote a rule for, and a statement nobody anticipated -- a DDL form no rule
// covers, hand-written SQL, a spelling a dialect grew last year -- passes in
// silence. Silence there reads exactly like approval, which is fine for advice
// and useless for a guarantee.
//
// So its rules invert it. Each proves a statement belongs to a set
// measured on the engine, and reports everything else, including everything it
// does not recognize. A form that arrives tomorrow is refused until somebody
// measures it, which is the only way the answer can be a guarantee rather than
// a hope.
//
// # What the mode proves, exactly
//
// Every statement in this migration is, on this engine at this version, a
// catalog-only change or an operation whose lock does not conflict with reads
// and writes for longer than it takes to acquire.
//
// It deliberately does not say "and scans no rows". PostgreSQL's
// `VALIDATE CONSTRAINT` scans the table under SHARE UPDATE EXCLUSIVE, which
// blocks neither readers nor writers -- measured on PostgreSQL 18.6 through
// pg_locks -- and it is the second half of the form Ptah generates to make a
// constraint addition online at all. A property that refused it would refuse
// the repair along with the hazard. What a scan costs depends on how many rows
// there are, which is the first of the things below that nothing here can see.
//
// # What it does not prove, and must not be read as proving
//
// Table size, concurrent load, replication lag and application rollout are all
// outside what any reading of the SQL can answer. The lock a statement takes
// is a property of the statement; an outage is a property of the system around
// it. On PostgreSQL the gap between the two is usually the lock queue: an
// instant ADD COLUMN still takes ACCESS EXCLUSIVE briefly, and if anything
// holds a conflicting lock the ALTER waits -- with every later reader and
// writer of that table waiting behind it. That is why the apply-time mode
// requires a lock timeout rather than only reading the SQL.

// onlineModeDialects are the dialects the online mode covers, in the order a
// refusal names them.
//
// The list is short because the guarantee is: each of these has a measurement
// behind it in this package. An engine is added here when somebody measures
// it, not when its documentation claims online DDL.
func onlineModeDialects() []string {
	return []string{"postgres", "mysql", "mariadb"}
}

// onlineModeCovers reports whether the mode has a measured answer for the
// dialect.
func onlineModeCovers(dialect string) bool {
	return slices.Contains(onlineModeDialects(), dialect)
}

// ValidateOnlineDialect refuses the online mode on an engine it has no
// measurement for, naming the ones it covers.
//
// It is exported because the refusal has two callers: the configuration, which
// can check the dialect it names itself, and the apply-time gate, which learns
// the engine from the connection. One predicate for both, so a policy accepted
// offline cannot be refused at apply for a different reason than it would have
// been refused here.
func ValidateOnlineDialect(dialect string) error {
	if onlineModeCovers(dialect) {
		return nil
	}
	return fmt.Errorf(
		"online mode covers %s; it is refused on %q rather than reporting a guarantee nothing "+
			"here has measured on that engine",
		strings.Join(onlineModeDialects(), ", "), dialect,
	)
}

// OnlineFamily is the identifier prefix of the rules the online mode reports
// under. The apply-time gate blocks on it when the mode is selected.
const OnlineFamily = "ON"
