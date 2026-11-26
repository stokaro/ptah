// Package offlinenames marks the refusals that a connection to the target
// would have answered.
//
// Some targets compare names by a collation only their catalog knows: the
// dialect's offline rules state [identifier.ComparisonCatalogUnknown], and SQL
// Server is one. Without a connection to such a target, two names may be one
// name or two, and the checks that keep a plan from losing an object refuse
// them. The checks live below the command layer and do not know how a command
// is given a connection, so they mark the refusal with [Mark]. A command that
// can be given one reads the mark with [Marked] and says how.
package offlinenames

import (
	"errors"

	"ptah.run/core/platform/identifier"
)

// Unanswered reports whether semantics leave every name to a catalog nobody
// asked: they are the offline rules of a target whose names only its catalog
// can compare. Rules a connection resolved, and rules that compare names
// without a catalog, answer false.
//
// The checks that mark a refusal and the commands that ask a server in place
// of the offline rules read this one predicate, so that a command asks
// exactly where a check would otherwise refuse.
func Unanswered(semantics identifier.Semantics) bool {
	return semantics.TableNames == identifier.ComparisonCatalogUnknown
}

// Mark returns err marked as a refusal that a connection to the target would
// have answered. It returns nil for a nil err. The marked error reads as err,
// and [errors.Is] and [errors.As] reach err through it.
func Mark(err error) error {
	if err == nil {
		return nil
	}
	return &marked{err: err}
}

// Marked reports whether err, or an error it wraps, was returned by [Mark].
func Marked(err error) bool {
	var refusal *marked
	return errors.As(err, &refusal)
}

type marked struct {
	err error
}

func (m *marked) Error() string { return m.err.Error() }

func (m *marked) Unwrap() error { return m.err }
