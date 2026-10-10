package devclean

import (
	"ptah.run/catalog"
)

// BaselineGuard holds the baseline a plan rehearsal writes, to recreate the
// target's current schema in the dev database, to the realm a migration replay
// on the same dev server is held to.
//
// The baseline is DDL Ptah derives from the target, but deriving a statement
// from the target does not confine its effects: a routine or a trigger body
// can write outside the dev database when it runs, a comment on an extension
// is not restored by the cleanup, and a role or a grant outlives it. So every
// refusal of [NewDevReplayGuard] holds, and a server the run owns lifts the
// same refusals it lifts for a replay. The two differ only in a refusal's
// words. A YDB baseline holds no permission on the database root at all; see
// the rehearsal's own omission of them.
type BaselineGuard struct {
	replay *ReplayGuard
}

// NewBaselineGuard is the guard for the baseline of a plan rehearsal on the
// dev database reached through info.
func NewBaselineGuard(info catalog.ServerInfo) *BaselineGuard {
	replay := *NewDevReplayGuard(info)
	replay.baseline = true
	return &BaselineGuard{replay: &replay}
}

// ValidateStatement refuses a baseline statement whose effects cannot be
// confined to the dev database's realm, as [ReplayGuard.ValidateStatement]
// does. The refusal says it is the baseline's, since a reader of a migration
// replay's refusal would look for a migration file, and it ends with the
// remedy when owning the server would lift it.
func (g *BaselineGuard) ValidateStatement(stmt string) error {
	return g.replay.ValidateStatement(stmt)
}
