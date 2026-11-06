package devclean

import (
	"ptah.run/catalog"
	"ptah.run/dbschema"
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
// same refusals it lifts for a replay. The two differ in a refusal's words and
// in [BaselineGuard.WithSchemaStateReturned]. A YDB baseline holds no permission
// on the database root at all; see the rehearsal's own omission of them.
type BaselineGuard struct {
	replay *ReplayGuard
}

// NewBaselineGuard is the guard for the baseline of a plan rehearsal on the
// dev database reached through info.
func NewBaselineGuard(info catalog.ServerInfo) *BaselineGuard {
	return newBaselineGuard(NewDevReplayGuard(info))
}

// NewBaselineGuardFor is [NewBaselineGuard] for the dev database conn reaches,
// with the guard [NewDevReplayGuardFor] gives it.
func NewBaselineGuardFor(conn *dbschema.DatabaseConnection) *BaselineGuard {
	return newBaselineGuard(NewDevReplayGuardFor(conn))
}

func newBaselineGuard(guard *ReplayGuard) *BaselineGuard {
	replay := *guard
	replay.baseline = true
	return &BaselineGuard{replay: &replay}
}

// WithSchemaStateReturned returns a copy of the guard for a dev database whose
// every reset returns the schemas it keeps to what the claim recorded, as
// [Baseline.ReturnsSchemaState] answers.
//
// The copy accepts a comment on a schema and a privilege on a schema. A target
// holds them often, on public or on its own schemas, and the rehearsal needs
// them on the dev database: a native plan is made again there, and refused if
// it is not the target's. Written there, they end with the run. A schema the
// reset keeps goes back to its claimed comment and access list, and any other
// schema is dropped. A dev database whose reset does not return them stays
// refused. A migration replay takes the comment on the same terms, whoever wrote
// it, and stays refused on the privilege; see
// [ReplayGuard.WithSchemaCommentReturned].
func (g *BaselineGuard) WithSchemaStateReturned(returned bool) *BaselineGuard {
	return &BaselineGuard{replay: g.replay.WithSchemaStateReturned(returned)}
}

// ValidateStatement refuses a baseline statement whose effects cannot be
// confined to the dev database's realm, as [ReplayGuard.ValidateStatement]
// does. The refusal says it is the baseline's, since a reader of a migration
// replay's refusal would look for a migration file, and it ends with the
// remedy when owning the server would lift it.
func (g *BaselineGuard) ValidateStatement(stmt string) error {
	return g.replay.ValidateStatement(stmt)
}
