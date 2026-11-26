package shadow

import (
	"ptah.run/internal/devclean"
	"ptah.run/internal/devclean/replay"
	"ptah.run/migration/internal/shadowdb"
)

// ErrReplayRefused is what an error from an entry point of this package
// matches with errors.Is when the replay guard refused the history or the
// candidate before the first statement of the replay ran: a statement whose
// effect the reset of the shadow database cannot take back, or a session whose
// statements the guard cannot read. Nothing of the refused replay reached the
// shadow database or the target. The message names the file and the statement,
// and ends with the remedies [ReplayTrust] supplies where they would lift the
// refusal.
var ErrReplayRefused = replay.ErrRefused

// DefaultTrustRemedy ends a refusal that [ReplayTrust.Trusted] would lift when
// the caller supplies no [ReplayTrust.Remedy] of its own.
const DefaultTrustRemedy = shadowdb.DefaultTrustRemedy

// ReplayTrust is a caller's trust in the SQL a verification replays on the
// shadow database. The zero value trusts nothing, and a refusal trust would
// lift ends with [DefaultTrustRemedy]. Nothing in this package reads an
// environment variable for it.
type ReplayTrust struct {
	// Trusted runs, with the privileges of the shadow connection, every
	// statement the replay guard refuses only because its effect reaches past
	// the shadow database, such as a DO block, a routine or a role on
	// PostgreSQL, or a stored body on MySQL. The guard still refuses what
	// trust does not lift: an executable comment on MySQL, a change to the
	// session's settings, a temporary object, and a statement the reset could
	// not take back from the shadow database itself. On an engine a provider
	// serves, the provider opens its session for trusted SQL as well. The
	// cleanup still empties only the shadow database, so a role or another
	// server-level object the SQL creates stays on the server.
	Trusted bool
	// Remedy ends a refusal that Trusted would lift, when Trusted is false.
	// Empty ends it with [DefaultTrustRemedy]. A caller that takes trust from
	// its own flag or setting names that here.
	Remedy string
	// ServerRemedy ends a refusal that a server the run discards after it
	// would lift, such as a comment on a PostgreSQL extension. Empty appends
	// none. A caller that can provision such a server names here how.
	ServerRemedy string
}

// guardTrust is trust in the form the replay guard reads it.
func (t ReplayTrust) guardTrust() devclean.ShadowTrust {
	return shadowdb.Trust(t.Trusted, t.Remedy, t.ServerRemedy)
}
