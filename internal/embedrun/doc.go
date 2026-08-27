// Package embedrun is the durable state of one embedding migration: where it
// got to, who is allowed to move it, and what happened.
//
// # Why it is durable
//
// A migration over a corpus is long, and the process running it will be
// restarted, killed, rescheduled or replaced before it finishes. State held in
// terminal output, an Assist session, an MCP connection or a local variable
// does not survive any of those, and a run that cannot resume has to start
// again -- which for an embedding migration means paying the provider twice for
// the same answers (stokaro/ptah#2068).
//
// # Why a fencing token and not only a lease
//
// A lease answers "who should be working"; it does not stop a process that
// believes it still holds one. A worker paused long enough for its lease to
// expire, then resumed, will happily commit -- so every mutating operation
// carries a token, and a token behind the run's own is refused. The check is on
// the state rather than on the clock, because a clock is what the stale worker
// also has.
//
// # What is deliberately absent
//
// The event log has no field for source content or for a vector, and that is
// structural rather than a rule somebody follows: an audit trail that could
// carry the corpus would become a second copy of it, outside every control the
// corpus has.
package embedrun
