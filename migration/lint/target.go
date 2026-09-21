package lint

import (
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/lintdialect"
	"ptah.run/internal/servertarget"
)

// Target is the server a lint run plans against.
//
// Whether a statement is safe to run against a live database is decided by the
// server version at least as often as by the statement: ADD COLUMN with a
// non-volatile default rewrites the table on PostgreSQL 10 and edits the
// catalog on 11, and DROP COLUMN copies the table on MySQL 8.0.28 and is
// INSTANT on 8.0.29. A rule that fires on every version is noise on the new
// ones and a rule that stays quiet is wrong on the old ones, so a rule that
// knows the difference needs somewhere to read it.
//
// Capabilities is that place. It is the same shape the renderer and the planner
// gate on, so a rule asks what the server can do rather than comparing version
// numbers of its own. Version and Note are for a finding that has to say what
// it planned against.
//
// A rule asks Capabilities, never the version string: the key it needs belongs
// on the capability ladder where the renderer and the planner already read it,
// and a rule comparing version numbers of its own is the second version model
// this deliberately does not build.
//
// The zero value names no server. An analysis resolves the dialect default
// into it, so a statement carries a resolved set rather than an empty one --
// except where the run named no dialect either, which has no ladder to resolve
// and leaves every key unanswered. [capability.Capabilities.Established] is
// what tells that apart from a server that answers false.
type Target struct {
	// Dialect is the canonical dialect the capabilities were resolved for,
	// empty when the run named no dialect and every rule ran.
	Dialect string
	// Version is the server version the run planned against, exactly as the
	// operator wrote it or as the dev database reported it. Empty when the run
	// named no server, which is when Capabilities is the dialect default.
	Version string
	// Capabilities is what a server on that version can do.
	Capabilities capability.Capabilities
	// Note is empty when Version selected an exact measured release line, and
	// otherwise says in one sentence what was planned instead. A run whose
	// version fell between measured lines planned against something the
	// operator did not name, and saying nothing there is how such a run reads
	// as a run against the server they asked for.
	Note string
}

// Named reports whether the run named a server. A target that names none
// carries the dialect default, which is a starting point rather than a
// measurement, and a rule or a report that distinguishes the two asks here.
func (t Target) Named() bool {
	return t.Version != ""
}

// ResolveTarget maps a dialect and a server version onto the target a lint run
// plans against.
//
// An empty version is not a mistake: it means the run named no server, and the
// dialect default is what every other offline verb plans with there. A
// non-empty version that names no server is refused, and so is one naming a
// different product than the dialect, because a typo that resolved to the
// default would silently change which rules can fire. A version with no
// dialect is refused for the same reason: a run with no dialect runs every
// rule against every engine, and there is no one target for the version to
// describe. A dialect this linter has no rules for is refused too, with the
// list of the ones it has.
//
// The dialect is canonicalized: an accepted alias such as `pgx` resolves to
// `postgres`, and that is what [Target.Dialect] carries.
//
// The resolution is the same one --server-version carries on every other
// offline command, so a version accepted by `ptah schema render` is accepted
// here and resolves to the same capabilities.
func ResolveTarget(dialect, version string) (Target, error) {
	// Canonicalized here rather than stored as written, because everything
	// downstream compares by exact string: Rule.Dialects, the lexer mode, and
	// a report a reader compares across runs. An alias left in place would
	// name a dialect no rule selects while the capabilities came from the
	// engine it aliases.
	canonical, ok := lintdialect.Canonical(dialect)
	if !ok {
		return Target{}, fmt.Errorf("unsupported lint dialect %q: expected %s", dialect, lintdialect.Expected)
	}
	if version != "" && canonical == "" {
		return Target{}, fmt.Errorf(
			"server version %q needs a dialect: with none, every rule runs and there is no "+
				"single target the version could describe",
			version,
		)
	}
	resolved, err := servertarget.Resolve(canonical, version)
	if err != nil {
		return Target{}, err
	}
	return Target{
		Dialect:      canonical,
		Version:      version,
		Capabilities: resolved.Capabilities,
		Note:         resolved.Note,
	}, nil
}

// effectiveTarget is the target an analysis runs with.
//
// A caller that resolved one through [ResolveTarget] gets it back untouched.
// A caller that resolved none -- every caller that never heard of a server --
// gets the dialect default, resolved here rather than left empty, because an
// empty capability set answers false to every key and a rule cannot tell that
// apart from a server that genuinely lacks everything. A run with no dialect
// has no ladder to resolve and keeps the empty set, which is the same thing
// its hybrid lexer says: the target is unknown.
func effectiveTarget(opts Options) (Target, error) {
	if opts.Target.Named() || len(opts.Target.Capabilities) > 0 {
		// Cloned on the way in as well as on the way out: the caller keeps
		// its own Target, and an analysis that stored that map would change
		// under it -- and change what its rules read -- if the caller wrote to
		// the set afterwards.
		return opts.Target.clone(), nil
	}
	return ResolveTarget(opts.Dialect, "")
}

// TargetFromServer is the target a live server established about itself.
//
// It takes what the connection already resolved rather than resolving the
// banner a second time, so a lint run plans against exactly what every other
// verb on that connection plans against, note included.
//
// Nothing is refused here, which is the difference from [ResolveTarget]. A
// version an operator typed can be a typo, and refusing it is how they learn;
// a banner a server wrote is what is actually there, and refusing to lint
// because Ptah has not measured that release would turn a note into an outage
// in somebody's pipeline.
func TargetFromServer(dialect, version string, capabilities capability.Capabilities, note string) Target {
	// Canonicalized where it can be and passed through where it cannot. A
	// server may name an engine this linter has no rules for -- Oracle is the
	// one -- and blanking the name there would say the target is unknown when
	// it is known and unanalyzed.
	if canonical, ok := lintdialect.Canonical(dialect); ok {
		dialect = canonical
	}
	return Target{
		Dialect:      dialect,
		Version:      version,
		Capabilities: capabilities,
		Note:         note,
	}
}

// clone returns a target that shares no map with this one.
//
// Capabilities is a map, so a Target handed out by an accessor that promises a
// deep copy would otherwise let a caller rewrite what every rule in the
// analysis reads. The other fields are strings and copy with the struct.
func (t Target) clone() Target {
	t.Capabilities = t.Capabilities.Clone()
	return t
}
