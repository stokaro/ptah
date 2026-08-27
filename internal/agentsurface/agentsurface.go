// Package agentsurface classifies every Ptah verb by what it does to the
// databases it is given.
//
// It exists because the question "which operations may an autonomous agent
// drive" was answered once, in prose, in a record that must never be edited --
// and the answer went stale silently. ADR 0002 enumerated the surface from the
// built binary on 2026-08-21 and classified the readers by which of them
// register `--dev-url`. Both halves aged badly: the binary grew nine verbs the
// table does not name, and the flag name turned out not to be the axis.
//
// The classification here is hand-written, because what a verb does to a
// database is not visible in its flag set. What IS visible is checked against
// it: [Walk] measures which database-naming flags each verb registers, and the
// guards in this package require the two to agree. A verb classified as
// touching no database while registering `--db-url` fails the build, and so
// does one this map does not name at all (stokaro/ptah#1484).
package agentsurface

import "sort"

// Target says what a verb does to the database it is pointed at.
type Target string

const (
	// TargetNone is a verb that opens no connection to a target at all.
	TargetNone Target = "none"
	// TargetReads is a verb that reads the target and writes nothing to it.
	TargetReads Target = "reads"
	// TargetWrites is a verb that changes the target.
	TargetWrites Target = "writes"
)

// Scratch says what a verb does to the SECOND database some of them take: a
// dev, shadow or throwaway database that is not the target.
//
// It is a separate axis rather than a stronger grade of [Target] because the
// two are independent, and conflating them is what ADR 0002 §1.2 did: an
// operation can read the target and still destroy something -- `schema inspect`
// is a reader whose dev database "is reset destructively", in its own flag's
// words.
type Scratch string

const (
	// ScratchNone is a verb that takes no second database.
	ScratchNone Scratch = "none"
	// ScratchProbes is a verb that creates objects in the second database and
	// drops them again. It writes there, and it preserves what it found.
	ScratchProbes Scratch = "probes"
	// ScratchRewrites is a verb that replays a migration directory or a plan
	// into the second database, or cleans it first. What was in it does not
	// survive.
	ScratchRewrites Scratch = "rewrites"
)

// targetFromProject names the verbs that reach their target through the
// PROJECT FILE rather than through a flag.
//
// [Walk] checks the hand-written classification against each command's flags,
// on the premise that a verb registering no `--db-url` cannot touch a target.
// That premise holds for every verb which is given a database; it runs out for
// a verb which is given a PROJECT and finds the database inside it.
//
// `project adopt --preflight` is that verb. It reads the revision history the
// project's own configuration points at, and it takes no URL flag on purpose:
// the project file is where the answer to "which database" already lives, and a
// flag that disagreed with it would report confidently about a history the
// project does not target.
//
// This is a list rather than a field on [Verb] so that the classifications stay
// positional literals, and a list rather than a relaxed guard so that every
// other verb keeps the bidirectional check.
var targetFromProject = map[string]struct{}{
	"project adopt": {},
}

// TargetFromProject reports whether a verb reaches its target through the
// project file instead of a flag. A caller checking a classification against a
// command's flags has to allow for these; nothing else may reach a database
// without saying so in a flag.
func TargetFromProject(name string) bool {
	_, ok := targetFromProject[name]
	return ok
}

// Verb is one command's classification, with the reason it carries that one.
type Verb struct {
	Target  Target
	Scratch Scratch
	// Reason says what the verb does, in enough words to be checkable against
	// the command's own help. A classification without one is an assertion.
	Reason string
}

// DatabaseSafe reports whether driving this verb can change or destroy a
// database.
//
// It is a property of the classification rather than a second list: a verb that
// writes the target, or rewrites a database it is handed, is not safe whatever
// anyone would like. A prober is, on ADR 0002 §2.1's condition -- the second
// database is configured out of band and is never an agent-supplied parameter.
//
// It answers about DATABASES and nothing else, which is the whole scope of this
// package. Several verbs it calls safe write files in the working tree
// (`introspect`, `schema fmt`, `migrations create`) or reach a registry or a
// model endpoint (`schema push`, `oci copy`, `assist provider test`). Those are
// real exposures and they are governed by a different record: ADR 0004 decides
// what an agent may do to artifacts. Reading this answer as "safe to run" would
// be exactly the mistake ADR 0002 §1.2 made in the other direction, where a
// verb that reads its target was called a pure reader while its dev database
// was being reset.
func (v Verb) DatabaseSafe() bool {
	return v.Target != TargetWrites && v.Scratch != ScratchRewrites
}

// Lookup returns the classification of one verb, named the way the command line
// spells it without the program name: "schema inspect".
func Lookup(name string) (Verb, bool) {
	verb, ok := verbs[name]
	return verb, ok
}

// Names lists every classified verb, sorted.
func Names() []string {
	names := make([]string, 0, len(verbs))
	for name := range verbs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
