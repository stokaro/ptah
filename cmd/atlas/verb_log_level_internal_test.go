package atlas

// White-box testing required: the subject is the unexported atlasVerb table
// itself — each verb's prefixArgs and the native target its factory builds.
// Neither is reachable from the exported command tree, because a forward
// applies its prefix inside RunE and the resulting log level is invisible in
// help output and in any successful run's streams. Driving this through the
// exported surface could only observe one verb at a time, and only by running
// a real migration per verb; the point of this test is to measure every verb.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/cliobs"
)

// The Atlas-compatible surface is quiet by construction: cmd/ptah-compat's main
// installs cliobs.QuietDefaultLogger, which lowers what the *default* slog
// logger accepts. A forwarded verb whose native target starts its own
// observability runtime escapes that, because cliobs.Start installs a fresh
// logger over the quiet default at the target's own --log-level default of
// "info" — which is how stokaro/ptah#969 put eight lines of INFO narration on
// `ptah-compat migrate down`'s stderr.
//
// The fix is a per-verb `--log-level warn` prefix rather than a compat-wide
// one, because a compat-wide prefix is measurably wrong: a verb whose target
// does not register the flag exits 1 with `Error: unknown flag: --log-level`
// (`migrate validate` and `migrate hash` both do).
//
// This test is the guard that the choice cannot be forgotten on the next verb.
// It enumerates every table-driven verb, builds its native target, and measures
// two things per verb — whether the target registers --log-level, and what
// value the verb's prefix pins it to. A new forwarded verb whose target starts
// a runtime fails here by name, and so does a verb whose prefix drifts to a
// different level.

// verbLogLevelObservation is what the guard measures for one verb.
type verbLogLevelObservation struct {
	// TargetRegistersFlag reports whether the native target the verb forwards
	// into accepts --log-level at all. Only such a target can start its own
	// observability runtime, and only such a target tolerates the prefix.
	TargetRegistersFlag bool
	// PrefixLevel is the value the verb's prefixArgs pin --log-level to, or ""
	// when the prefix does not carry the flag.
	PrefixLevel string
}

// allAtlasTableVerbs returns every verb the Atlas surface builds through the
// atlasVerb table, keyed by the command path a failure should name.
func allAtlasTableVerbs() map[string]atlasVerb {
	verbs := map[string]atlasVerb{
		"migrate down":     atlasMigrateDownVerb(),
		"migrate hash":     atlasMigrateHashVerb(),
		"migrate validate": atlasMigrateValidateVerb(),
		"schema test":      atlasSchemaTestVerb(),
	}
	for _, verb := range atlasMigrateForwardVerbs() {
		verbs["migrate "+verb.use] = verb
	}
	return verbs
}

// prefixLogLevel returns the value prefixArgs pin --log-level to, or "" when
// the prefix does not set it.
func prefixLogLevel(prefixArgs []string) string {
	index := slices.Index(prefixArgs, "--"+cliobs.LogLevelFlagName)
	if index < 0 || index+1 >= len(prefixArgs) {
		return ""
	}
	return prefixArgs[index+1]
}

// observeVerbLogLevel builds the verb's native target and reports what the
// guard measures.
func observeVerbLogLevel(verb atlasVerb) verbLogLevelObservation {
	target := verb.factory()
	return verbLogLevelObservation{
		TargetRegistersFlag: target.Flags().Lookup(cliobs.LogLevelFlagName) != nil,
		PrefixLevel:         prefixLogLevel(verb.prefixArgs),
	}
}

func TestAtlasVerbsCarryLogLevelExactlyWhereTargetTakesIt(t *testing.T) {
	c := qt.New(t)

	// Pinned per verb, not derived, so the value itself is part of the
	// contract. `error` would silence the run log just as completely on a clean
	// run — and would also drop the Warn-level diagnostics that exist on no
	// other channel (a migration lock that would not release, a skipped
	// out-of-order migration, a connection that would not close), making the
	// forwarded verb quieter than the rest of the compat binary.
	want := map[string]verbLogLevelObservation{
		"migrate down":       {TargetRegistersFlag: true, PrefixLevel: "warn"},
		"migrate checkpoint": {},
		"migrate edit":       {},
		"migrate hash":       {},
		"migrate new":        {},
		"migrate rebase":     {},
		"migrate rm":         {},
		"migrate test":       {},
		"migrate validate":   {},
		"schema test":        {},
	}

	verbs := allAtlasTableVerbs()
	got := make(map[string]verbLogLevelObservation, len(verbs))
	for name, verb := range verbs {
		got[name] = observeVerbLogLevel(verb)
	}

	// The exact map catches a new verb appearing, a verb disappearing, and a
	// prefix drifting to another level.
	c.Assert(got, qt.DeepEquals, want)

	// The same measurement restated as the rule it encodes, so the guard keeps
	// meaning even if someone rewrites the table above: a verb is quieted
	// exactly when its target can take the flag.
	quieted := make(map[string]bool, len(got))
	registers := make(map[string]bool, len(got))
	for name, observation := range got {
		quieted[name] = observation.PrefixLevel == "warn"
		registers[name] = observation.TargetRegistersFlag
	}
	c.Assert(quieted, qt.DeepEquals, registers)
}
