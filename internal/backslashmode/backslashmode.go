// Package backslashmode decides which spelling of a backslash Ptah writes for
// a MySQL-family session, and when text written for one session must not run
// on another. A session whose sql_mode names NO_BACKSLASH_ESCAPES reads a
// backslash in a string as the character, and any other session reads it as
// the start of an escape. The connector records the mode as
// [capability.NoBackslashEscapes]; the renderers read that key to spell a
// literal for the session that runs it, and the callers here decide which
// session that is.
//
// Text bound for a session is cut into statements with the session's rule
// ([Escapes]), since the cut decides what is sent: a default-mode lexer reads
// `\'` as an escaped quote, so on NO_BACKSLASH_ESCAPES text it ends a string
// early and cuts a statement at a semicolon inside it, or reads a string on
// past its end and hides the statements behind it. The scanners that read
// a statement once it is cut -- the rehearsal's escape lint and re-scoping,
// the risk rating, the transaction preflight, the definer and owner checks --
// read it with the dialect's default rule. Each reads one statement at a
// time, so a wrong token boundary inside it can misjudge that statement only:
// it can neither hide a statement boundary nor invent one.
package backslashmode

import (
	"slices"
	"strings"

	"ptah.run/core/platform/capability"
)

// Escapes reports whether a session reads a backslash in a string as the
// start of an escape: its dialect does, as the descriptor states in
// dialectEscapes, and caps does not hold [capability.NoBackslashEscapes]. It is
// the one place the session's sql_mode overrides the dialect's rule: the
// renderers, the data literals and the statement splitter all ask it.
func Escapes(dialectEscapes bool, caps capability.Capabilities) bool {
	return dialectEscapes && !caps.Has(capability.NoBackslashEscapes)
}

// ForLater returns caps for text Ptah writes for a session it does not know
// yet: a migration file, a saved plan, the statements a diff prints. Such text
// is written for the default sql_mode, whatever the session that planned it
// reads, so caps loses [capability.NoBackslashEscapes]. caps itself is not
// changed.
func ForLater(caps capability.Capabilities) capability.Capabilities {
	if !caps.Has(capability.NoBackslashEscapes) {
		return caps
	}
	return caps.With(capability.NoBackslashEscapes, false)
}

// ReadDifferently reports whether statements written for a session with the
// capabilities writtenFor would mean something else on a session with the
// capabilities runOn: the two disagree on NO_BACKSLASH_ESCAPES and a statement
// holds a backslash.
//
// The test is on the backslash byte because NO_BACKSLASH_ESCAPES changes how
// a backslash is read and nothing else. Text without one means the same on
// both sessions, so a dev database or a session with the other sql_mode still
// runs it. A backslash outside a string reads the same in both modes too, so
// the byte test can name text that would run correctly, and never passes text
// that would not; it needs no lexer for that.
func ReadDifferently(writtenFor, runOn capability.Capabilities, statements []string) bool {
	return writtenFor.Has(capability.NoBackslashEscapes) != runOn.Has(capability.NoBackslashEscapes) &&
		slices.ContainsFunc(statements, func(statement string) bool { return strings.Contains(statement, `\`) })
}

// Describe names the mode caps reads a backslash in, as a refusal states it.
func Describe(caps capability.Capabilities) string {
	if caps.Has(capability.NoBackslashEscapes) {
		return "names NO_BACKSLASH_ESCAPES"
	}
	return "does not name NO_BACKSLASH_ESCAPES"
}
