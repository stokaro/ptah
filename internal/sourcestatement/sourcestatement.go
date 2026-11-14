// Package sourcestatement recovers a statement the migrator sent as the
// migration body spells it.
//
// The migrator sends each statement trimmed and without its terminator, and
// reports a failure by the statement's 1-based position among the statements it
// split the body into. A diagnostic that quotes the statement the way the
// author wrote it reads the body's own text at that position instead. Apply,
// validate and lint in ptah-compat quote a failed statement this way, which is
// how the community Atlas binary quotes it.
package sourcestatement

import (
	"strings"

	"ptah.run/core/sqlutil"
)

// Written is the statement at the 1-based index of body, as body spells it
// from its first token through its terminating semicolon. sent is the
// statement as the migrator sent it, and dialect names the engine whose rules
// split body.
//
// The body's text at index is used only when it is the statement that was
// sent once its terminator is set aside, so a body the migrator split
// differently -- a template, a directive, a query unit -- quotes sent rather
// than a neighbor. A last statement with no semicolon after it is quoted as
// sent, which is the same text. Written returns sent whenever index is out of
// range.
func Written(body, dialect string, index int, sent string) string {
	written := sqlutil.SplitSourceStatements(body, dialect)
	if index < 1 || index > len(written) {
		return sent
	}
	candidate := written[index-1]
	if !candidate.Terminated {
		return sent
	}
	if strings.TrimSpace(strings.TrimSuffix(candidate.Text, ";")) != strings.TrimSpace(sent) {
		return sent
	}
	return candidate.Text
}
