package txrequire

import (
	"ptah.run/core/platform/capability"
	"ptah.run/internal/filetx"
)

// AloneOutsideATransaction reports a file whose one statement the engine
// refuses inside a transaction, such as CREATE INDEX CONCURRENTLY, where the
// runner sends the file on its own ([filetx.RunsAlone]) and the engine runs it
// without one.
//
// It is the one answer to "does this file of a directory written for a runner
// that opens no transaction around a file run without one". The migrator asks
// it before it applies or replays such a file, and lint asks it to judge the
// file as one that runs without a transaction, so the three cannot disagree.
// A caller that reads the file's transaction directive decides before asking:
// a file that names a mode keeps it.
func AloneOutsideATransaction(dialect string, caps capability.Capabilities, statements []string) bool {
	if !filetx.RunsAlone(statements) {
		return false
	}
	analyzed := make([]Statement, 0, len(statements))
	for index, statement := range statements {
		analyzed = append(analyzed, Statement{Index: index, SQL: statement})
	}
	return Analyze(dialect, caps, analyzed).RequiresAutocommit()
}
