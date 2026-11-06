package dbcli

import (
	"fmt"
	"strings"

	"ptah.run/migration/migrator"
)

// LostLogEntriesNotice is what a command says to a person when the migration log
// did not get every entry the run tried to write. It is empty when the log got
// them all, and when the run keeps no log.
//
// It names the migrations and leaves the reasons to the warnings each failed
// write printed, because a run that lost one entry usually lost all of them for
// one reason, and the reason is long.
func LostLogEntriesNotice(report *migrator.MigrationLogReport) string {
	if report == nil || len(report.Lost) == 0 {
		return ""
	}
	return fmt.Sprintf("⚠️  The migration log lost %d of %d entries: %s.\n"+
		"`ptah migrations log` does not show them. `ptah migrations status` still answers "+
		"what is applied, and the warnings above say why each write failed.",
		len(report.Lost), report.Written+len(report.Lost), describeLostEntries(report.Lost))
}

// describeLostEntries lists the lost entries migration by migration, in the order
// the run tried them.
func describeLostEntries(lost []migrator.MigrationLogLoss) string {
	var parts []string
	for index := 0; index < len(lost); {
		first := lost[index]
		if first.State == "" {
			parts = append(parts, "a "+first.Operation+" whose migrations could not be named")
			index++
			continue
		}
		var states []string
		for ; index < len(lost) && sameLogAttempt(first, lost[index]); index++ {
			states = append(states, string(lost[index].State))
		}
		parts = append(parts, fmt.Sprintf("version %d (%s)", first.Version, strings.Join(states, ", ")))
	}
	return strings.Join(parts, ", ")
}

// sameLogAttempt reports whether two lost entries belong to one attempt, which
// is what writes a start and its outcome.
func sameLogAttempt(left, right migrator.MigrationLogLoss) bool {
	return right.State != "" && left.Operation == right.Operation &&
		left.Version == right.Version && left.VersionKey == right.VersionKey
}
