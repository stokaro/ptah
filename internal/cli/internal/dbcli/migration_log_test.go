package dbcli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/migration/migrator"
)

func TestLostLogEntriesNotice_NamesEachAttempt(t *testing.T) {
	entry := func(operation string, version int64, state migrator.MigrationLogState) migrator.MigrationLogLoss {
		return migrator.MigrationLogLoss{Operation: operation, Version: version, State: state}
	}
	tests := []struct {
		name    string
		written int
		lost    []migrator.MigrationLogLoss
		want    string
	}{
		{
			name: "every entry of every file",
			lost: []migrator.MigrationLogLoss{
				entry("up", 1, migrator.MigrationLogStarted),
				entry("up", 1, migrator.MigrationLogApplied),
				entry("up", 2, migrator.MigrationLogStarted),
				entry("up", 2, migrator.MigrationLogApplied),
			},
			want: "lost 4 of 4 entries: version 1 (started, applied), version 2 (started, applied).",
		},
		{
			name:    "only the second file",
			written: 2,
			lost: []migrator.MigrationLogLoss{
				entry("up", 2, migrator.MigrationLogStarted),
				entry("up", 2, migrator.MigrationLogApplied),
			},
			want: "lost 2 of 4 entries: version 2 (started, applied).",
		},
		{
			name:    "one transaction for the batch settles its files in order",
			written: 2,
			lost: []migrator.MigrationLogLoss{
				entry("up", 1, migrator.MigrationLogApplied),
				entry("up", 2, migrator.MigrationLogApplied),
			},
			want: "lost 2 of 4 entries: version 1 (applied), version 2 (applied).",
		},
		{
			name: "the same version up and down are two attempts",
			lost: []migrator.MigrationLogLoss{
				entry("up", 1, migrator.MigrationLogApplied),
				entry("down", 1, migrator.MigrationLogStarted),
			},
			want: "lost 2 of 2 entries: version 1 (applied), version 1 (started).",
		},
		{
			name: "a rollback that could not name its migrations",
			lost: []migrator.MigrationLogLoss{{Operation: "down"}},
			want: "lost 1 of 1 entries: a down whose migrations could not be named.",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			notice := dbcli.LostLogEntriesNotice(&migrator.MigrationLogReport{Written: test.written, Lost: test.lost})

			c.Assert(notice, qt.Contains, test.want)
		})
	}
}

// The absent report and the report with nothing lost are different answers to a
// reader of the document, and the same answer to a person: nothing to say.
func TestLostLogEntriesNotice_IsEmptyWhenNothingWasLost(t *testing.T) {
	tests := []struct {
		name   string
		report *migrator.MigrationLogReport
	}{
		{name: "the run keeps no log"},
		{name: "every entry was written", report: &migrator.MigrationLogReport{Written: 4}},
		{name: "nothing to record", report: &migrator.MigrationLogReport{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(dbcli.LostLogEntriesNotice(test.report), qt.Equals, "")
		})
	}
}
