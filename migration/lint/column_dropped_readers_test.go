package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// dropEmailAnalysis lints one migration that drops a column, with the readers
// the caller resolved from the state that version starts from.
func dropEmailAnalysis(c *qt.C, dependents []lint.BaselineDependent) lint.Analysis {
	c.Helper()

	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"1_drop.sql": "ALTER TABLE users DROP COLUMN email;",
	}), lint.Options{
		Dialect:            "postgres",
		DirFormat:          migrationfile.DirFormatAtlas,
		BaselineDependents: dependents,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// messagesFor returns the messages one rule reported across the analysis.
func messagesFor(analysis lint.Analysis, code string) []string {
	messages := make([]string, 0)
	for _, finding := range findingsFor(analysis.Findings(), code) {
		messages = append(messages, finding.Message)
	}
	return messages
}

// TestColumnDroppedWithReaders_NamesWhatBreaks is #1270's criterion 9.
//
// `DROP COLUMN email` under a view that selects it reported DS102 alone and
// never named the view, so the operator learned about it when the migration
// ran. The reader is resolved from the state the version starts from, because a
// view body is not in the file that drops the column.
func TestColumnDroppedWithReaders_NamesWhatBreaks(t *testing.T) {
	c := qt.New(t)

	analysis := dropEmailAnalysis(c, []lint.BaselineDependent{
		{Version: 1, Table: "users", Column: "email", Dependent: "active_users", Kind: "view"},
	})

	messages := messagesFor(analysis, "DS110P")
	c.Assert(messages, qt.HasLen, 1)
	c.Assert(messages[0], qt.Contains, "view active_users")
	c.Assert(messages[0], qt.Contains, "email")
}

// TestColumnDroppedWithReaders_NamesEveryReader keeps the answer complete.
//
// Dropping the column breaks all of them, and a finding naming one would send
// the operator to fix that one and run into the next.
func TestColumnDroppedWithReaders_NamesEveryReader(t *testing.T) {
	c := qt.New(t)

	analysis := dropEmailAnalysis(c, []lint.BaselineDependent{
		{Version: 1, Table: "users", Column: "email", Dependent: "active_users", Kind: "view"},
		{Version: 1, Table: "users", Column: "email", Dependent: "notify", Kind: "function"},
	})

	messages := messagesFor(analysis, "DS110P")
	c.Assert(messages, qt.HasLen, 1)
	c.Assert(messages[0], qt.Contains, "view active_users")
	c.Assert(messages[0], qt.Contains, "function notify")
}

// TestColumnDroppedWithReaders_SaysNothingWhenNothingReadsIt is the control.
//
// A rule that fired on every drop would pass the tests above and make the
// finding worthless: the whole point is that it separates a column something
// depends on from one nothing does.
func TestColumnDroppedWithReaders_SaysNothingWhenNothingReadsIt(t *testing.T) {
	c := qt.New(t)

	analysis := dropEmailAnalysis(c, []lint.BaselineDependent{
		{Version: 1, Table: "users", Column: "created_at", Dependent: "active_users", Kind: "view"},
	})

	c.Assert(messagesFor(analysis, "DS110P"), qt.HasLen, 0)
	c.Assert(messagesFor(analysis, "DS102"), qt.Not(qt.HasLen), 0)
}

// TestColumnDroppedWithReaders_ReportsItsInputWhenTheRunSuppliesNone is the
// property the input declaration exists for.
//
// A run with no dev database resolves no readers. Reporting nothing and exiting
// 0 is the failure mode RuleInput was introduced to remove: the rule finds
// less, and only the unmet-input list says why.
func TestColumnDroppedWithReaders_ReportsItsInputWhenTheRunSuppliesNone(t *testing.T) {
	c := qt.New(t)

	analysis := dropEmailAnalysis(c, nil)

	c.Assert(messagesFor(analysis, "DS110P"), qt.HasLen, 0)
	c.Assert(analysis.UnmetInputs(), qt.Not(qt.HasLen), 0)
}
