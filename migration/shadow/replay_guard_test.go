package shadow_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/shadow"
)

// Every entry point replays the history through the dev replay guard. On
// SQLite the guard refuses ATTACH, which opens another database file on the
// connection the replay goes on using, so a history whose migration 1 begins
// with one is refused before its first statement runs. The controls in
// history_tx_mode_test.go replay the same history without it.

// replayGuardAttach leads migration 1 of the refused history.
const replayGuardAttach = "ATTACH DATABASE ':memory:' AS aux;\n"

// replayGuardRefusal is the guard's refusal of replayGuardAttach.
const replayGuardRefusal = `(?s).*sqlite migration replay rejects ATTACH because its effects cannot be confined to the disposable database realm.*`

// replayGuardEntryPoints are the entry points that replay a history, each
// run on a new target and shadow database with header leading migration 1.
var replayGuardEntryPoints = []struct {
	name string
	run  func(c *qt.C, header string) error
}{
	{
		name: "VerifyApplied",
		run: func(c *qt.C, header string) error {
			target, shadowURL := replayTarget(c)
			_, err := shadow.VerifyApplied(c.Context(), replayAppliedOptions(target, shadowURL, header))
			return err
		},
	},
	{
		name: "VerifyRollback",
		run: func(c *qt.C, header string) error {
			target, shadowURL := replayTarget(c)
			return shadow.VerifyRollback(c.Context(), replayRollbackOptions(target, shadowURL, header))
		},
	},
	{
		name: "PlanDynamicRollback",
		run: func(c *qt.C, header string) error {
			target, devURL := replayTarget(c)
			_, err := shadow.PlanDynamicRollback(c.Context(), replayDynamicRollbackOptions(target, devURL, header))
			return err
		},
	},
	{
		name: "VerifyBaseline",
		run: func(c *qt.C, header string) error {
			target, shadowURL := replayTarget(c)
			return shadow.VerifyBaseline(c.Context(), replayBaselineOptions(target, shadowURL, header))
		},
	},
	{
		name: "VerifyMigration",
		run: func(c *qt.C, header string) error {
			return shadow.VerifyMigration(c.Context(), replayMigrationOptions(c, header))
		},
	},
}

func TestReplayGuard_FailurePath(t *testing.T) {
	for _, test := range replayGuardEntryPoints {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.run(c, replayGuardAttach)

			c.Assert(err, qt.ErrorMatches, replayGuardRefusal)
		})
	}
}

// A candidate [shadow.VerifyMigration] applies on top of the history is read
// by the guard as the history is: the history replays, and the candidate's
// ATTACH is refused before the candidate runs.
func TestReplayGuard_FailurePathForACandidate(t *testing.T) {
	c := qt.New(t)
	opts := replayMigrationOptions(c, "")
	candidate := replayCandidate
	candidate.UpSQL = replayGuardAttach + candidate.UpSQL
	opts.Candidates = []shadow.Candidate{candidate}

	err := shadow.VerifyMigration(c.Context(), opts)

	c.Assert(err, qt.ErrorMatches, replayGuardRefusal)
}
