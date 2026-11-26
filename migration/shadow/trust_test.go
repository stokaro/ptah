package shadow_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/replaytrust"
	"ptah.run/migration/generator"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// On SQLite the replay guard refuses VACUUM INTO, which writes a copy of the
// database to a file outside it, unless the caller trusts the replayed SQL:
// its effect reaches past the shadow database and changes nothing there. So a
// history whose migration 1 runs one is refused with the remedy the caller
// supplies, and replays when the caller sets [shadow.ReplayTrust.Trusted].
// Migration 1 runs without a transaction, which VACUUM needs.

// trustVacuumInto leads migration 1 with a VACUUM INTO the file at path.
func trustVacuumInto(path string) string {
	return "-- atlas:txmode none\nVACUUM INTO '" + path + "';\n"
}

// trustRefusal is the guard's refusal of trustVacuumInto ended with remedy,
// and nothing after it but the statement.
func trustRefusal(remedy string) string {
	return `(?s).*sqlite migration replay rejects VACUUM INTO because its effects cannot be confined to the ` +
		`disposable database realm; ` + regexp.QuoteMeta(remedy) + `\nSQL: VACUUM INTO .*`
}

// trustEntryPoints are the entry points that replay a history, each run on a
// new target and shadow database with header leading migration 1 and trust in
// the options.
var trustEntryPoints = []struct {
	name string
	run  func(c *qt.C, header string, trust shadow.ReplayTrust) error
}{
	{
		name: "VerifyApplied",
		run: func(c *qt.C, header string, trust shadow.ReplayTrust) error {
			target, shadowURL := replayTarget(c)
			opts := replayAppliedOptions(target, shadowURL, header)
			opts.Trust = trust
			_, err := shadow.VerifyApplied(c.Context(), opts)
			return err
		},
	},
	{
		name: "VerifyRollback",
		run: func(c *qt.C, header string, trust shadow.ReplayTrust) error {
			target, shadowURL := replayTarget(c)
			opts := replayRollbackOptions(target, shadowURL, header)
			opts.Trust = trust
			return shadow.VerifyRollback(c.Context(), opts)
		},
	},
	{
		name: "PlanDynamicRollback",
		run: func(c *qt.C, header string, trust shadow.ReplayTrust) error {
			target, devURL := replayTarget(c)
			opts := replayDynamicRollbackOptions(target, devURL, header)
			opts.Trust = trust
			_, err := shadow.PlanDynamicRollback(c.Context(), opts)
			return err
		},
	},
	{
		name: "VerifyBaseline",
		run: func(c *qt.C, header string, trust shadow.ReplayTrust) error {
			target, shadowURL := replayTarget(c)
			opts := replayBaselineOptions(target, shadowURL, header)
			opts.Trust = trust
			return shadow.VerifyBaseline(c.Context(), opts)
		},
	},
	{
		name: "VerifyMigration",
		run: func(c *qt.C, header string, trust shadow.ReplayTrust) error {
			opts := replayMigrationOptions(c, header)
			opts.Trust = trust
			return shadow.VerifyMigration(c.Context(), opts)
		},
	},
	{
		name: "generator.GenerateCheckpointFromShadow",
		run: func(c *qt.C, header string, trust shadow.ReplayTrust) error {
			_, shadowURL := replayTarget(c)
			_, _, err := generator.GenerateCheckpointFromShadow(c.Context(), generator.CheckpointFromShadowOptions{
				Runtime:           builtintest.Runtime(),
				ShadowDatabaseURL: shadowURL,
				MigrationsFS:      replayHistory(header),
				Dialect:           "sqlite",
				Trust:             trust,
			})
			return err
		},
	},
}

// TestReplayTrust_FailurePath runs each entry point without trust. The zero
// value names the option that lifts the refusal, a remedy the caller supplies
// takes its place, and either refusal matches [shadow.ErrReplayRefused]. The
// copy VACUUM INTO would write is not there.
func TestReplayTrust_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		trust      shadow.ReplayTrust
		wantRemedy string
	}{
		{name: "the zero value", wantRemedy: shadow.DefaultTrustRemedy},
		{
			name:       "a remedy of the caller's",
			trust:      shadow.ReplayTrust{Remedy: "pass --trust-me"},
			wantRemedy: "pass --trust-me",
		},
	}
	for _, entry := range trustEntryPoints {
		for _, test := range tests {
			t.Run(entry.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				copied := filepath.Join(c.TempDir(), "copy.db")

				err := entry.run(c, trustVacuumInto(copied), test.trust)

				c.Assert(err, qt.ErrorMatches, trustRefusal(test.wantRemedy))
				c.Assert(err, qt.ErrorIs, shadow.ErrReplayRefused)
				_, statErr := os.Stat(copied)
				c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
			})
		}
	}
}

// TestReplayTrust_HappyPath trusts the replayed SQL: each entry point replays
// migration 1, and VACUUM INTO wrote its copy.
func TestReplayTrust_HappyPath(t *testing.T) {
	for _, entry := range trustEntryPoints {
		t.Run(entry.name, func(t *testing.T) {
			c := qt.New(t)
			copied := filepath.Join(c.TempDir(), "copy.db")

			err := entry.run(c, trustVacuumInto(copied), shadow.ReplayTrust{Trusted: true})

			c.Assert(err, qt.IsNil)
			_, statErr := os.Stat(copied)
			c.Assert(statErr, qt.IsNil)
		})
	}
}

// TestReplayTrust_FailurePathKeepsWhatTrustDoesNotLift trusts a history the
// guard refuses for a reason trust does not lift: ATTACH changes the
// connection the replay goes on using. The refusal names no trust remedy.
func TestReplayTrust_FailurePathKeepsWhatTrustDoesNotLift(t *testing.T) {
	for _, entry := range trustEntryPoints {
		t.Run(entry.name, func(t *testing.T) {
			c := qt.New(t)

			err := entry.run(c, replayGuardAttach, shadow.ReplayTrust{Trusted: true})

			c.Assert(err, qt.ErrorMatches, replayGuardRefusal)
			c.Assert(err, qt.ErrorIs, shadow.ErrReplayRefused)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*`+regexp.QuoteMeta(shadow.DefaultTrustRemedy)+`.*`)
		})
	}
}

// A replay the caller trusts is recorded for the shadow URL while it runs, so
// an engine a provider serves opens its session for trusted SQL; the record
// ends when the verification returns, and an untrusted replay makes none. A
// migration written in Go reads the record in the middle of the replay.
func TestReplayTrust_HappyPathRecordsTheShadowURLWhileTheReplayRuns(t *testing.T) {
	tests := []struct {
		name       string
		trust      shadow.ReplayTrust
		wantDuring bool
	}{
		{name: "trusted", trust: shadow.ReplayTrust{Trusted: true}, wantDuring: true},
		{name: "untrusted", wantDuring: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, shadowURL := replayTarget(c)
			var during bool
			items := &migrator.Migration{
				Version:     1,
				Description: "items",
				Up: func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
					during = replaytrust.Trusted(shadowURL)
					_, err := conn.ExecContext(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, note TEXT)")
					return err
				},
			}

			_, err := shadow.VerifyApplied(c.Context(), shadow.AppliedVerifyOptions{
				Runtime:           builtintest.Runtime(),
				ShadowDatabaseURL: shadowURL,
				TargetConn:        target,
				Migrations:        []*migrator.Migration{items},
				Dialect:           target.Info().Dialect,
				Trust:             test.trust,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(during, qt.Equals, test.wantDuring)
			c.Assert(replaytrust.Trusted(shadowURL), qt.IsFalse)
		})
	}
}
