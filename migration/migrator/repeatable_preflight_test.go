package migrator_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// These tests read what a pre-migration hook sees of a run whose only work is a
// changed repeatable migration. The hook is where `ptah migrations up` runs its
// destructive-change gate, its pre-up command, its webhook and its backup, so a
// plan it never sees is a plan none of them judges.

// recordingPreflight returns a hook that keeps every plan it sees and answers
// with refusal.
func recordingPreflight(seen *[]migrator.MigrationPlan, refusal error) migrator.PreMigrationHook {
	return func(_ context.Context, plan migrator.MigrationPlan) error {
		*seen = append(*seen, plan)
		return refusal
	}
}

func TestRepeatablePreflight_HappyPath(t *testing.T) {
	t.Run("the hook sees a plan whose only work is a changed repeatable migration", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		var seen []migrator.MigrationPlan
		_, err = runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewChangedUp}),
			migrator.MigrateUpOptions{Preflight: recordingPreflight(&seen, nil)})
		c.Assert(err, qt.IsNil)
		c.Assert(seen, qt.HasLen, 1)
		c.Assert(seen[0].Versions, qt.HasLen, 0)
		c.Assert(seen[0].Repeatables, qt.DeepEquals, []migrator.RepeatableWork{{
			Identity:        "user_names",
			Path:            "R__user_names.sql",
			Checksum:        h1(repeatableViewChangedUp),
			AppliedChecksum: h1(repeatableViewUp),
			Reason:          migrator.RepeatableReasonChanged,
		}})
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names", "user_names"})
	})

	t.Run("the hook does not run for a plan that selects nothing", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		var seen []migrator.MigrationPlan
		_, err = runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{Preflight: recordingPreflight(&seen, nil)})
		c.Assert(err, qt.IsNil)
		c.Assert(seen, qt.HasLen, 0)
	})
}

func TestRepeatablePreflight_FailurePath(t *testing.T) {
	t.Run("a hook that refuses a plan whose only work is a repeatable migration stops it before it runs", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		refused := errors.New("refused by the hook")
		changed := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewChangedUp})
		var seen []migrator.MigrationPlan
		_, err = runRepeatableUp(c, dbPath, changed, migrator.MigrateUpOptions{Preflight: recordingPreflight(&seen, refused)})
		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(seen, qt.HasLen, 1)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
		c.Assert(repeatableStatus(c, dbPath, changed).Repeatables[0].AppliedChecksum, qt.Equals, h1(repeatableViewUp))
	})
}
