package goannotationcleanup

// White-box testing required: this file injects source mutations at the
// unexported post-staging commit barrier, which public APIs cannot observe
// deterministically without timing-dependent filesystem polling.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
	"go.5x5.cz/ptah/internal/goannotationsource"
)

// concurrentEditOriginal is the annotated source a cleanup plans to rewrite,
// and concurrentEditRival is what a rival writer leaves in its place. They are
// package level because the commit window and the validation window measure the
// same pair of bytes from separate tests.
var (
	concurrentEditOriginal = []byte(
		"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
	)
	concurrentEditRival = []byte(
		"package models\n\n// hand edit by another writer\ntype User struct{}\n",
	)
)

// writeGoSourceInPlace and replaceGoSourceByRename are the two ways a rival
// writer leaves new bytes at a path: an editor that truncates and rewrites, and
// one that writes beside the target and renames over it. They take no checker
// because they assert nothing -- the caller reports what they return.
func writeGoSourceInPlace(path string, body []byte) error {
	return os.WriteFile(path, body, 0o600)
}

func replaceGoSourceByRename(path string, body []byte) error {
	replacement := path + ".rival"
	if err := os.WriteFile(replacement, body, 0o600); err != nil {
		return err
	}
	return os.Rename(replacement, path)
}

// commitWindow injects between the last validation and the rename that replaces
// the source. validationWindow injects one step earlier, where validatePlanPath
// still gets to see it.
func commitWindow(revalidate func() error, inject func()) applyHooks {
	return applyHooks{revalidate: revalidate, beforeCommit: inject}
}

func validationWindow(_ func() error, inject func()) applyHooks {
	return applyHooks{revalidate: func() error {
		inject()
		return nil
	}}
}

func TestApplyPlans_FailurePath_RevalidatesCompleteSourceSetAfterStaging(t *testing.T) {
	tests := []struct {
		name           string
		mutate         func(root, plainPath string) error
		wantPlain      []byte
		wantEntryNames []string
	}{
		{
			name: "added source",
			mutate: func(root, _ string) error {
				return os.WriteFile(
					filepath.Join(root, "new.go"),
					[]byte("package models\n"),
					0o600,
				)
			},
			wantPlain:      []byte("package models\n\ntype Plain struct{}\n"),
			wantEntryNames: []string{"annotated.go", "new.go", "plain.go"},
		},
		{
			name: "edited unannotated source",
			mutate: func(_ string, plainPath string) error {
				return os.WriteFile(
					plainPath,
					[]byte("package models\n\ntype Other struct{}\n"),
					0o600,
				)
			},
			wantPlain:      []byte("package models\n\ntype Other struct{}\n"),
			wantEntryNames: []string{"annotated.go", "plain.go"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			annotatedPath := filepath.Join(root, "annotated.go")
			plainPath := filepath.Join(root, "plain.go")
			annotated := []byte(
				"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
			)
			plain := []byte("package models\n\ntype Plain struct{}\n")
			c.Assert(os.WriteFile(annotatedPath, annotated, 0o600), qt.IsNil)
			c.Assert(os.WriteFile(plainPath, plain, 0o600), qt.IsNil)
			snapshot, err := goannotationsource.Capture(root)
			c.Assert(err, qt.IsNil)
			plan, err := NewPlan(snapshot)
			c.Assert(err, qt.IsNil)
			var mutateErr error

			err = applyPlans(plan.changes, applyHooks{
				revalidate: func() error {
					mutateErr = test.mutate(root, plainPath)
					return snapshot.Revalidate()
				},
				afterCommit: func() {},
			})

			c.Assert(mutateErr, qt.IsNil)
			c.Assert(err, qt.ErrorIs, goannotationsource.ErrChanged)
			assertInternalFileBytes(c, annotatedPath, annotated)
			assertInternalFileBytes(c, plainPath, test.wantPlain)
			entries, err := os.ReadDir(root)
			c.Assert(err, qt.IsNil)
			c.Assert(internalEntryNames(entries), qt.DeepEquals, test.wantEntryNames)
		})
	}
}

func TestApplyPlans_FailurePath_PreservesConcurrentEditAndRecoveryBackup(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	firstPath := filepath.Join(root, "a.go")
	secondPath := filepath.Join(root, "z.go")
	firstOriginal := []byte(
		"package models\n\n//ptah:schema:table name=\"first\"\ntype First struct{}\n",
	)
	secondOriginal := []byte(
		"package models\n\n//ptah:schema:table name=\"second\"\ntype Second struct{}\n",
	)
	firstConcurrent := []byte("package models\n\ntype First struct{ Concurrent bool }\n")
	secondConcurrent := []byte("package models\n\ntype Second struct{ Concurrent bool }\n")
	c.Assert(os.WriteFile(firstPath, firstOriginal, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(secondPath, secondOriginal, 0o600), qt.IsNil)
	c.Assert(os.Chmod(firstPath, 0o640), qt.IsNil)
	c.Assert(os.Chmod(secondPath, 0o640), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	plan, err := NewPlan(snapshot)
	c.Assert(err, qt.IsNil)

	applyErr := applyPlans(plan.changes, applyHooks{
		revalidate: snapshot.Revalidate,
		afterCommit: func() {
			c.Assert(os.WriteFile(firstPath, firstConcurrent, 0o600), qt.IsNil)
			c.Assert(os.WriteFile(secondPath, secondConcurrent, 0o600), qt.IsNil)
		},
	})

	c.Assert(applyErr, qt.ErrorIs, ErrRollbackConflict)
	c.Assert(applyErr.Error(), qt.Contains, "preserved backup")
	assertInternalFileBytes(c, firstPath, firstConcurrent)
	assertInternalFileBytes(c, secondPath, secondConcurrent)
	backups, err := filepath.Glob(filepath.Join(root, ".a.go.ptah-backup-*"))
	c.Assert(err, qt.IsNil)
	c.Assert(backups, qt.HasLen, 1)
	assertInternalFileBytes(c, backups[0], firstOriginal)
	backupInfo, err := os.Stat(backups[0])
	c.Assert(err, qt.IsNil)
	c.Assert(backupInfo.Mode().Perm(), qt.Equals, plan.changes[0].source.Mode().Perm())
	c.Assert(applyErr.Error(), qt.Contains, backups[0])
	cleanedFiles, err := filepath.Glob(filepath.Join(root, ".*.ptah-cleaned-*"))
	c.Assert(err, qt.IsNil)
	c.Assert(cleanedFiles, qt.HasLen, 0)
}

func TestApplyPlans_FailurePath_RollbackRestoresBytesAndMode(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	firstPath := filepath.Join(root, "a.go")
	secondPath := filepath.Join(root, "z.go")
	firstOriginal := []byte(
		"package models\n\n//ptah:schema:table name=\"first\"\ntype First struct{}\n",
	)
	secondOriginal := []byte(
		"package models\n\n//ptah:schema:table name=\"second\"\ntype Second struct{}\n",
	)
	secondConcurrent := []byte("package models\n\ntype Second struct{ Concurrent bool }\n")
	c.Assert(os.WriteFile(firstPath, firstOriginal, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(secondPath, secondOriginal, 0o600), qt.IsNil)
	c.Assert(os.Chmod(firstPath, 0o640), qt.IsNil)
	c.Assert(os.Chmod(secondPath, 0o640), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	plan, err := NewPlan(snapshot)
	c.Assert(err, qt.IsNil)

	applyErr := applyPlans(plan.changes, applyHooks{
		revalidate: snapshot.Revalidate,
		afterCommit: func() {
			c.Assert(os.WriteFile(secondPath, secondConcurrent, 0o600), qt.IsNil)
		},
	})

	c.Assert(applyErr, qt.IsNotNil)
	c.Assert(applyErr, qt.Not(qt.ErrorIs), ErrRollbackConflict)
	assertInternalFileBytes(c, firstPath, firstOriginal)
	assertInternalFileBytes(c, secondPath, secondConcurrent)
	firstInfo, err := os.Stat(firstPath)
	c.Assert(err, qt.IsNil)
	c.Assert(firstInfo.Mode().Perm(), qt.Equals, plan.changes[0].source.Mode().Perm())
	entries, err := os.ReadDir(root)
	c.Assert(err, qt.IsNil)
	c.Assert(internalEntryNames(entries), qt.DeepEquals, []string{"a.go", "z.go"})
}

func TestApplyPlans_FailurePath_RejectsReplacedStagedFile(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	sourcePath := filepath.Join(root, "model.go")
	original := []byte(
		"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
	)
	c.Assert(os.WriteFile(sourcePath, original, 0o600), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	plan, err := NewPlan(snapshot)
	c.Assert(err, qt.IsNil)

	applyErr := applyPlans(plan.changes, applyHooks{
		revalidate: func() error {
			staged, err := filepath.Glob(filepath.Join(root, ".model.go.ptah-cleaned-*"))
			c.Assert(err, qt.IsNil)
			c.Assert(staged, qt.HasLen, 1)
			c.Assert(os.Remove(staged[0]), qt.IsNil)
			c.Assert(os.WriteFile(staged[0], []byte("attacker-controlled\n"), 0o600), qt.IsNil)
			return snapshot.Revalidate()
		},
		afterCommit: func() {},
	})

	c.Assert(applyErr, qt.ErrorIs, fsdurable.ErrStagedFileChanged)
	assertInternalFileBytes(c, sourcePath, original)
	entries, err := os.ReadDir(root)
	c.Assert(err, qt.IsNil)
	c.Assert(internalEntryNames(entries), qt.DeepEquals, []string{"model.go"})
}

// applyPlansOverConcurrentEdit runs a cleanup over one annotated source whose
// bytes a rival writer replaces inside the window `hooks` selects, and returns
// the error the run reported.
//
// Everything asserted here holds in both windows, and the surviving bytes are
// the point: before the commit became conditional the injected edit was
// destroyed with no error at all, so an exit status alone cannot tell a refusal
// from a silent overwrite. Which refusal fired is what separates the two
// windows, so that assertion is left to the callers.
func applyPlansOverConcurrentEdit(
	c *qt.C,
	inject func(sourcePath string) error,
	hooks func(revalidate func() error, inject func()) applyHooks,
) error {
	c.Helper()
	root := c.TempDir()
	sourcePath := filepath.Join(root, "model.go")
	c.Assert(os.WriteFile(sourcePath, concurrentEditOriginal, 0o600), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	plan, err := NewPlan(snapshot)
	c.Assert(err, qt.IsNil)
	var injectErr error

	applyErr := applyPlans(plan.changes, hooks(snapshot.Revalidate, func() {
		injectErr = inject(sourcePath)
	}))

	c.Assert(injectErr, qt.IsNil)
	c.Assert(applyErr, qt.IsNotNil)
	c.Assert(applyErr, qt.Not(qt.ErrorIs), fsdurable.ErrReplacementCommitted)
	assertInternalFileBytes(c, sourcePath, concurrentEditRival)
	entries, err := os.ReadDir(root)
	c.Assert(err, qt.IsNil)
	c.Assert(internalEntryNames(entries), qt.DeepEquals, []string{"model.go"})
	return applyErr
}

// TestApplyPlans_FailurePath_RefusesForwardCommitOverConcurrentEdit measures
// the gap between validatePlanPath and the rename that replaces the source.
// Both rows leave the same bytes at the same path; they differ only in how the
// rival got them there, and a rename is the shape a stat-based check misses.
//
// TestApplyPlans_FailurePath_RejectsConcurrentEditBeforeTheLastValidation runs
// the identical mutation one step earlier, where validatePlanPath already
// rejects it.
func TestApplyPlans_FailurePath_RefusesForwardCommitOverConcurrentEdit(t *testing.T) {
	tests := []struct {
		name   string
		inject func(sourcePath string) error
	}{
		{
			name: "in-place edit inside the commit window",
			inject: func(sourcePath string) error {
				return writeGoSourceInPlace(sourcePath, concurrentEditRival)
			},
		},
		{
			name: "rename replacement inside the commit window",
			inject: func(sourcePath string) error {
				return replaceGoSourceByRename(sourcePath, concurrentEditRival)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			applyErr := applyPlansOverConcurrentEdit(c, test.inject, commitWindow)

			c.Assert(applyErr, qt.ErrorIs, fsdurable.ErrDestinationChanged)
		})
	}
}

// TestApplyPlans_FailurePath_RejectsConcurrentEditBeforeTheLastValidation is
// the control for the commit window above: the same edit, one step earlier, is
// refused by validatePlanPath and reports so in its own words. Without it a
// cleanup that refused every run would satisfy the rows above.
func TestApplyPlans_FailurePath_RejectsConcurrentEditBeforeTheLastValidation(t *testing.T) {
	c := qt.New(t)

	applyErr := applyPlansOverConcurrentEdit(c, func(sourcePath string) error {
		return writeGoSourceInPlace(sourcePath, concurrentEditRival)
	}, validationWindow)

	c.Assert(applyErr.Error(), qt.Contains, "go source changed before cleanup")
}

// TestApplyPlans_FailurePath_RefusesRollbackOverPostCommitEdit measures the
// restoration window. It is the most damaging of the three gaps: a rollback
// that loses this race overwrites a third party's post-commit edit with bytes
// captured before the run, content that never existed in any artifact anyone
// could recover from.
//
// The control row moves the identical mutation one step earlier, where
// validateCommittedPlan already refuses.
func TestApplyPlans_FailurePath_RefusesRollbackOverPostCommitEdit(t *testing.T) {
	firstOriginal := []byte(
		"package models\n\n//ptah:schema:table name=\"first\"\ntype First struct{}\n",
	)
	secondOriginal := []byte(
		"package models\n\n//ptah:schema:table name=\"second\"\ntype Second struct{}\n",
	)
	firstConcurrent := []byte("package models\n\ntype First struct{ Concurrent bool }\n")
	secondConcurrent := []byte("package models\n\ntype Second struct{ Concurrent bool }\n")
	editInPlace := func(path string) error {
		return writeGoSourceInPlace(path, firstConcurrent)
	}
	replaceByRename := func(path string) error {
		return replaceGoSourceByRename(path, firstConcurrent)
	}
	restoreWindow := func(base applyHooks, injectFirst func()) applyHooks {
		base.beforeRestore = injectFirst
		return base
	}
	// earlierValidationWindow moves the injection ahead of validateCommittedPlan,
	// which is a different hook from the package-level validationWindow: this
	// test's rollback is provoked by a second file, so the base hooks have to
	// survive the move.
	earlierValidationWindow := func(base applyHooks, injectFirst func()) applyHooks {
		failSecond := base.afterCommit
		base.afterCommit = func() {
			failSecond()
			injectFirst()
		}
		return base
	}
	tests := []struct {
		name   string
		inject func(path string) error
		hooks  func(base applyHooks, injectFirst func()) applyHooks
	}{
		{
			name:   "in-place edit inside the restoration window",
			inject: editInPlace,
			hooks:  restoreWindow,
		},
		{
			name:   "rename replacement inside the restoration window",
			inject: replaceByRename,
			hooks:  restoreWindow,
		},
		{
			name:   "control: in-place edit before the last validation",
			inject: editInPlace,
			hooks:  earlierValidationWindow,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			firstPath := filepath.Join(root, "a.go")
			secondPath := filepath.Join(root, "z.go")
			c.Assert(os.WriteFile(firstPath, firstOriginal, 0o600), qt.IsNil)
			c.Assert(os.WriteFile(secondPath, secondOriginal, 0o600), qt.IsNil)
			c.Assert(os.Chmod(firstPath, 0o640), qt.IsNil)
			c.Assert(os.Chmod(secondPath, 0o640), qt.IsNil)
			snapshot, err := goannotationsource.Capture(root)
			c.Assert(err, qt.IsNil)
			plan, err := NewPlan(snapshot)
			c.Assert(err, qt.IsNil)
			base := applyHooks{
				revalidate: snapshot.Revalidate,
				afterCommit: func() {
					c.Assert(os.WriteFile(secondPath, secondConcurrent, 0o600), qt.IsNil)
				},
			}

			var injectErr error

			applyErr := applyPlans(plan.changes, test.hooks(base, func() {
				injectErr = test.inject(firstPath)
			}))

			c.Assert(injectErr, qt.IsNil)
			c.Assert(applyErr, qt.ErrorIs, ErrRollbackConflict)
			c.Assert(applyErr.Error(), qt.Contains, "preserved backup")
			assertInternalFileBytes(c, firstPath, firstConcurrent)
			assertInternalFileBytes(c, secondPath, secondConcurrent)
			backups, err := filepath.Glob(filepath.Join(root, ".a.go.ptah-backup-*"))
			c.Assert(err, qt.IsNil)
			c.Assert(backups, qt.HasLen, 1)
			assertInternalFileBytes(c, backups[0], firstOriginal)
			c.Assert(applyErr.Error(), qt.Contains, backups[0])
			cleanedFiles, err := filepath.Glob(filepath.Join(root, ".*.ptah-cleaned-*"))
			c.Assert(err, qt.IsNil)
			c.Assert(cleanedFiles, qt.HasLen, 0)
		})
	}
}

func assertInternalFileBytes(c *qt.C, path string, want []byte) {
	c.Helper()
	got, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, want)
}

func internalEntryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}
