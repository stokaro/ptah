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

func TestApplyPlans_FailurePath_RevalidatesCompleteSourceSetAfterStaging(t *testing.T) {
	tests := []struct {
		name           string
		mutate         func(c *qt.C, root, plainPath string)
		wantPlain      []byte
		wantEntryNames []string
	}{
		{
			name: "added source",
			mutate: func(c *qt.C, root, _ string) {
				c.Assert(os.WriteFile(
					filepath.Join(root, "new.go"),
					[]byte("package models\n"),
					0o600,
				), qt.IsNil)
			},
			wantPlain:      []byte("package models\n\ntype Plain struct{}\n"),
			wantEntryNames: []string{"annotated.go", "new.go", "plain.go"},
		},
		{
			name: "edited unannotated source",
			mutate: func(c *qt.C, _ string, plainPath string) {
				c.Assert(os.WriteFile(
					plainPath,
					[]byte("package models\n\ntype Other struct{}\n"),
					0o600,
				), qt.IsNil)
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

			err = applyPlans(plan.changes, applyHooks{
				revalidate: func() error {
					test.mutate(c, root, plainPath)
					return snapshot.Revalidate()
				},
				afterCommit: func() {},
			})

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

// TestApplyPlans_FailurePath_RefusesForwardCommitOverConcurrentEdit measures
// the gap between validatePlanPath and the rename that replaces the source.
// Before the commit became conditional the injected edit was destroyed with no
// error at all, so the assertion is on the surviving bytes rather than on the
// exit status.
//
// The control row runs the identical mutation one step earlier, where
// validatePlanPath already rejects it.
func TestApplyPlans_FailurePath_RefusesForwardCommitOverConcurrentEdit(t *testing.T) {
	original := []byte(
		"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
	)
	rival := []byte("package models\n\n// hand edit by another writer\ntype User struct{}\n")
	editInPlace := func(c *qt.C, sourcePath string) {
		c.Assert(os.WriteFile(sourcePath, rival, 0o600), qt.IsNil)
	}
	replaceByRename := func(c *qt.C, sourcePath string) {
		replacement := sourcePath + ".rival"
		c.Assert(os.WriteFile(replacement, rival, 0o600), qt.IsNil)
		c.Assert(os.Rename(replacement, sourcePath), qt.IsNil)
	}
	commitWindow := func(revalidate func() error, inject func()) applyHooks {
		return applyHooks{revalidate: revalidate, beforeCommit: inject}
	}
	validationWindow := func(_ func() error, inject func()) applyHooks {
		return applyHooks{revalidate: func() error {
			inject()
			return nil
		}}
	}
	tests := []struct {
		name   string
		inject func(c *qt.C, sourcePath string)
		hooks  func(revalidate func() error, inject func()) applyHooks
		check  func(c *qt.C, err error)
	}{
		{
			name:   "in-place edit inside the commit window",
			inject: editInPlace,
			hooks:  commitWindow,
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.ErrorIs, fsdurable.ErrDestinationChanged)
			},
		},
		{
			name:   "rename replacement inside the commit window",
			inject: replaceByRename,
			hooks:  commitWindow,
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.ErrorIs, fsdurable.ErrDestinationChanged)
			},
		},
		{
			name:   "control: in-place edit before the last validation",
			inject: editInPlace,
			hooks:  validationWindow,
			check: func(c *qt.C, err error) {
				c.Assert(err.Error(), qt.Contains, "go source changed before cleanup")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			sourcePath := filepath.Join(root, "model.go")
			c.Assert(os.WriteFile(sourcePath, original, 0o600), qt.IsNil)
			snapshot, err := goannotationsource.Capture(root)
			c.Assert(err, qt.IsNil)
			plan, err := NewPlan(snapshot)
			c.Assert(err, qt.IsNil)

			applyErr := applyPlans(plan.changes, test.hooks(snapshot.Revalidate, func() {
				test.inject(c, sourcePath)
			}))

			c.Assert(applyErr, qt.IsNotNil)
			c.Assert(applyErr, qt.Not(qt.ErrorIs), fsdurable.ErrReplacementCommitted)
			test.check(c, applyErr)
			assertInternalFileBytes(c, sourcePath, rival)
			entries, err := os.ReadDir(root)
			c.Assert(err, qt.IsNil)
			c.Assert(internalEntryNames(entries), qt.DeepEquals, []string{"model.go"})
		})
	}
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
	editInPlace := func(c *qt.C, path string) {
		c.Assert(os.WriteFile(path, firstConcurrent, 0o600), qt.IsNil)
	}
	replaceByRename := func(c *qt.C, path string) {
		replacement := path + ".rival"
		c.Assert(os.WriteFile(replacement, firstConcurrent, 0o600), qt.IsNil)
		c.Assert(os.Rename(replacement, path), qt.IsNil)
	}
	restoreWindow := func(base applyHooks, injectFirst func()) applyHooks {
		base.beforeRestore = injectFirst
		return base
	}
	validationWindow := func(base applyHooks, injectFirst func()) applyHooks {
		failSecond := base.afterCommit
		base.afterCommit = func() {
			failSecond()
			injectFirst()
		}
		return base
	}
	tests := []struct {
		name   string
		inject func(c *qt.C, path string)
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
			hooks:  validationWindow,
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

			applyErr := applyPlans(plan.changes, test.hooks(base, func() {
				test.inject(c, firstPath)
			}))

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
