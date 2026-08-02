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
	c := qt.New(t)
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
		c.Run(test.name, func(c *qt.C) {
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
