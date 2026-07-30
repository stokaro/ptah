package goannotationcleanup

// White-box testing required: this file injects source mutations at the
// unexported post-staging commit barrier, which public APIs cannot observe
// deterministically without timing-dependent filesystem polling.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/goannotationsource"
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

			err = applyPlans(plan.changes, func() error {
				test.mutate(c, root, plainPath)
				return snapshot.Revalidate()
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
