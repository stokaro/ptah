package migrate

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrateNewCommandCreatesSkeletonFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	cmd := newMigrateNewCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"add_user_preferences", "--migrations-dir", dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generated empty migration files:")
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_add_user_preferences.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)

	upBytes, readErr := os.ReadFile(matches[0])
	c.Assert(readErr, qt.IsNil)
	content := string(upBytes)
	c.Assert(content, qt.Contains, "-- Migration: add_user_preferences")
	c.Assert(content, qt.Contains, "-- Add your migration SQL here.")
}

func TestMigrateNewCommandAcceptsNameFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	cmd := newMigrateNewCommand()
	cmd.SetArgs([]string{"--name", "manual_hotfix", "--migrations-dir", dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)
}

func TestMigrateNewCommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		err  string
	}{
		{
			name: "missing name",
			args: []string{"--migrations-dir", t.TempDir()},
			err:  "migration name is required",
		},
		{
			name: "missing migrations dir",
			args: []string{"manual_hotfix"},
			err:  "migrations directory is required",
		},
		{
			name: "name argument and flag conflict",
			args: []string{"manual_hotfix", "--name", "other", "--migrations-dir", t.TempDir()},
			err:  "migration name must be provided either as an argument or --name, not both",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := newMigrateNewCommand()
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.err)
		})
	}
}

func TestAddMigrateNewCommandIsIdempotent(t *testing.T) {
	c := qt.New(t)

	cmd := NewMigrateCommand()
	addMigrateNewCommand(cmd)
	addMigrateNewCommand(cmd)

	count := 0
	for _, child := range cmd.Commands() {
		if child.Name() == "new" {
			count++
		}
	}
	c.Assert(count, qt.Equals, 1)
}
