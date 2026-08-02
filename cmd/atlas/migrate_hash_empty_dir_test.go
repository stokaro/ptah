package atlas_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

func TestMigrateHashRejectsExplicitEmptyDirectoryWithoutWritingSum(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	cmd := atlas.NewCompatCommand("atlas")
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"migrate", "hash", "--dir="})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `migrations directory : stat : no such file or directory`)
	_, statErr := os.Stat(filepath.Join(root, "atlas.sum"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
