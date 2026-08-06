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

	// An empty --dir carries no scheme, so it is refused by the scheme gate
	// rather than by the directory lookup behind it. That is what the pinned
	// community binary v1.3.0 does with the same spelling, measured 2026-08-06:
	// `migrate hash --dir=` exits 1 with this line, where Ptah used to reach the
	// stat and report `migrations directory : stat : no such file or directory`
	// (stokaro/ptah#1186). What the test is really about is below: nothing is
	// written either way.
	c.Assert(err, qt.ErrorMatches, `missing scheme for dir url\. Did you mean "file://"\?`)
	_, statErr := os.Stat(filepath.Join(root, "atlas.sum"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
