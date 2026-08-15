package atlasmigrate

// White-box testing required: the window these rows measure is between the
// checksum commit's capture of the state it intends to replace and the rename
// that replaces it. Nothing in the product needs a name for that instant, and
// no exported entry point offers one -- a fixture that writes over the checksum
// before calling the writer is answered by the capture, which simply observes
// the newer state, and one that writes after the rename has nothing left to
// measure. Everything asserted is otherwise observable: the error the commit
// returns and the bytes left on disk.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestPublishDirSum_RefusesADestinationReplacedInsideTheCommitWindow is the
// atlas.sum commit point stokaro/ptah#1118's acceptance criteria name, and it is
// the one commit point in the writer transaction that is not an exclusive
// create: the checksum is a replacement by construction, so "refuse to
// overwrite" cannot be the exclusive-create rule the migration files rely on.
// It has to be a conditional rename against the state this transaction observed.
//
// Both destination shapes are measured because they take different branches:
// a directory with no checksum yet commits under ExpectAbsent, one that already
// has a checksum under ExpectFile. Either can be defeated separately.
//
// The rows run over both layouts. atlas.sum and ptah.sum are the same commit
// with a different name, and until this change only the Atlas one reached it --
// the paired writers behind `ptah migrations checkpoint` and
// `ptah migrations data` wrote ptah.sum by pathname, unconditionally.
//
// The assertion is the surviving bytes, not the message. A commit that reported
// an error and still replaced the intruder would satisfy an error-only
// assertion and lose the data the criterion exists to protect.
func TestPublishDirSum_RefusesADestinationReplacedInsideTheCommitWindow(t *testing.T) {
	const intruder = "h1:rival=\n0000000001_rival.sql h1:rival=\n"

	tests := []struct {
		name    string
		format  migrator.MigrationDirFormat
		sumName string
		// seed writes whatever checksum the directory already carries, so the
		// commit captures ExpectAbsent or ExpectFile.
		seed func(c *qt.C, dir, sumName string)
	}{
		{
			name:    "atlas.sum, absent when the commit captured its destination",
			format:  migrator.MigrationDirFormatAtlas,
			sumName: migratesum.AtlasFileName,
			seed:    func(*qt.C, string, string) {},
		},
		{
			name:    "atlas.sum, present when the commit captured its destination",
			format:  migrator.MigrationDirFormatAtlas,
			sumName: migratesum.AtlasFileName,
			seed: func(c *qt.C, dir, sumName string) {
				c.Assert(os.WriteFile(
					filepath.Join(dir, sumName), []byte("h1:stale=\n"), 0o600,
				), qt.IsNil)
			},
		},
		{
			name:    "ptah.sum, absent when the commit captured its destination",
			format:  migrator.MigrationDirFormatPtah,
			sumName: migratesum.FileName,
			seed:    func(*qt.C, string, string) {},
		},
		{
			name:    "ptah.sum, present when the commit captured its destination",
			format:  migrator.MigrationDirFormatPtah,
			sumName: migratesum.FileName,
			seed: func(c *qt.C, dir, sumName string) {
				c.Assert(os.WriteFile(
					filepath.Join(dir, sumName), []byte("h1:stale=\n"), 0o600,
				), qt.IsNil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			test.seed(c, dir, test.sumName)
			writer := openTestWriter(c.TB, dir)
			sum, err := migratesum.ComputeWithFormat(os.DirFS(dir), test.format)
			c.Assert(err, qt.IsNil)

			// The hostile step: a rival commits its own checksum after this one
			// captured the destination it intends to replace.
			beforeDirSumCommit = func() {
				c.Assert(os.WriteFile(
					filepath.Join(dir, test.sumName), []byte(intruder), 0o600,
				), qt.IsNil)
			}
			defer func() { beforeDirSumCommit = nil }()

			path, err := publishDirSumAs(writer, test.sumName, sum)

			c.Assert(err, qt.ErrorIs, fsdurable.ErrDestinationChanged)
			c.Assert(path, qt.Equals, "")
			body, readErr := os.ReadFile(filepath.Join(dir, test.sumName))
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(body), qt.Equals, intruder)
			// A refused commit leaves no staged temporary behind either.
			staged, globErr := filepath.Glob(filepath.Join(dir, "."+test.sumName+".*.tmp"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(staged, qt.HasLen, 0)
		})
	}
}

// TestPublishDirSum_CommitsWhenNothingReplacedTheDestination is the
// non-interference control for the rows above.
//
// Without it, a commit that refused unconditionally -- one that never published
// anything at all -- would pass every assertion in this file. The row runs the
// identical setup with the hook left nil, so the only difference is the
// replacement itself.
func TestPublishDirSum_CommitsWhenNothingReplacedTheDestination(t *testing.T) {
	tests := []struct {
		name    string
		format  migrator.MigrationDirFormat
		sumName string
	}{
		{
			name:    "atlas.sum",
			format:  migrator.MigrationDirFormatAtlas,
			sumName: migratesum.AtlasFileName,
		},
		{
			name:    "ptah.sum",
			format:  migrator.MigrationDirFormatPtah,
			sumName: migratesum.FileName,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(
				filepath.Join(dir, test.sumName), []byte("h1:stale=\n"), 0o600,
			), qt.IsNil)
			writer := openTestWriter(c.TB, dir)
			sum, err := migratesum.ComputeWithFormat(os.DirFS(dir), test.format)
			c.Assert(err, qt.IsNil)

			path, err := publishDirSumAs(writer, test.sumName, sum)

			c.Assert(err, qt.IsNil)
			c.Assert(path, qt.Equals, filepath.Join(writer.Path(), test.sumName))
			body, readErr := os.ReadFile(filepath.Join(dir, test.sumName))
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(body), qt.Equals, string(sum.Bytes()))
		})
	}
}
