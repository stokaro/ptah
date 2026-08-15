//go:build !windows

package atlasmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
)

// This is stokaro/ptah#895's own reproduction, kept as a regression test.
//
// It differs from the rows in diff_root_unix_test.go in the shape of the
// replacement, and that difference is the point. Those rows put a symlink at
// the --dir pathname up front and re-point it; here --dir names a real
// directory when the run binds it, and the directory itself is then MOVED --
// renamed aside, with a symlink to a decoy dropped into the name it vacated.
//
// A writer that carries the pathname cannot tell the two apart: both leave the
// same string resolving somewhere else. A writer that carries the opened handle
// survives both, but for different reasons -- a re-pointed symlink never
// touched the object it opened, whereas here the object it opened is the one
// that moved, and the handle has to follow it.
//
// The decoy is seeded with a live copy of the migration directory, staged
// temporary file included, rather than being left empty. That is deliberate:
// against an empty decoy a pathname writer merely errors out on a staging file
// it can no longer find, which is a visible failure. Against a copy it succeeds
// -- exit 0, a migration and an atlas.sum written somewhere the operator never
// named -- and silent success is the defect the issue reported.
//
// The swap runs from PreparePublication, the last callback before anything is
// published: the lock is held, recovery has run, the snapshot has been captured
// and verified, and the batch is already staged. Everything the run writes after
// that point -- the migration file, atlas.sum, the journal, the commit marker
// and any rollback -- is therefore decided entirely by whether the writer still
// means the directory it opened.
//
// Unix only. The fixture renames a directory that the run holds open, which
// POSIX permits and Win32 refuses unless the handle carries FILE_SHARE_DELETE.
// The guarantee under test is that a retained handle keeps naming the same
// object across such a rename; on Windows the rename is denied outright, so the
// hostile step cannot be performed and there is nothing to measure.
func TestGenerateDiff_RenamedDirectoryCannotRedirectPublication(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		// swap replaces the migration directory from inside the callback. The
		// control leaves the tree alone, so the same assertions must hold with
		// and without the hostile step -- that is what makes "the decoy received
		// nothing" evidence rather than an artifact of reading the wrong tree.
		swap func(c *qt.C, migrations, retained, decoy string)
		// published names the directory that must receive the run's output.
		published func(migrations, retained string) string
	}{
		{
			name: "directory renamed aside and replaced by a symlink to a live copy",
			swap: func(c *qt.C, migrations, retained, decoy string) {
				// The copy carries the staged temporary file along with the
				// migrations. That is what lets a pathname writer complete its
				// run against the decoy instead of failing on a staging file it
				// can no longer find.
				c.Assert(os.CopyFS(decoy, os.DirFS(migrations)), qt.IsNil)
				c.Assert(os.Rename(migrations, retained), qt.IsNil)
				c.Assert(os.Symlink(decoy, migrations), qt.IsNil)
			},
			published: func(_, retained string) string { return retained },
		},
		{
			name:      "control: nothing is replaced",
			swap:      func(*qt.C, string, string, string) {},
			published: func(migrations, _ string) string { return migrations },
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			root := c.TempDir()
			migrations := filepath.Join(root, "migrations")
			retained := filepath.Join(root, "migrations.moved")
			decoy := filepath.Join(root, "decoy")
			c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
			c.Assert(os.MkdirAll(decoy, 0o755), qt.IsNil)
			// The directory already holds a migration, so a run that follows the
			// replacement still passes every version and checksum check on the
			// way in and fails only where the write lands.
			c.Assert(os.WriteFile(
				filepath.Join(migrations, "1_init.sql"),
				[]byte("CREATE TABLE legacy (id INTEGER PRIMARY KEY);\n"),
				0o600,
			), qt.IsNil)
			schemaPath := filepath.Join(root, "schema.sql")
			c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE legacy (id INTEGER PRIMARY KEY);
CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL DEFAULT '');
`), 0o600), qt.IsNil)
			conn := connectSQLite(c, filepath.Join(root, "dev.db"))
			defer dbschema.CloseAndWarn(conn)

			prepared := 0
			var decoyAfterSwap []string
			result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
				Dir:         migrations,
				Root:        openDiffProjectRoot(c, root),
				Desired:     localDesiredSet(c, "file://"+schemaPath),
				Name:        "add_users",
				LockTimeout: time.Second,
				PreparePublication: func([]string) error {
					prepared++
					test.swap(c, migrations, retained, decoy)
					decoyAfterSwap = dirEntryNames(c, decoy)
					return nil
				},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(prepared, qt.Equals, 1)
			c.Assert(result.MigrationPaths, qt.HasLen, 1)
			// The run published into the object it opened.
			published := test.published(migrations, retained)
			c.Assert(atlasSQLFiles(c, published), qt.HasLen, 2)
			c.Assert(fileExists(filepath.Join(published, "atlas.sum")), qt.IsTrue)
			// The decoy holds exactly what the swap left in it: no migration, no
			// checksum, no journal and no recovery artifact reached it.
			c.Assert(dirEntryNames(c, decoy), qt.DeepEquals, decoyAfterSwap)
		})
	}
}
