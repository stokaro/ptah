//go:build unix

package migrationlintreport_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/migrationlintreport"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestBuild_GitBaseAcceptsARepositoryReachedThroughASymlink pins that the
// containment check compares directories rather than spellings of them.
//
// git answers `rev-parse --show-toplevel` with the real path while
// filepath.Abs keeps whatever the caller wrote, so a repository reached through
// a symlink made filepath.Rel return "../../link/migrations" and a directory
// plainly inside the repository was refused as outside it.
//
// The other tests in this file call filepath.EvalSymlinks on t.TempDir()
// themselves, which is what kept this invisible: they hand both sides the same
// spelling before the code can disagree about it. This one deliberately does
// not.
//
// Windows meets the same defect by another road -- the process reports the 8.3
// short name C:\Users\RUNNER~1\... where git reports C:/Users/runneradmin/... --
// and that is what windows-latest reported. It is unix-tagged because creating
// a symlink there needs a privilege the runner may not have, not because the
// bug is unix's.
func TestBuild_GitBaseAcceptsARepositoryReachedThroughASymlink(t *testing.T) {
	c := qt.New(t)
	realRoot, err := filepath.EvalSymlinks(t.TempDir())
	c.Assert(err, qt.IsNil)
	repo := filepath.Join(realRoot, "repo")
	c.Assert(os.Mkdir(repo, 0o700), qt.IsNil)
	migrationsDir := filepath.Join(repo, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o700), qt.IsNil)
	runGit(c, repo, "init")
	runGit(c, repo, "config", "commit.gpgsign", "false")
	runGit(c, repo, "config", "user.email", "ptah@example.com")
	runGit(c, repo, "config", "user.name", "Ptah Test")
	writeLintTestFile(c, migrationsDir, "1_create_users.sql", "CREATE TABLE users (id int);\n")
	runGit(c, repo, "add", "migrations")
	runGit(c, repo, "commit", "-m", "baseline")
	writeLintTestFile(c, migrationsDir, "2_drop_users.sql", "DROP TABLE users;\n")
	runGit(c, repo, "add", "migrations/2_drop_users.sql")
	runGit(c, repo, "commit", "-m", "second")

	// The repository is reached only through the link from here on.
	link := filepath.Join(realRoot, "link")
	c.Assert(os.Symlink(repo, link), qt.IsNil)
	linkedMigrations := filepath.Join(link, "migrations")

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       linkedMigrations,
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		Dialect:   "sqlite",
		GitBase:   "HEAD~1",
		GitDir:    linkedMigrations,
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			GitBase: true,
			GitDir:  true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Versions, qt.DeepEquals, []int64{2})
}
