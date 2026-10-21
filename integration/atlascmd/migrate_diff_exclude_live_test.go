//go:build integration

package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

func TestMigrateDiffProjectExcludePreservesFullReplayPostgresLive(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	devURL := newCleanGateDatabase(c, adminURL, nil)
	targetURL := newCleanGateDatabase(c, adminURL, nil)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	original := "CREATE TABLE manual_parent (id integer PRIMARY KEY);\nINSERT INTO manual_parent VALUES (8);\nCREATE TABLE widgets (id integer PRIMARY KEY);\n"
	first := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(first, []byte(original), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	source := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(source, []byte("CREATE TABLE manual_parent (id integer PRIMARY KEY) PARTITION BY HASH (id);\nCREATE TABLE widgets (id integer PRIMARY KEY, note text);\n"), 0o600), qt.IsNil)
	project := filepath.Join(root, "atlas.hcl")
	c.Assert(os.WriteFile(project, []byte("env \"authoring\" {\n dev = "+strconv.Quote(devURL)+"\n src = \"file://schema.sql\"\n exclude = [\"manual_parent\"]\n migration { dir = \"file://migrations\" }\n}\n"), 0o600), qt.IsNil)
	out, err := runExcludeProjectCommand(c, project, "migrate", "diff", "add_note", "--env", "authoring")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	firstAfter, err := os.ReadFile(first)
	c.Assert(err, qt.IsNil)
	c.Assert(string(firstAfter), qt.Equals, original)
	out, err = runExcludeProjectCommand(c, project, "migrate", "apply", "--dir", "file://"+filepath.ToSlash(dir), "--url", targetURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	var preservedID, columnCount int
	c.Assert(target.QueryRowContext(c.Context(), "SELECT id FROM manual_parent").Scan(&preservedID), qt.IsNil)
	c.Assert(preservedID, qt.Equals, 8)
	c.Assert(target.QueryRowContext(c.Context(), "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'note'").Scan(&columnCount), qt.IsNil)
	c.Assert(columnCount, qt.Equals, 1)
	out, err = runExcludeProjectCommand(c, project, "migrate", "diff", "already_synced", "--env", "authoring")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	filesAfter, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(filesAfter, qt.DeepEquals, files)
}

func runExcludeProjectCommand(c *qt.C, project string, args ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetContext(c.Context())
	cmd.SetArgs(append([]string{"--config", "file://" + filepath.ToSlash(project)}, args...))
	err := cmd.Execute()
	return output.String(), err
}
