package migraterepair_test

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migraterepair"
	"ptah.run/internal/cli/migrateup"
)

const repairRepeatableView = "DROP VIEW IF EXISTS widget_ids;\nCREATE VIEW widget_ids AS SELECT id FROM widgets;\n"

// appliedRepeatableDirectory applies a directory of one versioned migration
// and the repeatable migration R__widget_ids.sql to a new SQLite database, and
// returns the directory and the database path.
func appliedRepeatableDirectory(c *qt.C) (dir, dbPath string) {
	c.Helper()
	dir = c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "R__widget_ids.sql"), []byte(repairRepeatableView), 0o600), qt.IsNil)
	dbPath = filepath.Join(c.TempDir(), "app.db")
	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}

// repairStdout runs `migrations repair` with args and returns its standard
// output alone.
func repairStdout(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := migraterepair.NewMigrateRepairCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), err
}

// recordRows counts the rows of the repeatable migration record at dbPath.
func recordRows(c *qt.C, dbPath string) int {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var count int
	c.Assert(db.QueryRow("SELECT COUNT(*) FROM schema_migrations_repeatable").Scan(&count), qt.IsNil)
	return count
}

func TestMigrateRepairRepeatable_HappyPath(t *testing.T) {
	t.Run("the row of a deleted file is removed without force", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := appliedRepeatableDirectory(c)
		c.Assert(os.Remove(filepath.Join(dir, "R__widget_ids.sql")), qt.IsNil)

		out, err := repairStdout(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--repeatable", "widget_ids")

		c.Assert(err, qt.IsNil)
		c.Assert(out, qt.Equals, "Removed the record of repeatable migration widget_ids, state orphaned; "+
			"no file of the directory carries it; no migration SQL ran\n")
		c.Assert(recordRows(c, dbPath), qt.Equals, 0)
	})

	t.Run("the row of a file still in the directory is removed with force", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := appliedRepeatableDirectory(c)

		out, err := repairStdout(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--repeatable", "widget_ids", "--force")

		c.Assert(err, qt.IsNil)
		c.Assert(out, qt.Equals, "Removed the record of repeatable migration widget_ids, state applied; "+
			"its file is still in the directory, and the next up run executes it as new; no migration SQL ran\n")
		c.Assert(recordRows(c, dbPath), qt.Equals, 0)
	})
}

func TestMigrateRepairRepeatable_FailurePath(t *testing.T) {
	t.Run("the row of a file still in the directory is kept without force", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := appliedRepeatableDirectory(c)

		out, err := repairStdout(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--repeatable", "widget_ids")

		c.Assert(err, qt.ErrorMatches, `a file of the directory still carries this repeatable migration: `+
			`R__widget_ids.sql carries widget_ids, .*rerun with --force to remove the row anyway`)
		c.Assert(out, qt.Equals, "")
		c.Assert(recordRows(c, dbPath), qt.Equals, 1)
	})

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "a version and a repeatable identity",
			args:    []string{"--version", "1", "--repeatable", "widget_ids"},
			wantErr: "--version and --repeatable select different repairs; give one of them",
		},
		{
			name:    "a resumed statement of a repeatable migration",
			args:    []string{"--repeatable", "widget_ids", "--resume-from", "2"},
			wantErr: "--resume-from resumes a versioned migration; .*'ptah migrations up --allow-dirty'",
		},
		{
			name:    "an identity no file name could carry",
			args:    []string{"--repeatable", "widget.ids"},
			wantErr: `invalid repeatable migration identity "widget.ids": .*`,
		},
		{
			name:    "neither a version nor an identity",
			args:    nil,
			wantErr: "migration version is required, or --repeatable for a repeatable migration's record",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := appliedRepeatableDirectory(c)
			args := append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir}, tc.args...)

			out, err := repairStdout(c, args...)

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(out, qt.Equals, "")
			c.Assert(recordRows(c, dbPath), qt.Equals, 1)
		})
	}
}
