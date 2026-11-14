package atlasreport_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasreport"
	"ptah.run/migration/migrator"
)

// applyTextFile is one migration file of a text report fixture.
type applyTextFile struct {
	version int64
	name    string
	body    string
}

// applyTextOptions builds the options for a run over files on SQLite. The run
// lasts exactly one second, so every duration the report prints is `1s`.
func applyTextOptions(c *qt.C, files []applyTextFile, current int64, selected []int64) atlasreport.MigrateApplyResultOptions {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), "text.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	fsys := fstest.MapFS{}
	migrations := make([]*migrator.Migration, 0, len(files))
	for _, file := range files {
		fsys[file.name] = &fstest.MapFile{Data: []byte(file.body)}
		migrations = append(migrations, &migrator.Migration{Version: file.version, UpSQL: file.body})
	}
	return atlasreport.MigrateApplyResultOptions{
		Conn:             conn,
		FS:               fsys,
		Dir:              "file://migrations",
		URL:              "sqlite://text.db",
		Status:           &migrator.MigrationStatus{CurrentVersion: current, PendingMigrations: selected},
		Migrations:       migrations,
		SelectedVersions: selected,
		CurrentVersion:   current,
		StartedAt:        time.Unix(100, 0).UTC(),
		EndedAt:          time.Unix(101, 0).UTC(),
		Applied:          len(selected) > 0,
	}
}

// The success fixture is the directory the pinned community binary v1.3.0 was
// measured on: a file with a statement over several lines, which the report
// indents under its arrow, and a file with two statements.
var applyTextSuccessFiles = []applyTextFile{
	{version: 1, name: "1.sql", body: "CREATE TABLE users (id int);\n"},
	{version: 2, name: "2.sql", body: "ALTER TABLE users ADD COLUMN name text;\nCREATE INDEX users_name ON users (name);\n"},
	{version: 3, name: "3.sql", body: "-- a comment above\nCREATE TABLE t3 (\n  id int,\n  v text\n);\nINSERT INTO t3 VALUES (1, 'a;b');\n"},
}

// TestWriteMigrateApplyText_HappyPath pins the report for runs that either
// succeed or have nothing to do. The texts are the community binary's for the
// same directories, with its durations replaced by the fixture's one second.
func TestWriteMigrateApplyText_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		files    []applyTextFile
		current  int64
		selected []int64
		want     string
	}{
		{
			name:     "every migration of an empty database",
			files:    applyTextSuccessFiles,
			selected: []int64{1, 2, 3},
			want: "Migrating to version 3 (3 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> ALTER TABLE users ADD COLUMN name text;\n" +
				"    -> CREATE INDEX users_name ON users (name);\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -- migrating version 3\n" +
				"    -> CREATE TABLE t3 (\n" +
				"         id int,\n" +
				"         v text\n" +
				"       );\n" +
				"    -> INSERT INTO t3 VALUES (1, 'a;b');\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1s\n" +
				"  -- 3 migrations\n" +
				"  -- 5 sql statements\n",
		},
		{
			name:     "one migration over an applied one names the version it moves from",
			files:    applyTextSuccessFiles[:2],
			current:  1,
			selected: []int64{2},
			want: "Migrating to version 2 from 1 (1 migrations in total):\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> ALTER TABLE users ADD COLUMN name text;\n" +
				"    -> CREATE INDEX users_name ON users (name);\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1s\n" +
				"  -- 1 migration\n" +
				"  -- 2 sql statements\n",
		},
		{
			name:    "nothing pending",
			files:   applyTextSuccessFiles[:1],
			current: 1,
			want:    "No migration files to execute\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := applyTextOptions(c, test.files, test.current, test.selected)
			var out bytes.Buffer

			err := atlasreport.WriteMigrateApplyText(&out, opts)

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// A statement Ptah refused before sending it is not in the file's list, so the
// summary counts what the database saw: the statement before it passed, and
// the refused one failed. The community binary takes the failed statement off
// its passed count because its list always ends with it; counting the same way
// here would report no statement passed.
func TestWriteMigrateApplyText_UnsentRefusalCountsWhatTheDatabaseSaw(t *testing.T) {
	c := qt.New(t)
	opts := applyTextOptions(c, []applyTextFile{
		{version: 1, name: "1.sql", body: "CREATE TABLE a (id int);\nCREATE TABLE b (id int);\n"},
	}, 0, []int64{1})
	opts.ApplyError = &migrator.MigrationExecutionError{
		Err:            errors.New("refused before sending"),
		Statement:      "CREATE TABLE b (id int)",
		StatementIndex: 2,
		Total:          2,
	}
	var out bytes.Buffer

	err := atlasreport.WriteMigrateApplyText(&out, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Migrating to version 1 (1 migrations in total):\n"+
		"\n"+
		"  -- migrating version 1\n"+
		"    -> CREATE TABLE a (id int);\n"+
		"    refused before sending\n"+
		"\n"+
		"  -------------------------\n"+
		"  -- 1s\n"+
		"  -- 1 migration with errors\n"+
		"  -- 1 sql statement ok, 1 with errors\n")
}

// checkDirectiveFile is a migration carrying a `-- +ptah check` directive,
// which runs before the file's statements.
var checkDirectiveFile = applyTextFile{
	version: 1,
	name:    "1.sql",
	body:    "-- +ptah check assert=\"SELECT 1\"\nCREATE TABLE users (id int);\n",
}

// A check that passed is listed before the statements it guards. The community
// binary runs no pre-migration checks, so these lines are Ptah's own. A check
// prints no duration, because the run does not time checks one by one.
func TestWriteMigrateApplyText_ListsAPassedCheck(t *testing.T) {
	c := qt.New(t)
	opts := applyTextOptions(c, []applyTextFile{checkDirectiveFile}, 0, []int64{1})
	var out bytes.Buffer

	err := atlasreport.WriteMigrateApplyText(&out, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Migrating to version 1 (1 migrations in total):\n"+
		"\n"+
		"  -- pre-migration checks of version 1\n"+
		"    -> SELECT 1\n"+
		"  -- ok\n"+
		"  -- migrating version 1\n"+
		"    -> CREATE TABLE users (id int);\n"+
		"  -- ok (1s)\n"+
		"\n"+
		"  -------------------------\n"+
		"  -- 1s\n"+
		"  -- 1 migration\n"+
		"  -- 1 check\n"+
		"  -- 1 sql statement\n")
}

// A check the run did not evaluate -- a dry run defers it, PTAH_SKIP_CHECKS
// bypasses it -- is left out rather than reported as passed.
func TestWriteMigrateApplyText_LeavesOutChecksNotRun(t *testing.T) {
	c := qt.New(t)
	opts := applyTextOptions(c, []applyTextFile{checkDirectiveFile}, 0, []int64{1})
	opts.ChecksNotRun = []int64{1}
	var out bytes.Buffer

	err := atlasreport.WriteMigrateApplyText(&out, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Migrating to version 1 (1 migrations in total):\n"+
		"\n"+
		"  -- migrating version 1\n"+
		"    -> CREATE TABLE users (id int);\n"+
		"  -- ok (1s)\n"+
		"\n"+
		"  -------------------------\n"+
		"  -- 1s\n"+
		"  -- 1 migration\n"+
		"  -- 1 sql statement\n")
}

// A txtar check that refused stops its file before any statement: the report
// marks the assertion, prints the refusal, and lists no statement. The
// migrator names the check after its section and position, `checks.sql#1`,
// and the mark has to land on the `checks.sql` group all the same.
func TestWriteMigrateApplyText_MarksTheTxtarCheckThatRefused(t *testing.T) {
	c := qt.New(t)
	migrations, fsys := txtarCheckMigrations(c)
	opts := applyTextOptions(c, nil, 0, []int64{1})
	opts.FS = fsys
	opts.Migrations = migrations
	refusal := &migrator.CheckFailedError{
		Version: 1,
		Name:    "checks.sql#1",
		Assert:  "SELECT NOT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'users')",
	}
	opts.ApplyError = refusal
	var out bytes.Buffer

	err := atlasreport.WriteMigrateApplyText(&out, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Migrating to version 1 (1 migrations in total):\n"+
		"\n"+
		"  -- pre-migration checks of version 1 (checks.sql)\n"+
		"    -> SELECT NOT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'users')\n"+
		"    "+refusal.Error()+"\n"+
		"\n"+
		"  -------------------------\n"+
		"  -- 1s\n"+
		"  -- 1 migration with errors\n"+
		"  -- 1 check with errors\n")
}

// txtarMigrationsFrom builds one txtar migration the way the migrator does, so
// its check sections are what a real run reports.
func txtarMigrationsFrom(c *qt.C, body string) ([]*migrator.Migration, fstest.MapFS) {
	c.Helper()
	fsys := fstest.MapFS{"1_checked.sql": {Data: []byte(body)}}
	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	return migrations, fsys
}

// threeChecksTxtar is a migration whose checks.sql holds three assertions.
const threeChecksTxtar = "-- atlas:txtar\n\n" +
	"-- checks.sql --\n" +
	"SELECT 1;\nSELECT 2;\nSELECT 3;\n\n" +
	"-- migration.sql --\n" +
	"CREATE TABLE users (id int);\n"

// threeGroupsTxtar is a migration with three check files of one assertion each.
const threeGroupsTxtar = "-- atlas:txtar\n\n" +
	"-- checks/a.sql --\n" +
	"SELECT 1;\n\n" +
	"-- checks/b.sql --\n" +
	"SELECT 2;\n\n" +
	"-- checks/c.sql --\n" +
	"SELECT 3;\n\n" +
	"-- migration.sql --\n" +
	"CREATE TABLE users (id int);\n"

// A refused check stops its group and every group after it, so the report
// lists the checks that ran and the summary counts those alone. A group
// refusal names no single check: every check of the group ran and none held.
func TestWriteMigrateApplyText_CountsTheChecksThatRan(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		applyErr error
		want     string
	}{
		{
			name:     "the checks after a refused one did not run",
			body:     threeChecksTxtar,
			applyErr: &migrator.CheckFailedError{Version: 1, Name: "checks.sql#2", Assert: "SELECT 2"},
			want: "Migrating to version 1 (1 migrations in total):\n" +
				"\n" +
				"  -- pre-migration checks of version 1 (checks.sql)\n" +
				"    -> SELECT 1\n" +
				"    -> SELECT 2\n" +
				"    pre-migration check checks.sql#2 for migration 1 was not satisfied (assert: SELECT 2)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1s\n" +
				"  -- 1 migration with errors\n" +
				"  -- 1 check ok, 1 with errors\n",
		},
		{
			name:     "a group refusal lists every check of the group",
			body:     threeChecksTxtar,
			applyErr: &migrator.CheckGroupFailedError{Version: 1, Name: "checks.sql", Assertions: 3},
			want: "Migrating to version 1 (1 migrations in total):\n" +
				"\n" +
				"  -- pre-migration checks of version 1 (checks.sql)\n" +
				"    -> SELECT 1\n" +
				"    -> SELECT 2\n" +
				"    -> SELECT 3\n" +
				"    pre-migration check group checks.sql for migration 1 was not satisfied: none of 3 assertions passed\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1s\n" +
				"  -- 1 migration with errors\n" +
				"  -- 3 checks with errors\n",
		},
		{
			name:     "the groups after a refused group did not run",
			body:     threeGroupsTxtar,
			applyErr: &migrator.CheckFailedError{Version: 1, Name: "checks/b.sql#1", Assert: "SELECT 2"},
			want: "Migrating to version 1 (1 migrations in total):\n" +
				"\n" +
				"  -- pre-migration checks of version 1 (checks/a.sql)\n" +
				"    -> SELECT 1\n" +
				"  -- ok\n" +
				"  -- pre-migration checks of version 1 (checks/b.sql)\n" +
				"    -> SELECT 2\n" +
				"    pre-migration check checks/b.sql#1 for migration 1 was not satisfied (assert: SELECT 2)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1s\n" +
				"  -- 1 migration with errors\n" +
				"  -- 1 check ok, 1 with errors\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrations, fsys := txtarMigrationsFrom(c, test.body)
			opts := applyTextOptions(c, nil, 0, []int64{1})
			opts.FS = fsys
			opts.Migrations = migrations
			opts.ApplyError = test.applyErr
			var out bytes.Buffer

			err := atlasreport.WriteMigrateApplyText(&out, opts)

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// The default report's helpers are reachable from a --format template, so a
// template written against the community binary's report data runs here too.
func TestWriteMigrateApplyFormat_ReadsTheDefaultReportHelpers(t *testing.T) {
	c := qt.New(t)
	opts := applyTextOptions(c, applyTextSuccessFiles[:2], 1, []int64{2})
	var out bytes.Buffer

	err := atlasreport.WriteMigrateApplyFormat(&out,
		`{{ .Header }}|{{ .Summary "" }}|{{ range .Applied }}{{ .File.Version }}{{ end }}`, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals,
		"Migrating to version 2 from 1 (1 migrations in total):|-- 1s\n-- 1 migration\n-- 2 sql statements|2")
}

// A --format template may color and indent what it prints with the helpers a
// template for the community binary's report calls. Ptah writes no color, so
// each color helper returns its text as it is, and indent_ln indents every line
// after the first.
func TestWriteMigrateApplyFormat_RunsTheColorAndIndentHelpers(t *testing.T) {
	c := qt.New(t)
	opts := applyTextOptions(c, applyTextSuccessFiles, 2, []int64{3})
	var out bytes.Buffer

	err := atlasreport.WriteMigrateApplyFormat(&out,
		`{{ range .Applied }}{{ cyan .File.Version }} {{ yellow "ok" }}`+
			`{{ range .Applied }}|{{ red (indent_ln . 2) }}{{ end }}{{ end }}`+
			`|{{ green "done" }}|{{ redBgWhiteFg "refused" }}`, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals,
		"3 ok|CREATE TABLE t3 (\n    id int,\n    v text\n  );|INSERT INTO t3 VALUES (1, 'a;b');|done|refused")
}

// errWriteRefused is what failingWriter answers every write with.
var errWriteRefused = errors.New("write refused")

// failingWriter refuses every write.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errWriteRefused }

func TestWriteMigrateApplyText_FailurePath(t *testing.T) {
	t.Run("the writer refuses the report", func(t *testing.T) {
		c := qt.New(t)
		opts := applyTextOptions(c, applyTextSuccessFiles[:1], 0, []int64{1})

		err := atlasreport.WriteMigrateApplyText(failingWriter{}, opts)

		c.Assert(err, qt.ErrorIs, errWriteRefused)
	})

	t.Run("no migration filesystem", func(t *testing.T) {
		c := qt.New(t)
		opts := applyTextOptions(c, applyTextSuccessFiles[:1], 0, []int64{1})
		opts.FS = nil
		var out bytes.Buffer

		err := atlasreport.WriteMigrateApplyText(&out, opts)

		c.Assert(err, qt.ErrorMatches, "migrate apply format requires migration filesystem")
		c.Assert(out.String(), qt.Equals, "")
	})
}
