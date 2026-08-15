package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// gooseDirectiveRow is one source directory shape, measured on the pinned
// community binary (v1.3.0) and asserted identically on every verb that reaches
// the directive parser.
//
// Package-level coverage of the parser lives in
// internal/atlasmigrateimport/goosedirectives_test.go. These rows exist because
// the three call sites are what users actually hit, and each reaches LoadFS by a
// different route: migrate apply via ConvertApplySource, migrate validate via
// ResolveApplySourceForFormat, and migrate import via LoadDir.
type gooseDirectiveRow struct {
	name      string
	format    string
	file      string
	assertErr func(c *qt.C, err error, out string)
	assertDB  func(c *qt.C, dbPath string)
}

func gooseDirectiveAccepted() func(c *qt.C, err error, out string) {
	return func(c *qt.C, err error, out string) {
		c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	}
}

func gooseDirectiveRefused() func(c *qt.C, err error, out string) {
	return func(c *qt.C, err error, out string) {
		c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
	}
}

func gooseDirectiveNoTables(tables ...string) func(c *qt.C, dbPath string) {
	return func(c *qt.C, dbPath string) {
		for _, table := range tables {
			c.Assert(sqliteTableCount(c.TB, dbPath, table), qt.Equals, 0, qt.Commentf("table %s", table))
		}
	}
}

// gooseDirectiveRows is the discriminating table for stokaro/ptah#981.
//
// The first two rows are the pair that discriminates, and neither works alone.
// "no directives" alone is passed by the naive remedy ("if no Up was found,
// execute the raw file"), which then ships a rollback-execution bug. "body then
// Down" alone passes without any change at all. Together they admit only a
// parser that knows the difference between a file with NO directives and a file
// with a BROKEN set of them.
func gooseDirectiveRows() []gooseDirectiveRow {
	const widgets = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"

	return []gooseDirectiveRow{
		{
			// #981: the community binary executes this, creates widgets, and
			// records the revision. Ptah refused it on all three verbs.
			name:      "no goose directives executes the body",
			format:    "goose",
			file:      widgets,
			assertErr: gooseDirectiveAccepted(),
			assertDB: func(c *qt.C, dbPath string) {
				c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
			},
		},
		{
			// The naive remedy exits 0 here AND RUNS THE DROP. The community
			// binary refuses, and so must Ptah: a Down with no Up is a broken
			// directive set, not a directive-free file.
			name:      "body followed by a Down section is refused",
			format:    "goose",
			file:      widgets + "-- +goose Down\nDROP TABLE widgets;\n",
			assertErr: gooseDirectiveRefused(),
			assertDB:  gooseDirectiveNoTables("widgets"),
		},
		{
			// Deliberate divergence. The community binary does not recognize the
			// typo, folds it into the body and executes "DROP TABLE a;" — the
			// table is created and then dropped, and the migration is recorded
			// as successful.
			name:      "lowercase down near miss is refused",
			format:    "goose",
			file:      "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n-- +goose down\nDROP TABLE a;\n",
			assertErr: gooseDirectiveRefused(),
			assertDB:  gooseDirectiveNoTables("a"),
		},
		{
			// Never-looser: ptah-compat used to exit 0 here and create widgets.
			name:      "Down before Up is refused",
			format:    "goose",
			file:      "-- +goose Down\nDROP TABLE widgets;\n-- +goose Up\n" + widgets,
			assertErr: gooseDirectiveRefused(),
			assertDB:  gooseDirectiveNoTables("widgets"),
		},
		{
			// Never-looser: ptah-compat used to exit 0 here and create a and b.
			name:      "a second Up is refused",
			format:    "goose",
			file:      "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n-- +goose Up\nCREATE TABLE b (id INTEGER PRIMARY KEY);\n",
			assertErr: gooseDirectiveRefused(),
			assertDB:  gooseDirectiveNoTables("a", "b"),
		},
		{
			// Never-looser: ptah-compat used to exit 0 here and create a.
			name:      "StatementEnd with no StatementBegin is refused",
			format:    "goose",
			file:      "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n-- +goose StatementEnd\n",
			assertErr: gooseDirectiveRefused(),
			assertDB:  gooseDirectiveNoTables("a"),
		},
		{
			// Silent divergence before the change: Ptah exited 0 but created
			// only widgets, dropping the author's first statement.
			name:      "SQL above the Up directive still executes",
			format:    "goose",
			file:      "CREATE TABLE pre (id INTEGER PRIMARY KEY);\n-- +goose Up\n" + widgets,
			assertErr: gooseDirectiveAccepted(),
			assertDB: func(c *qt.C, dbPath string) {
				c.Assert(sqliteTableCount(c.TB, dbPath, "pre"), qt.Equals, 1)
				c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
			},
		},
		{
			// Deliberate divergence, opposite verdict to the goose row above.
			// The community binary exits 0, records revision 1 with 0/0
			// statements, creates nothing, and on import writes a zero-byte file
			// over the authored bytes.
			name:      "dbmate file with no migrate:up is refused",
			format:    "dbmate",
			file:      widgets,
			assertErr: gooseDirectiveRefused(),
			assertDB:  gooseDirectiveNoTables("widgets"),
		},
		{
			// Control: the well-formed shape must not move, and the down section
			// must still not run.
			name:      "well-formed Up and Down runs only the up section",
			format:    "goose",
			file:      "-- +goose Up\n" + widgets + "-- +goose Down\nCREATE TABLE down_ran (id INTEGER PRIMARY KEY);\n",
			assertErr: gooseDirectiveAccepted(),
			assertDB: func(c *qt.C, dbPath string) {
				c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
				c.Assert(sqliteTableCount(c.TB, dbPath, "down_ran"), qt.Equals, 0)
			},
		},
	}
}

func TestMigrateApplyGooseDirectiveParsing(t *testing.T) {
	for _, tt := range gooseDirectiveRows() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			writeAtlasApplyProjectMigration(c.TB, migrationsDir, "1_init.sql", tt.file)
			hashConvertedApplyDir(c.TB, migrationsDir, tt.format)
			dbPath := filepath.Join(dir, "apply.db")

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"migrate", "apply",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir + "?format=" + tt.format,
			})

			err := cmd.Execute()

			tt.assertErr(c, err, out.String())
			tt.assertDB(c, dbPath)
		})
	}
}

func TestMigrateValidateGooseDirectiveParsing(t *testing.T) {
	for _, tt := range gooseDirectiveRows() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			writeAtlasApplyProjectMigration(c.TB, migrationsDir, "1_init.sql", tt.file)
			hashConvertedApplyDir(c.TB, migrationsDir, tt.format)

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"migrate", "validate",
				"--dir", "file://" + migrationsDir + "?format=" + tt.format,
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
			})

			err := cmd.Execute()

			tt.assertErr(c, err, out.String())
		})
	}
}

func TestMigrateImportGooseDirectiveParsing(t *testing.T) {
	for _, tt := range gooseDirectiveRows() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			writeAtlasApplyProjectMigration(c.TB, migrationsDir, "1_init.sql", tt.file)
			hashConvertedApplyDir(c.TB, migrationsDir, tt.format)

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"migrate", "import",
				"--from", "file://" + migrationsDir + "?format=" + tt.format,
				"--to", "file://" + filepath.Join(dir, "out"),
			})

			err := cmd.Execute()

			tt.assertErr(c, err, out.String())
		})
	}
}

// TestMigrateApplyGooseDirectivesViaEnvFormat pins the fourth spelling. The
// format can arrive from atlas.hcl instead of the ?format= query, and it reaches
// the same parser, so #981 reproduced here too.
func TestMigrateApplyGooseDirectivesViaEnvFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "1_init.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c.TB, migrationsDir, "goose")
	dbPath := filepath.Join(dir, "apply.db")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  migration {
    dir    = "file://migrations"
    format = goose
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--env", "local",
		"--url", "sqlite://" + dbPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}

// TestMigrateApplyGooseNoDirectivesReallyExecutesTheBody is the control that
// makes the #981 row mean something. A directive-free file whose body is not
// SQL must FAIL at execution: if it succeeded, "exit 0" on the valid fixture
// would prove only that the file was skipped, not that it ran.
func TestMigrateApplyGooseNoDirectivesReallyExecutesTheBody(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "1_init.sql", "THIS IS NOT SQL;\n")
	hashConvertedApplyDir(c.TB, migrationsDir, "goose")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=goose",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(out.String(), qt.Contains, "THIS")
}

// TestMigrateApplyGooseNoTransactionRunsOutsideATransaction proves that the
// source-layout conversion carries Goose's whole-file execution metadata, not
// only its SQL body. The second statement fails; the first table remains only
// when apply honored NO TRANSACTION.
func TestMigrateApplyGooseNoTransactionRunsOutsideATransaction(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(
		c.TB,
		migrationsDir,
		"1_init.sql",
		"-- +goose NO TRANSACTION\n"+
			"-- +goose Up\n"+
			"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"+
			"INSERT INTO missing_table (id) VALUES (1);\n",
	)
	hashConvertedApplyDir(c.TB, migrationsDir, "goose")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=goose",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(out.String(), qt.Contains, "missing_table")
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}
