package migratedown_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migratetag"
	"ptah.run/internal/cli/migrateup"
)

// `migrate down --to-tag` and `--skip-checks` are implemented rather than
// waived. Neither has the hosted dependency a waiver would name: tags resolve
// against the local tag namespace, and down bodies carry real pre-migration
// checks.

const (
	parentUpSQL   = "CREATE TABLE parent (id INTEGER PRIMARY KEY);\n"
	parentDownSQL = "DROP TABLE parent;\n"
	childUpSQL    = "CREATE TABLE child (id INTEGER PRIMARY KEY);\n"
	childDownSQL  = "DROP TABLE child;\n"
	// childDownCheckedSQL aborts the rollback while the table has rows, which
	// is what --skip-checks has to get past.
	childDownCheckedSQL = `-- +ptah check name="child_empty" assert="SELECT count(*) = 0 FROM child" on_fail=abort
DROP TABLE child;
`
)

// twoMigrationDir writes a directory with two migrations, the second carrying
// downSQL, and applies both to a fresh database.
func twoMigrationDir(c *qt.C, downSQL string) (dir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_parent.sql":      parentUpSQL,
		"1_parent.down.sql": parentDownSQL,
		"2_child.sql":       childUpSQL,
		"2_child.down.sql":  downSQL,
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(root, "app.db")

	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}

func runTag(args ...string) (string, error) {
	cmd := migratetag.NewMigrateTagCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestMigrateDownToTagStopsWhereTheTagPoints is the flag end to end.
func TestMigrateDownToTagStopsWhereTheTagPoints(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	_, err := runTag("v1.0.0", "--db-url", "sqlite://"+dbPath, "--version", "1")
	c.Assert(err, qt.IsNil)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--to-tag", "v1.0.0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// The catalog, not the report: only the tables say where the rollback
	// actually stopped.
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "child")
	c.Assert(tableCensus(c, dbPath), qt.Contains, "parent")
}

// TestMigrateDownRefusesAnUnknownTag keeps a typo from rolling back further
// than the operator asked.
//
// An unresolved tag must not be read as no tag: the run would then stop where
// something else names, or nowhere the operator chose.
func TestMigrateDownRefusesAnUnknownTag(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	before := tableCensus(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--to-tag", "v9.9.9", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "migration tag not found")
	// The row count is the assertion that matters: the message alone would
	// pass even if the refusal came after the damage.
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
}

// TestMigrateDownRefusesBothTargetAndTag keeps two names for where to stop
// from being resolved by a silent precedence rule.
func TestMigrateDownRefusesBothTargetAndTag(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	_, err := runTag("v1.0.0", "--db-url", "sqlite://"+dbPath, "--version", "1")
	c.Assert(err, qt.IsNil)
	before := tableCensus(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--to-tag", "v1.0.0", "--target", "0", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "both name where to stop")
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
}

// TestMigrateDownRunsDownChecksByDefault is the control for the next test.
//
// Without it, --skip-checks would pass against a rollback nothing was blocking,
// which proves nothing about skipping.
func TestMigrateDownRunsDownChecksByDefault(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownCheckedSQL)
	insertChildRow(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "child_empty")
	c.Assert(tableCensus(c, dbPath), qt.Contains, "child")
}

// TestMigrateDownSkipChecksBypassesABlockingDownCheck is the flag, measured
// against the control above: same directory, same row, one flag apart.
func TestMigrateDownSkipChecksBypassesABlockingDownCheck(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownCheckedSQL)
	insertChildRow(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--skip-checks", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "child")
}

// TestMigrationTagListsWhatWasRecorded covers the verb that fills the
// namespace, including the ordering two entries are needed to show.
func TestMigrationTagListsWhatWasRecorded(t *testing.T) {
	c := qt.New(t)
	_, dbPath := twoMigrationDir(c, childDownSQL)
	_, err := runTag("v2.0.0", "--db-url", "sqlite://"+dbPath, "--version", "2")
	c.Assert(err, qt.IsNil)
	_, err = runTag("v1.0.0", "--db-url", "sqlite://"+dbPath, "--version", "1")
	c.Assert(err, qt.IsNil)

	out, err := runTag("--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(slices.Index(splitLines(out), "TAG     VERSION  RECORDED") >= 0, qt.IsTrue,
		qt.Commentf("%s", out))
	c.Assert(indexOfPrefix(out, "v1.0.0") < indexOfPrefix(out, "v2.0.0"), qt.IsTrue,
		qt.Commentf("%s", out))
}

// TestMigrationTagWithNoNameAndAVersionIsRefused keeps a command that would
// list instead of recording from reading as success.
func TestMigrationTagWithNoNameAndAVersionIsRefused(t *testing.T) {
	c := qt.New(t)
	_, dbPath := twoMigrationDir(c, childDownSQL)

	out, err := runTag("--db-url", "sqlite://"+dbPath, "--version", "1")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "needs a tag name")
}

// TestMigrationTagDefaultsToTheCurrentVersion covers the no---version form.
func TestMigrationTagDefaultsToTheCurrentVersion(t *testing.T) {
	c := qt.New(t)
	_, dbPath := twoMigrationDir(c, childDownSQL)

	out, err := runTag("release", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Tagged version 2 as release.")
}

// insertChildRow makes the child table non-empty so its down check fails.
func insertChildRow(c *qt.C, dbPath string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), "INSERT INTO child (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
}

func splitLines(s string) []string { return strings.Split(strings.TrimRight(s, "\n"), "\n") }

func indexOfPrefix(out, prefix string) int {
	for i, line := range splitLines(out) {
		if strings.HasPrefix(line, prefix) {
			return i
		}
	}
	return -1
}

// --plan derives the rollback from the schema difference instead of running
// the down bodies (stokaro/ptah#1621).

const orphanUpSQL = "CREATE TABLE orphan (id INTEGER PRIMARY KEY, note TEXT);\n"

// noDownBodyDir writes a directory whose second migration has NO down file,
// which is the case --plan exists for, and applies both migrations.
func noDownBodyDir(c *qt.C) (dir, dbPath string) {
	c.Helper()
	return noDownBodyDirWithFormat(c, "ptah")
}

// noDownBodyDirWithFormat is noDownBodyDir under a chosen revision layout.
func noDownBodyDirWithFormat(c *qt.C, revisionFormat string) (dir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_parent.sql":      parentUpSQL,
		"1_parent.down.sql": parentDownSQL,
		"2_orphan.sql":      orphanUpSQL,
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(root, "app.db")

	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir,
		"--revision-format", revisionFormat})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}

func devURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
}

// TestMigrateDownWithoutPlanCannotRevertAMissingDownBody is the control.
//
// Without it, --plan succeeding proves nothing: the ordinary path might have
// handled this directory too.
func TestMigrateDownWithoutPlanCannotRevertAMissingDownBody(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "has no Atlas down migration")
	c.Assert(tableCensus(c, dbPath), qt.Contains, "orphan")
}

// TestMigrateDownPlanRevertsAMissingDownBody is the flag, measured against the
// control above: same directory, one flag apart.
func TestMigrateDownPlanRevertsAMissingDownBody(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--plan", "--shadow-db", devURL(c), "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "orphan")
	c.Assert(tableCensus(c, dbPath), qt.Contains, "parent")
	// The bookkeeping has to move with the schema. A derived rollback that
	// dropped the table but left the revision saying version 2 would leave
	// `migrations status` reporting a migration that is no longer applied, and
	// the next `up` would skip re-creating what was just removed.
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(1))
}

// TestMigrateDownPlanKeepsTheRevisionTable is a regression test for a rollback
// that destroyed its own bookkeeping.
//
// The two sides record migrations under different table names -- the live
// database uses the layout it was written with, the dev replay writes the one
// this run configured -- so the revision table looked exactly like a table the
// rollback should drop. Measured before the fix: the plan derived
// `DROP TABLE IF EXISTS "atlas_schema_revisions"`, applied it, and reported
// success, having destroyed the record of everything ever applied.
func TestMigrateDownPlanKeepsTheRevisionTable(t *testing.T) {
	for _, tc := range []struct {
		name           string
		revisionFormat string
		table          string
	}{
		// Both layouts, and the Atlas one is not decoration: it is the only
		// one that reproduces the bug. Under the native layout the live
		// database and the dev replay both write schema_migrations, the two
		// sides agree, and nothing is derived -- so a test that ran only this
		// case would pass with the exclusion deleted. Measured: it did.
		{name: "ptah layout", revisionFormat: "ptah", table: "schema_migrations"},
		{name: "atlas layout", revisionFormat: "atlas", table: "atlas_schema_revisions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := noDownBodyDirWithFormat(c, tc.revisionFormat)

			out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
				"--target", "1", "--plan", "--shadow-db", devURL(c),
				"--revision-format", tc.revisionFormat, "--confirm")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(tableCensus(c, dbPath), qt.Contains, tc.table)
			c.Assert(out, qt.Not(qt.Contains), "DROP TABLE IF EXISTS \""+tc.table+"\"")
		})
	}
}

// TestMigrateDownPlanDerivesOnlyWhatChanged keeps the plan minimal.
//
// An earlier implementation compared a converted declaration against the live
// catalog and reported every table as changed, so reverting one migration
// planned a rebuild of every table in the database. A rebuild copies and
// recreates, so a spurious one is not cosmetic.
func TestMigrateDownPlanDerivesOnlyWhatChanged(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--plan", "--shadow-db", devURL(c), "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Derived rollback plan: 1 statements")
	c.Assert(out, qt.Not(qt.Contains), "new_parent")
}

// TestMigrateDownPlanRequiresADevDatabase covers the refusal.
//
// The target version's schema exists nowhere else: not in the live database,
// which is what is being changed, and not in any single file, since a version's
// schema is the accumulation of every migration up to it. Without somewhere to
// build it there is nothing to compare against.
func TestMigrateDownPlanRequiresADevDatabase(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)
	before := tableCensus(c, dbPath)

	_, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--plan", "--confirm")

	// The refusal comes before the prompt, where the run has not started, so
	// it carries no "error running down migrations" prefix, as a failed shadow
	// verification carries none.
	c.Assert(err, qt.ErrorMatches, `--plan requires --shadow-db: the schema at version 1 has to be built somewhere `+
		`before it can be compared against the live database`)
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
}

// TestMigrateDownPlanDryRunChangesNothing keeps the preview a preview.
func TestMigrateDownPlanDryRunChangesNothing(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)
	before := tableCensus(c, dbPath)

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--target", "1", "--plan", "--shadow-db", devURL(c), "--dry-run", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// The plan is still shown: a dry run that printed nothing would be
	// indistinguishable from one that found nothing to do.
	c.Assert(out, qt.Contains, `DROP TABLE "orphan"`)
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
}

// recordedVersion reads the highest applied version straight from the revision
// table, so the assertion is about what was recorded and not about what the
// command said it recorded.
func recordedVersion(c *qt.C, dbPath string) int64 {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var version int64
	c.Assert(conn.QueryRowContext(context.Background(),
		"SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE state = 'applied'",
	).Scan(&version), qt.IsNil)
	return version
}

var (
	// revisionTablesOnly is the census of a database rolled back to version 0.
	revisionTablesOnly = []string{"schema_migrations", "schema_migrations_log"}
	// parentAndRevisionTables is the census of a database rolled back to version 1.
	parentAndRevisionTables = []string{"parent", "schema_migrations", "schema_migrations_log"}
)

// noStopRefusal is the refusal of a rollback that names no stop, as a pattern.
const noStopRefusal = `name where the rollback stops: pass --target <version> \(--target 0 rolls back every applied ` +
	`migration\), --to-tag <tag> or --limit <n>; "ptah migrations status" lists the applied versions`

// TestMigrateDownRequiresAStop_FailurePath refuses a rollback that names no
// stop. A default of version 0 would roll back every applied migration on a
// run that asked for none in particular, so there is no default. A preview, a
// derived plan and a JSON document are refused the same way. An empty stop
// variable is refused unless a stop is typed beside it: beside a stop another
// variable sets, read as unset, it would let that stop decide, and
// PTAH_TARGET=0 rolls the whole history back. The runs go through the root
// command, which installs the PTAH_* binding.
func TestMigrateDownRequiresAStop_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     [][2]string
		wantErr string
	}{
		{name: "no stop", wantErr: noStopRefusal},
		{name: "a preview", args: []string{"--dry-run"}, wantErr: noStopRefusal},
		{name: "a JSON document", args: []string{"--json"}, wantErr: noStopRefusal},
		{name: "a derived plan", args: []string{"--plan"}, wantErr: noStopRefusal},
		{name: "an empty --to-tag", args: []string{"--to-tag", ""}, wantErr: noStopRefusal},
		{name: "a --limit of 0, which sets no limit", args: []string{"--limit", "0"}, wantErr: "--limit 0 names no stop: " + noStopRefusal},
		{name: "PTAH_LIMIT of 0", env: [][2]string{{"PTAH_LIMIT", "0"}}, wantErr: "PTAH_LIMIT=0 names no stop: " + noStopRefusal},
		{
			name:    "an empty PTAH_TO_TAG",
			env:     [][2]string{{"PTAH_TO_TAG", ""}},
			wantErr: "PTAH_TO_TAG is set but empty: give it a value, or unset it and leave --to-tag out to stop at --target or --limit",
		},
		{
			name:    "an empty PTAH_TO_TAG beside PTAH_TARGET of 0",
			env:     [][2]string{{"PTAH_TARGET", "0"}, {"PTAH_TO_TAG", ""}},
			wantErr: "PTAH_TO_TAG is set but empty: give it a value, or unset it and leave --to-tag out to stop at --target or --limit",
		},
		{
			name:    "an empty PTAH_LIMIT beside PTAH_TARGET of 0",
			env:     [][2]string{{"PTAH_TARGET", "0"}, {"PTAH_LIMIT", ""}},
			wantErr: "PTAH_LIMIT is set but empty: give it a value, or unset it and leave --limit out to stop at --target or --to-tag",
		},
		{
			name: "a blank PTAH_TARGET beside PTAH_LIMIT",
			env:  [][2]string{{"PTAH_LIMIT", "1"}, {"PTAH_TARGET", "\t"}},
			wantErr: `PTAH_TARGET is set to "\\t", which names nothing: give it a value, or unset it and leave --target out ` +
				`to stop at --to-tag or --limit`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := twoMigrationDir(c, childDownSQL)
			before := tableCensus(c, dbPath)
			for _, variable := range tc.env {
				t.Setenv(variable[0], variable[1])
			}

			_, err := runDownThroughRoot(slices.Concat([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir, "--confirm"},
				tc.args)...)

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
		})
	}
}

// TestMigrateDownRequiresAStop_HappyPath names the stop each way the
// refusal accepts.
func TestMigrateDownRequiresAStop_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  [][2]string
		want []string
	}{
		{name: "a typed --target of 0", args: []string{"--target", "0"}, want: revisionTablesOnly},
		{name: "a typed --limit", args: []string{"--limit", "1"}, want: parentAndRevisionTables},
		{name: "PTAH_TARGET", env: [][2]string{{"PTAH_TARGET", "1"}}, want: parentAndRevisionTables},
		{name: "PTAH_LIMIT", env: [][2]string{{"PTAH_LIMIT", "2"}}, want: revisionTablesOnly},
		{name: "an empty PTAH_TARGET beside a typed --limit", args: []string{"--limit", "1"}, env: [][2]string{{"PTAH_TARGET", ""}}, want: parentAndRevisionTables},
		{name: "an empty PTAH_TO_TAG beside a typed --limit", args: []string{"--limit", "1"}, env: [][2]string{{"PTAH_TO_TAG", ""}}, want: parentAndRevisionTables},
		{name: "an empty PTAH_LIMIT beside a typed --target", args: []string{"--target", "1"}, env: [][2]string{{"PTAH_LIMIT", ""}}, want: parentAndRevisionTables},
		{name: "a blank PTAH_LIMIT beside a typed --target", args: []string{"--target", "1"}, env: [][2]string{{"PTAH_LIMIT", " "}}, want: parentAndRevisionTables},
		{name: "an empty PTAH_TO_TAG beside a typed --target of 0", args: []string{"--target", "0"}, env: [][2]string{{"PTAH_TO_TAG", ""}}, want: revisionTablesOnly},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := twoMigrationDir(c, childDownSQL)
			for _, variable := range tc.env {
				t.Setenv(variable[0], variable[1])
			}

			out, err := runDownThroughRoot(slices.Concat([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir, "--confirm"},
				tc.args)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(tableCensus(c, dbPath), qt.DeepEquals, tc.want)
		})
	}
}
