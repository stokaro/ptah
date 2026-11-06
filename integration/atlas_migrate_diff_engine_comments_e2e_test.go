//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasreference"
	"ptah.run/internal/dbtarget"
)

// engineCommentsStep is one desired state of an engine's row, the comments its
// files are expected to carry, and the columns of the table it changes once the
// forward file is applied.
type engineCommentsStep struct {
	schema      string
	wantUp      []string
	wantDown    []string
	wantColumns string
}

// engineCommentsCase is one engine: a target to apply the files to, and the
// query that reads the changed table's columns back from it.
type engineCommentsCase struct {
	name    string
	dialect string
	target  func(c *qt.C) string
	columns string
	steps   []engineCommentsStep
}

// diffEngineSteps runs `migrate diff` once per step into one golang-migrate
// directory, against dev, and returns the directory. binary empty runs this
// binary's verb in process.
func diffEngineSteps(c *qt.C, binary, dev string, schemas []string) string {
	c.Helper()
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	for i, schema := range schemas {
		name := "s" + string(rune('1'+i))
		path := filepath.Join(work, name+".sql")
		c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
		args := []string{
			"migrate", "diff", name,
			"--dir", "file://" + filepath.ToSlash(dir) + "?format=golang-migrate",
			"--to", "file://" + filepath.ToSlash(path),
			"--dev-url", dev,
			"--format", `{{ sql . "  " }}`,
		}
		output, err := runEngineVerb(binary, args)
		c.Assert(err, qt.IsNil, qt.Commentf("step %s:\n%s", name, output))
	}
	return dir
}

// runEngineVerb runs a verb of the binary at path, or of this repository's
// compatibility tree for an empty path.
func runEngineVerb(path string, args []string) (string, error) {
	if path == "" {
		return runCompatVerb(args...)
	}
	output, err := exec.Command(path, args...).CombinedOutput() // #nosec G204 G702 -- the reference path is operator-provided via PTAH_ATLAS_REFERENCE
	return string(output), err
}

// requireEngineReference is the pinned community binary. A run without it skips,
// which the integration contour refuses.
func requireEngineReference(t *testing.T) string {
	t.Helper()
	reference, ok := atlasreference.Binary()
	if !ok {
		t.Skipf("set %s to the pinned Atlas CE binary to compare the files", atlasreference.EnvVar)
	}
	return reference
}

// fileComments is the comment lines of a migration file, in order.
func fileComments(file string) []string {
	comments := make([]string, 0, 8)
	for line := range strings.Lines(file) {
		if text, ok := strings.CutPrefix(line, "-- "); ok {
			comments = append(comments, strings.TrimSpace(text))
		}
	}
	return comments
}

// applyEngineFile runs each statement of a migration file on target.
func applyEngineFile(c *qt.C, targetURL, dialect, file string) {
	c.Helper()
	conn := connectDevDialect(c, targetURL)
	for _, statement := range sqlutil.SplitSQLStatementsForDialect(file, dialect) {
		if strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)) == "" {
			continue
		}
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// heldColumns reads back the one text value query answers.
func heldColumns(c *qt.C, targetURL, query string) string {
	c.Helper()
	conn := connectDevDialect(c, targetURL)
	var columns string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&columns), qt.IsNil)
	return columns
}

var engineCommentsCases = []engineCommentsCase{
	{
		name:    "sqlite",
		dialect: platform.SQLite,
		target:  sqliteScratchURL,
		columns: "SELECT group_concat(name, ',') FROM (SELECT name FROM pragma_table_info('owners') ORDER BY cid)",
		steps: []engineCommentsStep{
			{
				schema:      "CREATE TABLE owners (id integer PRIMARY KEY, name text NOT NULL);\n",
				wantUp:      []string{`create "owners" table`},
				wantDown:    []string{`reverse: create "owners" table`},
				wantColumns: "id,name",
			},
			{
				schema: "CREATE TABLE owners (id integer PRIMARY KEY, name text NOT NULL, email text);\n",
				wantUp: []string{`add column "email" to table: "owners"`},
				wantDown: []string{
					"reverse: disable the enforcement of foreign-keys constraints",
					`reverse: create "new_owners" table`,
					`reverse: copy rows from old table "owners" to new temporary table "new_owners"`,
					`reverse: drop "owners" table after copying rows`,
					`reverse: rename temporary table "new_owners" to "owners"`,
					"reverse: enable back the enforcement of foreign-keys constraints",
				},
				wantColumns: "id,name,email",
			},
			{
				schema: "CREATE TABLE owners (id integer PRIMARY KEY, name text NOT NULL);\n",
				wantUp: []string{
					"disable the enforcement of foreign-keys constraints",
					`create "new_owners" table`,
					`copy rows from old table "owners" to new temporary table "new_owners"`,
					`drop "owners" table after copying rows`,
					`rename temporary table "new_owners" to "owners"`,
					"enable back the enforcement of foreign-keys constraints",
				},
				wantDown:    []string{`reverse: drop column "email" from table: "owners"`},
				wantColumns: "id,name",
			},
		},
	},
	{
		name:    "sqlserver",
		dialect: platform.SQLServer,
		target: func(c *qt.C) string {
			return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
				"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
		},
		columns: "SELECT STRING_AGG(name, ',') WITHIN GROUP (ORDER BY name) FROM sys.tables",
		steps: []engineCommentsStep{
			{
				schema:      "CREATE TABLE owners (id bigint PRIMARY KEY);\nCREATE TABLE gone (id bigint PRIMARY KEY);\n",
				wantUp:      []string{`create "gone" table`, `create "owners" table`},
				wantDown:    []string{`reverse: create "gone" table`, `reverse: create "owners" table`},
				wantColumns: "gone,owners",
			},
			{
				schema:      "CREATE TABLE owners (id bigint PRIMARY KEY);\nCREATE TABLE fresh (id bigint PRIMARY KEY);\n",
				wantUp:      []string{`create "fresh" table`, `drop "gone" table`},
				wantDown:    []string{`reverse: drop "gone" table`, `reverse: create "fresh" table`},
				wantColumns: "fresh,owners",
			},
		},
	},
	{
		name:    "clickhouse",
		dialect: platform.ClickHouse,
		target:  clickHouseScratchURL,
		columns: "SELECT arrayStringConcat(groupArray(name), ',') FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'owners' ORDER BY position)",
		steps: []engineCommentsStep{
			{
				schema:      "CREATE TABLE gone (id UInt64) ENGINE = MergeTree ORDER BY id;\nCREATE TABLE owners (id UInt64) ENGINE = MergeTree ORDER BY id;\n",
				wantUp:      []string{`create "gone" table`, `create "owners" table`},
				wantDown:    []string{`reverse: create "gone" table`, `reverse: create "owners" table`},
				wantColumns: "id",
			},
			{
				schema:      "CREATE TABLE owners (id UInt64, email Nullable(String)) ENGINE = MergeTree ORDER BY id;\n",
				wantUp:      []string{`modify "owners" table`, `drop "gone" table`},
				wantDown:    []string{`reverse: drop "gone" table`, `reverse: modify "owners" table`},
				wantColumns: "id,email",
			},
		},
	},
}

// TestCompatMigrateDiffEngineCommentsE2E_NameEachChange diffs each engine's
// steps, holds both files of each step to the comments it expects, and runs the
// files on the engine, forward and then back, reading the table's columns after
// each. The comments that only describe a statement are gone on every engine:
// a warning above a drop, and the notes around a SQLite table rebuild.
func TestCompatMigrateDiffEngineCommentsE2E_NameEachChange(t *testing.T) {
	for _, test := range engineCommentsCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schemas := make([]string, 0, len(test.steps))
			for _, step := range test.steps {
				schemas = append(schemas, step.schema)
			}

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", test.target(c), schemas))
			target := test.target(c)

			c.Assert(pairs, qt.HasLen, len(test.steps))
			for i, step := range test.steps {
				c.Assert(fileComments(pairs[i].Up), qt.DeepEquals, step.wantUp, qt.Commentf("%s", pairs[i].Up))
				c.Assert(fileComments(pairs[i].Down), qt.DeepEquals, step.wantDown, qt.Commentf("%s", pairs[i].Down))
				applyEngineFile(c, target, test.dialect, pairs[i].Up)
				c.Assert(heldColumns(c, target, test.columns), qt.Equals, step.wantColumns)
			}
			for i := len(pairs) - 1; i > 0; i-- {
				applyEngineFile(c, target, test.dialect, pairs[i].Down)
				c.Assert(heldColumns(c, target, test.columns), qt.Equals, test.steps[i-1].wantColumns)
			}
		})
	}
}

// TestCompatMigrateDiffEngineCommentsE2E_SpeakTheBinarysWordsOnSQLite runs the
// pinned community binary and this one over the same steps on SQLite and
// compares the comments of each forward file: an added column, and every step
// of a table rebuild. The comparison stops at the forward files because the
// binary's rollback of a rebuild drops a table that no longer exists:
// `DROP TABLE new_t` runs after that table was renamed.
func TestCompatMigrateDiffEngineCommentsE2E_SpeakTheBinarysWordsOnSQLite(t *testing.T) {
	c := qt.New(t)
	reference := requireEngineReference(t)
	schemas := []string{
		"CREATE TABLE t (id integer PRIMARY KEY, a int);\n",
		"CREATE TABLE t (id integer PRIMARY KEY, a int, b int, c text DEFAULT 'x');\n",
		"CREATE TABLE t (id integer PRIMARY KEY, a bigint NOT NULL, b int);\nCREATE INDEX i ON t (a);\n",
	}
	const dev = "sqlite://dev?mode=memory&_fk=1"

	ours := readMigrationPairs(c, diffEngineSteps(c, "", dev, schemas))
	theirs := readMigrationPairs(c, diffEngineSteps(c, reference, dev, schemas))

	c.Assert(ours, qt.HasLen, len(theirs))
	for i := range theirs {
		want, got := fileComments(theirs[i].Up), fileComments(ours[i].Up)
		slices.Sort(want)
		slices.Sort(got)
		c.Assert(got, qt.DeepEquals, want, qt.Commentf("ours:\n%s\ntheirs:\n%s", ours[i].Up, theirs[i].Up))
	}
}
