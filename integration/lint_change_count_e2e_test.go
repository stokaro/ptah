//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// countBase is the version 1 most rows below start from.
const countBase = "CREATE TABLE users (id INTEGER, name TEXT);\n"

// countTables returns one CREATE TABLE statement per table t<from> to t<to>.
func countTables(from, to int) string {
	var b strings.Builder
	for i := from; i <= to; i++ {
		fmt.Fprintf(&b, "CREATE TABLE t%d (id INTEGER);\n", i)
	}
	return b.String()
}

// countPair is a table a file creates and drops again.
const countPair = "CREATE TABLE tmp (id INTEGER);\nDROP TABLE tmp;\n"

// changeCountRows are the directories of stokaro/ptah#5995. community and
// communityTotal are the summary line and the `.TotalChanges` the pinned
// community binary v1.3.0 prints for the directory under `migrate lint
// --dev-url sqlite://dev?mode=memory --latest <latest>`, measured on that
// binary; "" is a summary with no count line. full and fullTotal are what the
// default compatibility surface prints, which also counts the views and
// triggers Ptah models.
var changeCountRows = []struct {
	name           string
	files          map[string]string
	latest         string
	community      string
	communityTotal string
	full           string
	fullTotal      string
}{
	{
		name:   "IF EXISTS drop of an absent index",
		files:  map[string]string{"1_base.sql": countBase, "2_head.sql": "DROP INDEX IF EXISTS users_name_idx;\n"},
		latest: "1", community: "", communityTotal: "0", full: "", fullTotal: "0",
	},
	{
		name: "control: IF EXISTS drop of a present index",
		files: map[string]string{
			"1_base.sql": countBase + "CREATE INDEX users_name_idx ON users (name);\n",
			"2_head.sql": "DROP INDEX IF EXISTS users_name_idx;\n",
		},
		latest: "1", community: "-- 1 schema change", communityTotal: "1", full: "-- 1 schema change", fullTotal: "1",
	},
	{
		name:   "IF NOT EXISTS creation of a present table",
		files:  map[string]string{"1_base.sql": countBase, "2_head.sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT);\n"},
		latest: "1", community: "", communityTotal: "0", full: "", fullTotal: "0",
	},
	{
		name: "IF NOT EXISTS creation of a present index",
		files: map[string]string{
			"1_base.sql": countBase + "CREATE INDEX users_name_idx ON users (name);\n",
			"2_head.sql": "CREATE INDEX IF NOT EXISTS users_name_idx ON users (name);\n",
		},
		latest: "1", community: "", communityTotal: "0", full: "", fullTotal: "0",
	},
	{
		name: "two absent index drops and a table",
		files: map[string]string{
			"1_base.sql": countBase,
			"2_head.sql": "DROP INDEX IF EXISTS users_name_idx;\nDROP INDEX IF EXISTS other_idx;\nCREATE TABLE t (id INTEGER);\n",
		},
		latest: "1", community: "-- 1 schema change", communityTotal: "1", full: "-- 1 schema change", fullTotal: "1",
	},
	{
		name: "a trigger",
		files: map[string]string{
			"1_base.sql": countBase,
			"2_head.sql": "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN UPDATE users SET name = name WHERE id = NEW.id; END;\n",
		},
		latest: "1", community: "", communityTotal: "0", full: "-- 1 schema change", fullTotal: "1",
	},
	{
		name:   "a view",
		files:  map[string]string{"1_base.sql": countBase, "2_head.sql": "CREATE VIEW v AS SELECT id FROM users;\n"},
		latest: "1", community: "", communityTotal: "0", full: "-- 1 schema change", fullTotal: "1",
	},
	{
		name:   "a view and a table",
		files:  map[string]string{"1_base.sql": countBase, "2_head.sql": "CREATE VIEW v AS SELECT id FROM users;\nCREATE TABLE t (id INTEGER);\n"},
		latest: "1", community: "-- 1 schema change", communityTotal: "1", full: "-- 2 schema changes", fullTotal: "2",
	},
	{
		name:   "a first file of twelve statements",
		files:  map[string]string{"1_init.sql": countTables(1, 10) + countPair},
		latest: "1", community: "-- 10 schema changes", communityTotal: "10", full: "-- 10 schema changes", fullTotal: "10",
	},
	{
		name:   "a first file of eleven statements",
		files:  map[string]string{"1_init.sql": countTables(1, 9) + countPair},
		latest: "1", community: "-- 9 schema changes", communityTotal: "9", full: "-- 9 schema changes", fullTotal: "9",
	},
	{
		name:   "a first file of ten statements",
		files:  map[string]string{"1_init.sql": countTables(1, 8) + countPair},
		latest: "1", community: "-- 10 schema changes", communityTotal: "10", full: "-- 10 schema changes", fullTotal: "10",
	},
	{
		name:   "a first file of eleven statements with an INSERT",
		files:  map[string]string{"1_init.sql": countTables(1, 8) + countPair + "INSERT INTO t1 (id) VALUES (1);\n"},
		latest: "1", community: "-- 8 schema changes", communityTotal: "8", full: "-- 8 schema changes", fullTotal: "8",
	},
	{
		name:   "a first file of twelve statements and a second file",
		files:  map[string]string{"1_init.sql": countTables(1, 10) + countPair, "2_later.sql": "CREATE TABLE later (id INTEGER);\n"},
		latest: "2", community: "-- 11 schema changes", communityTotal: "11", full: "-- 11 schema changes", fullTotal: "11",
	},
	{
		name:   "a second file of twelve statements",
		files:  map[string]string{"1_base.sql": "CREATE TABLE base (id INTEGER);\n", "2_large.sql": countTables(1, 10) + countPair},
		latest: "1", community: "-- 12 schema changes", communityTotal: "12", full: "-- 12 schema changes", fullTotal: "12",
	},
	{
		name:   "a first file of eleven statements with an IF EXISTS drop of an absent table",
		files:  map[string]string{"1_init.sql": countTables(1, 10) + "DROP TABLE IF EXISTS missing;\n"},
		latest: "1", community: "-- 10 schema changes", communityTotal: "10", full: "-- 10 schema changes", fullTotal: "10",
	},
	{
		name: "a first file of eleven statements with a trigger",
		files: map[string]string{"1_init.sql": countTables(1, 10) +
			"CREATE TRIGGER t1_ai AFTER INSERT ON t1 BEGIN UPDATE t1 SET id = id WHERE id = NEW.id; END;\n"},
		latest: "1", community: "-- 10 schema changes", communityTotal: "10", full: "-- 11 schema changes", fullTotal: "11",
	},
	{
		name:   "a first file with an IF EXISTS drop of an absent table",
		files:  map[string]string{"1_init.sql": "DROP TABLE IF EXISTS missing;\nCREATE TABLE t (id INTEGER);\n"},
		latest: "1", community: "-- 1 schema change", communityTotal: "1", full: "-- 1 schema change", fullTotal: "1",
	},
	{
		name:   "a first file with an IF EXISTS drop of an absent index",
		files:  map[string]string{"1_init.sql": "DROP INDEX IF EXISTS i;\nCREATE TABLE t (id INTEGER);\n"},
		latest: "1", community: "-- 1 schema change", communityTotal: "1", full: "-- 1 schema change", fullTotal: "1",
	},
}

// changeCountProject writes a project whose migrations directory holds files,
// hashed by ptah-compat, and returns the project directory.
func changeCountProject(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(content), 0o600), qt.IsNil)
	}
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
	return dir
}

// changeCountLine matches the summary line that counts schema changes.
var changeCountLine = regexp.MustCompile(`(?m)^\s*(-- \d+ schema changes?)$`)

// summaryCount returns the count line of a `migrate lint` summary, or "" when
// the summary has none.
func summaryCount(stdout string) string {
	match := changeCountLine.FindStringSubmatch(stdout)
	if match == nil {
		return ""
	}
	return match[1]
}

// sqliteDevURL is the dev database of the SQLite rows.
const sqliteDevURL = "sqlite://dev?mode=memory"

// totalTemplate is the `--format` that prints only the total.
var totalTemplate = []string{"--format", "{{ .TotalChanges }}"}

// changeCountArgs is the `migrate lint` invocation of one row, in the text
// format or with the template that prints the total.
func changeCountArgs(devURL, latest string, format ...string) []string {
	args := []string{"migrate", "lint", "--dir", "file://migrations", "--dev-url", devURL, "--latest", latest}
	return append(args, format...)
}

// strictEnvironment is the environment of a strict CE run.
func strictEnvironment() []string {
	return append(environmentWithoutPtahVariables(), "PTAH_ATLAS_STRICT_COMPAT=1")
}

// TestCompatMigrateLintChangeCountE2E holds the summary count of ptah-compat
// to the community binary's in strict CE mode, and to the count of every kind
// of object Ptah models on the default surface.
func TestCompatMigrateLintChangeCountE2E(t *testing.T) {
	for _, test := range changeCountRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := changeCountProject(c, test.files)
			full := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}
			strict := clirun.Options{Dir: dir, Env: strictEnvironment()}

			strictText := clirun.Run(c, clirun.Compat, strict, changeCountArgs(sqliteDevURL, test.latest)...)
			strictTotal := clirun.Run(c, clirun.Compat, strict, changeCountArgs(sqliteDevURL, test.latest, totalTemplate...)...)
			fullText := clirun.Run(c, clirun.Compat, full, changeCountArgs(sqliteDevURL, test.latest)...)
			fullTotal := clirun.Run(c, clirun.Compat, full, changeCountArgs(sqliteDevURL, test.latest, totalTemplate...)...)

			c.Assert(strictText.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", strictText.Stdout, strictText.Stderr))
			c.Assert(summaryCount(strictText.Stdout), qt.Equals, test.community, qt.Commentf("stdout:\n%s", strictText.Stdout))
			c.Assert(strictTotal.Stdout, qt.Equals, test.communityTotal)
			c.Assert(fullText.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", fullText.Stdout, fullText.Stderr))
			c.Assert(summaryCount(fullText.Stdout), qt.Equals, test.full, qt.Commentf("stdout:\n%s", fullText.Stdout))
			c.Assert(fullTotal.Stdout, qt.Equals, test.fullTotal)
		})
	}
}

// TestCommunityMigrateLintChangeCountE2E runs the pinned community binary on
// the same directories, so the expected counts above stay its measurement.
func TestCommunityMigrateLintChangeCountE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, test := range changeCountRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := changeCountProject(c, test.files)

			text, textStderr, textErr := runCLIProcess(c.Context(), dir, reference, changeCountArgs(sqliteDevURL, test.latest)...)
			total, totalStderr, totalErr := runCLIProcess(c.Context(), dir, reference,
				changeCountArgs(sqliteDevURL, test.latest, totalTemplate...)...)

			c.Assert(textErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", text, textStderr))
			c.Assert(summaryCount(text), qt.Equals, test.community, qt.Commentf("stdout:\n%s", text))
			c.Assert(totalErr, qt.IsNil, qt.Commentf("stderr:\n%s", totalStderr))
			c.Assert(total, qt.Equals, test.communityTotal)
		})
	}
}

// postgresCountBase is the version 1 the PostgreSQL rows below start from.
const postgresCountBase = "CREATE TABLE users (id int, name text);\n"

// postgresChangeCountRows are rows of stokaro/ptah#5995 on PostgreSQL, with
// the fields of [changeCountRows] but the default surface's total, which the
// SQLite rows already hold to its count line, measured on the pinned community binary
// v1.3.0 against PostgreSQL 17 under a dev URL whose search_path is public,
// on a fresh database for every run.
var postgresChangeCountRows = []struct {
	name           string
	files          map[string]string
	community      string
	communityTotal string
	full           string
}{
	{
		name:      "IF EXISTS drop of an absent index",
		files:     map[string]string{"1_base.sql": postgresCountBase, "2_head.sql": "DROP INDEX IF EXISTS users_name_idx;\n"},
		community: "", communityTotal: "0", full: "",
	},
	{
		name:      "IF EXISTS drop of an absent index qualified by the reviewed schema",
		files:     map[string]string{"1_base.sql": postgresCountBase, "2_head.sql": "DROP INDEX IF EXISTS public.users_name_idx;\n"},
		community: "", communityTotal: "0", full: "",
	},
	{
		name:      "IF NOT EXISTS addition of a present column",
		files:     map[string]string{"1_base.sql": postgresCountBase, "2_head.sql": "ALTER TABLE users ADD COLUMN IF NOT EXISTS name text;\n"},
		community: "", communityTotal: "0", full: "",
	},
	{
		name:      "a view",
		files:     map[string]string{"1_base.sql": postgresCountBase, "2_head.sql": "CREATE VIEW v AS SELECT id FROM users;\n"},
		community: "", communityTotal: "0", full: "-- 1 schema change",
	},
	{
		name:      "a sequence",
		files:     map[string]string{"1_base.sql": postgresCountBase, "2_head.sql": "CREATE SEQUENCE s;\n"},
		community: "", communityTotal: "0", full: "-- 1 schema change",
	},
	{
		name: "a sequence outside the reviewed schema",
		files: map[string]string{
			"1_base.sql": "CREATE SCHEMA app;\n" + postgresCountBase,
			"2_head.sql": "CREATE SEQUENCE app.s;\n",
		},
		community: "", communityTotal: "0", full: "",
	},
	{
		name:      "control: an enum type",
		files:     map[string]string{"1_base.sql": postgresCountBase, "2_head.sql": "CREATE TYPE mood AS ENUM ('a', 'b');\n"},
		community: "-- 1 schema change", communityTotal: "1", full: "-- 1 schema change",
	},
	{
		name:      "a first file with an IF EXISTS drop of an absent table",
		files:     map[string]string{"1_init.sql": "DROP TABLE IF EXISTS missing;\nCREATE TABLE t (id int);\n"},
		community: "-- 1 schema change", communityTotal: "1", full: "-- 1 schema change",
	},
	{
		name:      "a first file with a qualified IF EXISTS drop of an absent table",
		files:     map[string]string{"1_init.sql": "DROP TABLE IF EXISTS public.missing;\nCREATE TABLE t (id int);\n"},
		community: "-- 1 schema change", communityTotal: "1", full: "-- 1 schema change",
	},
	{
		name:      "a first file of twelve statements",
		files:     map[string]string{"1_init.sql": countTables(1, 10) + countPair},
		community: "-- 10 schema changes", communityTotal: "10", full: "-- 10 schema changes",
	},
	{
		name:      "a first file of eleven statements with a sequence",
		files:     map[string]string{"1_init.sql": countTables(1, 10) + "CREATE SEQUENCE s;\n"},
		community: "-- 10 schema changes", communityTotal: "10", full: "-- 11 schema changes",
	},
}

// postgresDevURL creates a database nothing has written to on the server
// adminURL names, drops it when the test ends, and returns a dev URL that
// reviews its public schema.
func postgresDevURL(c *qt.C, admin *sql.DB, adminURL string) string {
	c.Helper()
	name := fmt.Sprintf("ptah_lint_count_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	return withLintE2ESearchPath(c, replaceDatabaseName(c, adminURL, name), "public")
}

// postgresAdmin opens the server the PostgreSQL rows create their databases
// on, closed when the test ends.
func postgresAdmin(c *qt.C, adminURL string) *sql.DB {
	c.Helper()
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	return admin
}

// TestCompatMigrateLintChangeCountPostgresE2E holds the summary count of
// ptah-compat on PostgreSQL to the community binary's in strict CE mode, and
// to the count of every kind of object Ptah models on the default surface.
// Every run replays on a database of its own.
func TestCompatMigrateLintChangeCountPostgresE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	for _, test := range postgresChangeCountRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			admin := postgresAdmin(c, adminURL)
			dir := changeCountProject(c, test.files)
			full := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}
			strict := clirun.Options{Dir: dir, Env: strictEnvironment()}

			strictText := clirun.Run(c, clirun.Compat, strict, changeCountArgs(postgresDevURL(c, admin, adminURL), "1")...)
			strictTotal := clirun.Run(c, clirun.Compat, strict,
				changeCountArgs(postgresDevURL(c, admin, adminURL), "1", totalTemplate...)...)
			fullText := clirun.Run(c, clirun.Compat, full, changeCountArgs(postgresDevURL(c, admin, adminURL), "1")...)

			c.Assert(strictText.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", strictText.Stdout, strictText.Stderr))
			c.Assert(summaryCount(strictText.Stdout), qt.Equals, test.community, qt.Commentf("stdout:\n%s", strictText.Stdout))
			c.Assert(strictTotal.Stdout, qt.Equals, test.communityTotal)
			c.Assert(fullText.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", fullText.Stdout, fullText.Stderr))
			c.Assert(summaryCount(fullText.Stdout), qt.Equals, test.full, qt.Commentf("stdout:\n%s", fullText.Stdout))
		})
	}
}

// TestCommunityMigrateLintChangeCountPostgresE2E runs the pinned community
// binary on the PostgreSQL rows, so their expected counts stay its
// measurement.
func TestCommunityMigrateLintChangeCountPostgresE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	for _, test := range postgresChangeCountRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			admin := postgresAdmin(c, adminURL)
			dir := changeCountProject(c, test.files)

			text, textStderr, textErr := runCLIProcess(c.Context(), dir, reference,
				changeCountArgs(postgresDevURL(c, admin, adminURL), "1")...)
			total, totalStderr, totalErr := runCLIProcess(c.Context(), dir, reference,
				changeCountArgs(postgresDevURL(c, admin, adminURL), "1", totalTemplate...)...)

			c.Assert(textErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", text, textStderr))
			c.Assert(summaryCount(text), qt.Equals, test.community, qt.Commentf("stdout:\n%s", text))
			c.Assert(totalErr, qt.IsNil, qt.Commentf("stderr:\n%s", totalStderr))
			c.Assert(total, qt.Equals, test.communityTotal)
		})
	}
}

// TestMigrationsLintFirstVersionIfExistsDropE2E runs `ptah migrations lint` on
// a first version whose IF EXISTS drop finds nothing on the empty dev
// database: the drop deletes nothing and retires no name a deployed
// application read, so nothing is reported.
func TestMigrationsLintFirstVersionIfExistsDropE2E(t *testing.T) {
	tests := []struct {
		name  string
		first string
	}{
		{name: "a small first file", first: "DROP TABLE IF EXISTS missing;\nCREATE TABLE t (id INTEGER);\n"},
		{name: "a first file of eleven statements", first: countTables(1, 10) + "DROP TABLE IF EXISTS missing;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := changeCountProject(c, map[string]string{"1_init.sql": test.first})

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
				"migrations", "lint", "--dir", "migrations", "--dev-url", "sqlite://dev?mode=memory", "--latest", "1")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "No lint findings.")
		})
	}
}
