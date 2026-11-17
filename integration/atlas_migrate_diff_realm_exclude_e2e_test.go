//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// `ptah-compat migrate diff` counts an exclude pattern from its dev URL, the
// way `schema apply` counts it from its own: a URL that names no schema
// describes the realm, so `public.notes.search_vector` names a column, and a
// URL with `search_path=public` fills the schema slot, so the same pattern has
// one part too many. One selector list then serves both commands, which is how
// a caller that generates with `migrate diff` and pushes with `schema apply`
// writes it.
//
// The pinned community binary v1.3.0 takes no position here: measured on
// PostgreSQL 18 with this fixture, its `migrate diff` writes the column and the
// index under every `exclude` list, including a four-part pattern, on both
// URLs.

// migrateDiffRealmSchema declares a table whose column and index the rows
// exclude by a schema-qualified pattern.
const migrateDiffRealmSchema = "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL, search_vector text);\n" +
	"CREATE INDEX notes_body_idx ON notes (body);\n"

// migrateDiffRealmSelectors name the column and the index of notes from the
// realm.
const migrateDiffRealmSelectors = "public.notes.search_vector,public.notes.notes_body_idx"

// migrateDiffRealmDevURL creates an empty database on the PostgreSQL server and
// returns its URL with query added. The database is dropped when the test
// ends.
func migrateDiffRealmDevURL(c *qt.C, query url.Values) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	name := fmt.Sprintf("ptah_diff_realm_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	parsed, err := url.Parse(replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	values := parsed.Query()
	maps.Copy(values, query)
	parsed.RawQuery = values.Encode()
	return parsed.String()
}

// migrateDiffRealmRoot writes schema and an atlas.hcl whose env "local"
// excludes the realm selectors into a new directory, and returns it. The
// migration directory is not created.
func migrateDiffRealmRoot(c *qt.C, schema string) string {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte(schema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(`env "local" {
  exclude = ["public.notes.search_vector", "public.notes.notes_body_idx"]
}
`), 0o600), qt.IsNil)
	return root
}

// realmDiffNotes is how a migration written on a realm dev URL names notes.
const realmDiffNotes = `"public"."notes"`

// migrateDiffRealmNotes is the migration that creates notes, spelled table,
// with extra between its body column and its primary key, on one line as the
// default layout writes it. A realm dev URL writes the table qualified, as the
// pinned community binary does; a URL that pins search_path writes it bare.
func migrateDiffRealmNotes(table, extra string) string {
	return "-- Create \"notes\" table\n" +
		"CREATE TABLE " + table + " (\"id\" bigint NOT NULL, \"body\" text NOT NULL, " +
		extra +
		"PRIMARY KEY (\"id\"));\n"
}

// migrateDiffRealmFile returns the body of the one migration named "add" under
// root.
func migrateDiffRealmFile(c *qt.C, root string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(root, "migrations", "*_add.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	contents, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	return string(contents)
}

// TestAtlasCompatMigrateDiffCountsExcludesFromARealmDevURL_HappyPathE2E covers
// the desired side. On a dev URL that names no schema the realm-relative
// selectors leave the column and the index out of the file, whether they come
// from the variable or from the env's list; the unset row is the control and
// writes both. The two-part spelling reaches the same column on both URLs: on
// the realm URL because an object in the connection's schema also matches by
// its bare name, so counting from the realm adds the three-part spelling and
// takes none away.
func TestAtlasCompatMigrateDiffCountsExcludesFromARealmDevURL_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		query url.Values
		env   []string
		args  []string
		want  string
	}{
		{
			name: "realm URL, nothing excluded",
			want: migrateDiffRealmNotes(realmDiffNotes, `"search_vector" text NULL, `) +
				"-- Create index \"notes_body_idx\" to table: \"notes\"\n" +
				"CREATE INDEX \"notes_body_idx\" ON \"public\".\"notes\" (\"body\");\n",
		},
		{
			name: "realm URL, the variable",
			env:  []string{migrateDiffExcludeEnvVar + "=" + migrateDiffRealmSelectors},
			want: migrateDiffRealmNotes(realmDiffNotes, ""),
		},
		{
			name: "realm URL, the env list",
			args: []string{"--env", "local"},
			want: migrateDiffRealmNotes(realmDiffNotes, ""),
		},
		{
			name: "realm URL, the bound spelling",
			env:  []string{migrateDiffExcludeEnvVar + "=notes.search_vector,notes.notes_body_idx"},
			want: migrateDiffRealmNotes(realmDiffNotes, ""),
		},
		{
			name:  "bound URL, the bound spelling",
			query: url.Values{"search_path": {"public"}},
			env:   []string{migrateDiffExcludeEnvVar + "=notes.search_vector,notes.notes_body_idx"},
			want:  migrateDiffRealmNotes(`"notes"`, ""),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			devURL := migrateDiffRealmDevURL(c, test.query)
			root := migrateDiffRealmRoot(c, migrateDiffRealmSchema)
			env := append(environmentWithoutPtahVariables(), test.env...)
			args := append([]string{
				"migrate", "diff", "add", "--dir", "file://migrations", "--to", "file://schema.sql", "--dev-url", devURL,
			}, test.args...)

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env}, args...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(migrateDiffRealmFile(c, root), qt.Equals, test.want)
		})
	}
}

// migrateDiffRealmReplayRoot writes a migration directory whose history adds a
// column the desired schema does not declare, as a column a later step of the
// caller's own maintains, and hashes it.
func migrateDiffRealmReplayRoot(c *qt.C) string {
	c.Helper()
	root := migrateDiffRealmRoot(c, "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);\n")
	c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "migrations", "20200101000000_init.sql"), []byte(
		"CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);\n"+
			"ALTER TABLE notes ADD COLUMN search_vector text;\n",
	), 0o600), qt.IsNil)
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
	return root
}

// TestAtlasCompatMigrateDiffCountsReplayExcludesFromARealmDevURL_HappyPathE2E
// covers the current side, the replay of the directory: the realm-relative
// selector is what keeps the next migration from dropping the column the
// history added. Nothing is planned, so no file is written.
func TestAtlasCompatMigrateDiffCountsReplayExcludesFromARealmDevURL_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	devURL := migrateDiffRealmDevURL(c, nil)
	root := migrateDiffRealmReplayRoot(c)
	env := append(environmentWithoutPtahVariables(), migrateDiffExcludeEnvVar+"=public.notes.search_vector")

	result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env},
		"migrate", "diff", "add", "--dir", "file://migrations", "--to", "file://schema.sql", "--dev-url", devURL)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
	c.Assert(result.Stdout, qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
	files, err := filepath.Glob(filepath.Join(root, "migrations", "*_add.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 0)
}

// TestAtlasCompatMigrateDiffReplayWithoutExclude_HappyPathE2E is the control
// for the test above: the same directory without the selector plans the drop,
// so the test above measures the exclusion rather than a replay that never
// held the column.
func TestAtlasCompatMigrateDiffReplayWithoutExclude_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	devURL := migrateDiffRealmDevURL(c, nil)
	root := migrateDiffRealmReplayRoot(c)

	result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"migrate", "diff", "add", "--dir", "file://migrations", "--to", "file://schema.sql", "--dev-url", devURL)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
	c.Assert(migrateDiffRealmFile(c, root), qt.Equals, "-- Modify \"notes\" table\n"+
		"ALTER TABLE "+realmDiffNotes+" DROP COLUMN \"search_vector\";\n")
}

// TestAtlasCompatMigrateDiffRefusesARealmPatternOnABoundDevURL_FailurePathE2E
// keeps the bound count where the URL names a schema: the schema slot is
// filled, so the realm spelling has one part too many and is refused before a
// file is written, with the spelling that reaches the same column.
func TestAtlasCompatMigrateDiffRefusesARealmPatternOnABoundDevURL_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	devURL := migrateDiffRealmDevURL(c, url.Values{"search_path": {"public"}})
	root := migrateDiffRealmRoot(c, migrateDiffRealmSchema)
	env := append(environmentWithoutPtahVariables(), migrateDiffExcludeEnvVar+"=public.notes.search_vector")

	result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env},
		"migrate", "diff", "add", "--dir", "file://migrations", "--to", "file://schema.sql", "--dev-url", devURL)

	c.Assert(result.ExitCode, qt.Equals, 1)
	c.Assert(result.Stdout, qt.Equals, "")
	c.Assert(result.Stderr, qt.Equals, `Error: too many parts in pattern "public.notes.search_vector": `+
		`this connection is bound to schema "public", so a pattern names object or object.child; `+
		`write "notes.search_vector"`+"\n")
	files, err := filepath.Glob(filepath.Join(root, "migrations", "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 0)
}
