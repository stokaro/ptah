//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A Go tag and a YAML document are text an author typed, so Ptah removes the
// space around a schema or a name where it reads them, and keeps a name's
// bytes everywhere below. Each source below is applied once without the space,
// which creates the live objects, and then with a space after every schema and
// name. The second run has to name the same objects: read with the space, it
// planned a CREATE in "app " and a DROP of the live app object, and the apply
// destroyed the row.

// spacedIdentityGoSource declares app.orders and the sequence app.counter in
// Go. Each %s is where the space goes.
const spacedIdentityGoSource = `package models

//ptah:schema:table name="orders%s" schema="app%s"
type Order struct {
	//ptah:schema:field name="id%s" type="INTEGER" primary="true"
	ID int

	//ptah:schema:field name="note%s" type="TEXT"
	Note string
}

//ptah:schema:sequence name="counter%s" schema="app%s"
type Counter struct{}
`

// spacedIdentityYAMLSource declares app.orders as a YAML schema document.
const spacedIdentityYAMLSource = `tables:
  orders:
    name: "orders%s"
    schema: "app%s"
    columns:
      id:
        name: "id%s"
        type: INTEGER
        primary: true
      note:
        name: "note%s"
        type: TEXT
`

// spacedIdentityObjects lists every schema whose name holds "app", and every
// relation in one, with its kind and OID, in name order. The brackets show a
// space; the OID changes when a relation is dropped and created again.
const spacedIdentityObjects = `
	SELECT COALESCE(string_agg(entry, ', ' ORDER BY entry), '') FROM (
		SELECT '[' || n.nspname || ']' AS entry FROM pg_namespace n WHERE n.nspname LIKE '%app%'
		UNION ALL
		SELECT '[' || n.nspname || '].' || c.relname || '(' || c.relkind::text || ')@' || c.oid::text
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname LIKE '%app%'
	) entries`

func TestSpacedIdentityInASourceKeepsTheLiveObjectsE2E(t *testing.T) {
	tests := []struct {
		name   string
		flag   string
		file   string
		source string
		// relations are the relations the source creates, without their OIDs.
		relations []string
	}{
		{
			name: "Go annotations", flag: "--root-dir", file: "models.go", source: spacedIdentityGoSource,
			relations: []string{"[app].counter(S)", "[app].orders(r)", "[app].orders_pkey(i)"},
		},
		{
			name: "YAML schema document", flag: "--schema-file", file: "schema.yaml", source: spacedIdentityYAMLSource,
			relations: []string{"[app].orders(r)", "[app].orders_pkey(i)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := newEmptyReadScopeDatabase(c, "spaced_ident")
			plain := spacedIdentitySource(c, test.file, strings.ReplaceAll(test.source, "%s", ""))
			spaced := spacedIdentitySource(c, test.file, strings.ReplaceAll(test.source, "%s", " "))
			apply := func(source string, extra ...string) clirun.Result {
				return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					append([]string{"schema", "apply", "--db-url", database, test.flag, source}, extra...)...)
			}
			created := apply(plain, "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			execPostgresFamilySQL(c, c.Context(), database, "INSERT INTO app.orders (id, note) VALUES (1, 'kept')")
			before := spacedIdentityState(c, database)
			c.Assert(spacedIdentityWithoutOIDs(before), qt.Equals, "[app], "+strings.Join(test.relations, ", "))

			planned := apply(spaced, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "Schema is synced, no changes to be made.")

			applied := apply(spaced, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(spacedIdentityState(c, database), qt.Equals, before)
			var rows int
			c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(),
				"SELECT count(*) FROM app.orders WHERE note = 'kept'").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

// spacedIdentitySource writes body to a file named name in a directory of its
// own and returns the path a run names: the directory for Go, which is scanned,
// and the file otherwise.
func spacedIdentitySource(c *qt.C, name, body string) string {
	c.Helper()
	dir := c.TempDir()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	if filepath.Ext(name) == ".go" {
		return dir
	}
	return path
}

// spacedIdentityState reads spacedIdentityObjects from database.
func spacedIdentityState(c *qt.C, database string) string {
	c.Helper()
	var state string
	c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(), spacedIdentityObjects).Scan(&state), qt.IsNil)
	return state
}

// spacedIdentityWithoutOIDs is state with the OID after each relation removed.
func spacedIdentityWithoutOIDs(state string) string {
	entries := strings.Split(state, ", ")
	for i, entry := range entries {
		name, _, _ := strings.Cut(entry, "@")
		entries[i] = name
	}
	return strings.Join(entries, ", ")
}
