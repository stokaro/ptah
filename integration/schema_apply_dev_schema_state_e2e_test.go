//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// devSchemaStateTable is a table a schema document gains, so each apply holds a
// plan to rehearse.
const devSchemaStateTable = `table "added" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}

`

// devSchemaStateSurface is one binary's spelling of the same three steps, on the
// document that binary's own `schema inspect` wrote.
type devSchemaStateSurface struct {
	name    string
	target  clirun.Target
	inspect func(url string) []string
	apply   func(url, document, dev string) []string
}

var devSchemaStateSurfaces = []devSchemaStateSurface{
	{
		name:    "ptah-compat",
		target:  clirun.Compat,
		inspect: func(url string) []string { return []string{"schema", "inspect", "-u", url} },
		apply: func(url, document, dev string) []string {
			return []string{"schema", "apply", "-u", url, "--to", "file://" + filepath.ToSlash(document), "--dev-url", dev, "--auto-approve"}
		},
	},
	{
		name:    "ptah",
		target:  clirun.Ptah,
		inspect: func(url string) []string { return []string{"schema", "inspect", "--db-url", url} },
		apply: func(url, document, dev string) []string {
			return []string{"schema", "apply", "--db-url", url, "--schema-file", document, "--dev-url", dev, "--auto-approve"}
		},
	},
}

// inspectedDocument writes what the surface's `schema inspect` reads from url,
// with edit applied, and returns its path.
func (s devSchemaStateSurface) inspectedDocument(c *qt.C, url string, edit func(string) string) string {
	c.Helper()
	inspected := clirun.Run(c, s.target, clirun.Options{Dir: c.TempDir()}, s.inspect(url)...)
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(edit(inspected.Stdout)), 0o600), qt.IsNil)
	return path
}

// withAddedTable is a document with [devSchemaStateTable] in front of its table
// docs, so that the plan creates one table in public.
func withAddedTable(c *qt.C) func(string) string {
	return func(document string) string {
		c.Helper()
		c.Assert(strings.Count(document, `table "docs" {`), qt.Equals, 1, qt.Commentf("%s", document))
		return strings.Replace(document, `table "docs" {`, devSchemaStateTable+`table "docs" {`, 1)
	}
}

// newSchemaStateRole creates a role, dropped when the test ends, once the
// databases holding its privileges are gone. It is called before they are
// created, because cleanups run in reverse.
func newSchemaStateRole(c *qt.C) string {
	c.Helper()
	admin := newEmptyReadScopeDatabase(c, "dev_state_roles")
	role := fmt.Sprintf("ptah_dev_state_%d", time.Now().UnixNano())
	execPostgresFamilySQL(c, c.Context(), admin, "CREATE ROLE "+role)
	c.Cleanup(func() { execPostgresFamilySQL(c, context.Background(), admin, "DROP ROLE "+role) })
	return role
}

// devSchemasAndTables reads back the user schemas of a database and the tables
// each holds, which is what a cleanup of a dev database is held to.
func devSchemasAndTables(c *qt.C, database string) []string {
	c.Helper()
	return queryStrings(c, openReadScopeDatabase(c, database), `
		SELECT n.nspname || ':' || COALESCE(c.relname, '')
		FROM pg_namespace n LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relkind = 'r'
		WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema' ORDER BY 1`)
}

// devPublicAccessList reads back public's whole access list, which
// [devPublicSchemaState] reports only for PUBLIC, sorted: a reset that returns
// the list revokes and grants, so it holds the same entries in another order.
func devPublicAccessList(c *qt.C, database string) []string {
	c.Helper()
	return queryStrings(c, openReadScopeDatabase(c, database), `
		SELECT COALESCE(string_agg(entry::text, ',' ORDER BY entry::text), '(default)')
		FROM pg_namespace n LEFT JOIN LATERAL unnest(n.nspacl) AS entry ON true WHERE n.nspname = 'public'`)
}

// TestSchemaApplyRehearsesASchemasOwnStateE2E applies a document a binary's own
// `schema inspect` wrote, plus one new table, through --dev-url on a server the
// run does not own. The rehearsal rebuilds the target on the dev database first,
// and each row makes that rebuild write a schema's own comment or a privilege on
// a schema.
//
// Three facts are read back. The target gained the table, so the rehearsal let
// the apply through. The dev database holds what it held at the claim: public's
// owner, comment and access list, and no table or schema of the run's, so nothing
// outlived the run. And a second apply through the same dev database still
// rehearses.
func TestSchemaApplyRehearsesASchemasOwnStateE2E(t *testing.T) {
	const docs = "CREATE TABLE docs (id integer PRIMARY KEY, name text)"
	tests := []struct {
		name string
		// target and dev are run on each database in turn. %s is a role.
		target, dev []string
	}{
		{
			name:   "a dev database whose public was dropped and created again",
			target: []string{docs},
			dev:    []string{"DROP SCHEMA public CASCADE", "CREATE SCHEMA public"},
		},
		{
			name:   "a comment of the target's own on public",
			target: []string{docs, "COMMENT ON SCHEMA public IS 'our app'"},
		},
		{
			name:   "a privilege on public for a role",
			target: []string{docs, "GRANT CREATE ON SCHEMA public TO %s"},
		},
		{
			name: "a schema of the target's own, with a comment and a privilege",
			target: []string{
				docs, "CREATE SCHEMA app", "CREATE TABLE app.items (id integer PRIMARY KEY)",
				"COMMENT ON SCHEMA app IS 'application'", "GRANT USAGE ON SCHEMA app TO %s",
			},
		},
	}
	for _, surface := range devSchemaStateSurfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				role := newSchemaStateRole(c)
				target := newEmptyReadScopeDatabase(c, "dev_state_target")
				dev := newEmptyReadScopeDatabase(c, "dev_state_dev")
				for _, statement := range test.target {
					execPostgresFamilySQL(c, c.Context(), target, strings.ReplaceAll(statement, "%s", role))
				}
				for _, statement := range test.dev {
					execPostgresFamilySQL(c, c.Context(), dev, statement)
				}
				claimedState, claimedAccess := devPublicSchemaState(c, dev), devPublicAccessList(c, dev)
				document := surface.inspectedDocument(c, target, withAddedTable(c))

				applied := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, document, dev)...)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(devSchemasAndTables(c, target), qt.Contains, "public:added")
				c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimedState)
				c.Assert(devPublicAccessList(c, dev), qt.DeepEquals, claimedAccess)
				c.Assert(devSchemasAndTables(c, dev), qt.DeepEquals, []string{"public:"})

				again := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, document, dev)...)

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimedState)
			})
		}
	}
}

// TestSchemaApplyRehearsesAPlanThatChangesASchemasOwnStateE2E is the plan's half
// of the same rule. The plan itself changes public's comment or a privilege on
// it. The target takes the change, and the dev database, which ran the same
// statement in the rehearsal, gives it back.
func TestSchemaApplyRehearsesAPlanThatChangesASchemasOwnStateE2E(t *testing.T) {
	tests := []struct {
		name string
		edit func(document string) string
		// wantTarget is what [devPublicSchemaState] reads from the target after
		// the apply.
		wantTarget string
	}{
		{
			name: "a new comment",
			edit: func(document string) string {
				return strings.Replace(document, "standard public schema", "our app", 1)
			},
			wantTarget: "owner pg_database_owner; comment our app; PUBLIC USAGE true, CREATE false",
		},
		{
			name: "a new privilege",
			edit: func(document string) string {
				return document + "\npermission {\n  to = \"PUBLIC\"\n  for = schema.public\n  privileges = [\"CREATE\"]\n}\n"
			},
			wantTarget: "owner pg_database_owner; comment standard public schema; PUBLIC USAGE true, CREATE true",
		},
	}
	for _, surface := range devSchemaStateSurfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newEmptyReadScopeDatabase(c, "dev_state_target")
				dev := newEmptyReadScopeDatabase(c, "dev_state_dev")
				execPostgresFamilySQL(c, c.Context(), target, "CREATE TABLE docs (id integer PRIMARY KEY, name text)")
				claimedState, claimedAccess := devPublicSchemaState(c, dev), devPublicAccessList(c, dev)
				document := surface.inspectedDocument(c, target, test.edit)

				applied := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, document, dev)...)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(devPublicSchemaState(c, target), qt.Equals, test.wantTarget)
				c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimedState)
				c.Assert(devPublicAccessList(c, dev), qt.DeepEquals, claimedAccess)
			})
		}
	}
}

// TestSchemaApplyRehearsesASchemaCommentOnADisposableServerE2E covers the realm a
// declared server gets. Declaring it lifts a privilege but not a comment, because
// the server outlives the run, so what lets this comment through is the reset that
// returns it.
func TestSchemaApplyRehearsesASchemaCommentOnADisposableServerE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	target := newEmptyReadScopeDatabase(c, "dev_state_target")
	dev := newEmptyReadScopeDatabase(c, "dev_state_dev")
	execPostgresFamilySQL(c, c.Context(), target, "CREATE TABLE docs (id integer PRIMARY KEY, name text)")
	claimedState, claimedAccess := devPublicSchemaState(c, dev), devPublicAccessList(c, dev)
	native := devSchemaStateSurfaces[1]
	document := native.inspectedDocument(c, target, func(document string) string {
		return strings.Replace(document, "standard public schema", "our app", 1)
	})

	applied := clirun.Run(c, native.target, clirun.Options{Dir: c.TempDir()}, native.apply(target, document, dev)...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(devPublicSchemaState(c, target), qt.Contains, "comment our app;")
	c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimedState)
	c.Assert(devPublicAccessList(c, dev), qt.DeepEquals, claimedAccess)
}

// TestSchemaApplyRehearsesOnADevDatabaseTheCommunityBinaryUsedE2E is how this
// surfaced. The pinned community binary's own dev reset leaves public created
// again, with no access list and no comment, so a dev database shared with it has
// a public the rehearsal has to write to. Measured on v1.3.0: the access list reads
// NULL after one `schema apply --dev-url`.
func TestSchemaApplyRehearsesOnADevDatabaseTheCommunityBinaryUsedE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	c := qt.New(t)
	target := newEmptyReadScopeDatabase(c, "dev_state_target")
	dev := newEmptyReadScopeDatabase(c, "dev_state_dev")
	execPostgresFamilySQL(c, c.Context(), target, "CREATE TABLE docs (id integer PRIMARY KEY, name text)")
	compat := devSchemaStateSurfaces[0]
	document := compat.inspectedDocument(c, target, withAddedTable(c))
	community, err := exec.CommandContext(c.Context(), reference, "schema", "apply", "-u", target, // #nosec G204 G702 -- operator-provided pinned reference binary
		"--to", "file://"+filepath.ToSlash(document), "--dev-url", dev, "--dry-run").CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", community))
	c.Assert(devPublicAccessList(c, dev), qt.DeepEquals, []string{"(default)"})

	applied := clirun.Run(c, compat.target, clirun.Options{Dir: c.TempDir()}, compat.apply(target, document, dev)...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(devSchemasAndTables(c, target), qt.Contains, "public:added")
	c.Assert(devPublicAccessList(c, dev), qt.DeepEquals, []string{"(default)"})
}

// TestSchemaApplyRefusesASchemasOwnStateWhereTheResetKeepsItE2E is the control on
// the two tests above. A dev URL that pins a schema gets a cleanup that leaves the
// schema as it finds it, so a comment written there would reach the next run. The
// rehearsal refuses before any statement runs: the target does not take the plan,
// and the dev schema holds no comment.
func TestSchemaApplyRefusesASchemasOwnStateWhereTheResetKeepsItE2E(t *testing.T) {
	c := qt.New(t)
	target := newEmptyReadScopeDatabase(c, "dev_state_target")
	dev := newEmptyReadScopeDatabase(c, "dev_state_dev")
	for _, statement := range []string{
		"CREATE SCHEMA app", "CREATE TABLE app.docs (id integer PRIMARY KEY)", "COMMENT ON SCHEMA app IS 'application'",
	} {
		execPostgresFamilySQL(c, c.Context(), target, statement)
	}
	execPostgresFamilySQL(c, c.Context(), dev, "CREATE SCHEMA app")
	pinnedTarget, pinnedDev := planE2ESearchPath(c, target, "app"), planE2ESearchPath(c, dev, "app")
	compat := devSchemaStateSurfaces[0]
	document := compat.inspectedDocument(c, pinnedTarget, func(document string) string {
		c.Assert(strings.Count(document, `table "docs" {`), qt.Equals, 1, qt.Commentf("%s", document))
		return strings.Replace(document, `table "docs" {`,
			strings.ReplaceAll(devSchemaStateTable, "schema.public", "schema.app")+`table "docs" {`, 1)
	})

	got := clirun.Run(c, compat.target, clirun.Options{Dir: c.TempDir()}, compat.apply(pinnedTarget, document, pinnedDev)...)

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `COMMENT ON SCHEMA "app" IS 'application'`)
	c.Assert(got.Stderr, qt.Contains, "cannot be rehearsed")
	c.Assert(devSchemasAndTables(c, target), qt.Not(qt.Contains), "app:added")
	var comment string
	c.Assert(openReadScopeDatabase(c, dev).QueryRowContext(c.Context(),
		`SELECT COALESCE(obj_description('app'::regnamespace, 'pg_namespace'), '')`).Scan(&comment), qt.IsNil)
	c.Assert(comment, qt.Equals, "")
}
