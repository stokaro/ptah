//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A dev database is reset, so every verb that uses one refuses a dev URL that
// names a database the run reads. The URL pair below is the case only the
// server can answer: the pooler serves ptah_aliased under the name ptah_alias,
// so the two URLs name different databases and reach one. Without the live
// comparison ahead of every reset, each verb here empties ptah_aliased:
// `schema apply` refuses after the reset, and `schema diff` and `migrate diff`
// exit 0 (stokaro/ptah#3769). Each test reads ptah_aliased back over the
// direct URL after the refusal.
//
// Two guards stand between the reset and ptah_aliased, and each fixture state
// reaches a different one first. A database holding a table, or anything else
// the reset drops, is not clean, and the Atlas-compatible verbs refuse it
// before they compare anything (stokaro/ptah#3797, stokaro/ptah#3849). An
// empty database is clean to that check, so the live comparison is what
// refuses it. It holds nothing to read back, so the test reads the oid of its
// public schema instead: the realm reset drops public and creates it again,
// and the new schema has a new oid.

const (
	// aliasedTable is ptah_aliased holding a table, which is not clean.
	aliasedTable = "CREATE TABLE kept (id int NOT NULL, PRIMARY KEY (id));\nINSERT INTO kept (id) VALUES (1);\n"
	// aliasedEmpty is ptah_aliased holding nothing but an empty public
	// schema, which is clean.
	aliasedEmpty = ""
)

// devAliasFixture is the aliased database, emptied and given a state, the two
// URLs that reach it, and the files the verbs plan toward.
type devAliasFixture struct {
	direct, alias string
	conn          *dbschema.DatabaseConnection
	schemaFile    string
	dir           string
	emptyDir      string
}

func newDevAliasFixture(c *qt.C, state string) devAliasFixture {
	c.Helper()
	direct := dbtarget.URL(c, dbtarget.PostgreSQLAliased)
	alias := dbtarget.URL(c, dbtarget.PostgreSQLAlias)
	conn, err := dbschema.ConnectToDatabase(c.Context(), direct)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	// The database is this file's alone, and every test starts it over.
	c.Assert(atlasschema.ApplySQL(c.Context(), conn, migrator.MigrationTxModeNone,
		"DROP SCHEMA IF EXISTS public CASCADE;\nCREATE SCHEMA public;\n"+state), qt.IsNil)
	schemaFile, dir := writeDevIdentitySources(c)
	emptyDir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(emptyDir, 0o755), qt.IsNil)
	return devAliasFixture{direct: direct, alias: alias, conn: conn, schemaFile: schemaFile, dir: dir, emptyDir: emptyDir}
}

// keptRows counts the rows of the table `kept` over the direct URL.
func (f devAliasFixture) keptRows(c *qt.C) int {
	c.Helper()
	var rows int
	c.Assert(f.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM kept").Scan(&rows), qt.IsNil)
	return rows
}

// publicOID is the oid of the public schema over the direct URL. A reset of
// the realm drops public and creates it again, so the oid changes; the same
// oid afterwards means nothing reset the database.
func (f devAliasFixture) publicOID(c *qt.C) uint32 {
	c.Helper()
	var oid uint32
	c.Assert(f.conn.QueryRowContext(c.Context(), "SELECT oid FROM pg_namespace WHERE nspname = 'public'").Scan(&oid), qt.IsNil)
	return oid
}

// args spells a row's arguments with the fixture's URLs and files.
func (f devAliasFixture) args(template []string) []string {
	replacer := strings.NewReplacer(
		"{direct}", f.direct,
		"{alias}", f.alias,
		"{schema}", "file://"+f.schemaFile,
		"{dir}", "file://"+f.dir,
		"{rawschema}", f.schemaFile,
		"{emptydir}", "file://"+f.emptyDir,
	)
	args := make([]string, len(template))
	for i, arg := range template {
		args[i] = replacer.Replace(arg)
	}
	return args
}

// TestCompatVerbsRefuseADevURLThatAliasesADatabaseTheyReadE2E runs each
// Atlas-compatible verb that resets its dev database with the dev URL naming,
// through the pooler, the database the verb reads, which is empty.
func TestCompatVerbsRefuseADevURLThatAliasesADatabaseTheyReadE2E(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "schema apply rehearses a schema file on the dev database",
			args:    []string{"schema", "apply", "--url", "{direct}", "--to", "{schema}", "--dev-url", "{alias}", "--auto-approve"},
			wantErr: `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`,
		},
		{
			name:    "schema apply replays a migration directory on the dev database",
			args:    []string{"schema", "apply", "--url", "{direct}", "--to", "{dir}", "--dev-url", "{alias}", "--auto-approve"},
			wantErr: `load --to schema: --dev-url must not point at the target database: the dev database is reset destructively before the migration directory is replayed on it`,
		},
		{
			name:    "schema diff creates a --from schema file on the dev database",
			args:    []string{"schema", "diff", "--from", "{schema}", "--to", "{direct}", "--dev-url", "{alias}"},
			wantErr: `load --from schema: --to database must differ from --dev-url because the dev database is reset during planning`,
		},
		{
			name:    "schema diff replays a --from migration directory on the dev database",
			args:    []string{"schema", "diff", "--from", "{dir}", "--to", "{direct}", "--dev-url", "{alias}"},
			wantErr: `load --from schema: --to database must differ from --dev-url because the dev database is reset during planning`,
		},
		{
			// The pooler serves the alias in transaction mode, where the dev
			// realm lock this verb takes cannot be verified and the verb
			// refuses before the reset. The operator's documented override
			// lets it go on to the reset, which is the route the refusal
			// below has to close.
			name:    "migrate diff replays the directory on the dev database",
			args:    []string{"migrate", "diff", "aliased", "--dir", "{emptydir}", "--to", "{direct}", "--dev-url", "{alias}"},
			env:     map[string]string{"PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK": "1"},
			wantErr: `--to database must differ from --dev-url because the dev database is reset during planning`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newDevAliasFixture(c, aliasedEmpty)
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			before := fixture.publicOID(c)

			out, err := runCompatVerb(fixture.args(test.args)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", out))
			c.Assert(fixture.publicOID(c), qt.Equals, before)
		})
	}
}

// TestCompatVerbsRefuseAnAliasedDevDatabaseThatHoldsATableE2E is the same
// runs with ptah_aliased holding a table. The verbs take the dev database for
// a snapshot first, as the pinned binary does, and refuse it in that binary's
// words before the live comparison is reached; the dev lock the pooler cannot
// verify is not reached either.
func TestCompatVerbsRefuseAnAliasedDevDatabaseThatHoldsATableE2E(t *testing.T) {
	const notClean = `sql/migrate: taking database snapshot: sql/migrate: connected database is not clean: found table "kept" in schema "public"`
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema apply of a schema file", args: []string{"schema", "apply", "--url", "{direct}", "--to", "{schema}", "--dev-url", "{alias}", "--auto-approve"}},
		{name: "schema apply of a migration directory", args: []string{"schema", "apply", "--url", "{direct}", "--to", "{dir}", "--dev-url", "{alias}", "--auto-approve"}},
		{name: "schema diff of a schema file", args: []string{"schema", "diff", "--from", "{schema}", "--to", "{direct}", "--dev-url", "{alias}"}},
		{name: "schema diff of a migration directory", args: []string{"schema", "diff", "--from", "{dir}", "--to", "{direct}", "--dev-url", "{alias}"}},
		{name: "migrate diff", args: []string{"migrate", "diff", "aliased", "--dir", "{emptydir}", "--to", "{direct}", "--dev-url", "{alias}"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newDevAliasFixture(c, aliasedTable)

			out, err := runCompatVerb(fixture.args(test.args)...)

			c.Assert(err, qt.ErrorMatches, notClean, qt.Commentf("%s", out))
			c.Assert(fixture.keptRows(c), qt.Equals, 1)
		})
	}
}

// TestNativeSchemaApplyRefusesADevURLThatAliasesTheTargetE2E is the native
// verb, which rehearses on the dev database through the same code and compares
// the two databases live before it asks whether the dev database is clean.
func TestNativeSchemaApplyRefusesADevURLThatAliasesTheTargetE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newDevAliasFixture(c, aliasedTable)

	out, err := runPtahNativeWithError(fixture.args([]string{
		"schema", "apply", "--db-url", "{direct}", "--schema-file", "{rawschema}", "--dev-url", "{alias}", "--auto-approve",
	})...)

	c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`, qt.Commentf("%s", out))
	c.Assert(fixture.keptRows(c), qt.Equals, 1)
}
