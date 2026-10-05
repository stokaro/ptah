//go:build integration

package ydb_test

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/core/goschema"
	"ptah.run/dbschema"
	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/agenttarget"
	"ptah.run/internal/cli/introspect"
	"ptah.run/internal/cli/schema"
	"ptah.run/internal/dbtarget"
)

// runCommand runs a command in this process and returns what it wrote to
// standard output, and its error.
func runCommand(cmd *cobra.Command, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), err
}

// execute runs YQL statements against the line's database, one per query.
func execute(c *qt.C, conn *dbschema.DatabaseConnection, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

// TestYDBIntrospect_ModelsPlanNothingAgainstTheirDatabase runs `ptah
// introspect` over the round-trip schema, which carries every type the YDB
// type map writes on the line, a literal default of each kind, a Serial key, a
// composite key, unique, asynchronous and covering indexes, a nested directory
// and names that need escaping. The Go models it writes, parsed back and
// planned against the database they came from, plan nothing; and each field
// takes the Go type ydb-go-sdk scans its column into.
func TestYDBIntrospect_ModelsPlanNothingAgainstTheirDatabase(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, roundTripSchemas)
			c.Cleanup(func() { dropTables(c, conn, roundTripSchemas) })
			apply(c, conn, planAgainst(c, conn, roundTripDeclaration(conn.Info().Capabilities), roundTripSchemas))
			out := c.TempDir()

			stdout, err := runCommand(introspect.NewIntrospectCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", strings.Join(roundTripSchemas, ","), "--out", out)

			c.Assert(err, qt.IsNil, qt.Commentf("introspect:\n%s", stdout))
			c.Assert(stdout, qt.Contains, "Imported 4 table(s)")
			models, err := goschema.ParseDir(out)
			c.Assert(err, qt.IsNil)
			c.Assert(planAgainst(c, conn, models, roundTripSchemas), qt.HasLen, 0)

			accounts, err := os.ReadFile(filepath.Join(out, "ptah_ydb_roundtrip_accounts.go"))
			c.Assert(err, qt.IsNil)
			for _, want := range []string{
				"\tId int64\n", "\tEmail string\n", "\tTiny *int8\n", "\tU64 *uint64\n", "\tRatio *float32\n",
				"\tAvatar []byte\n", "\tDoc []byte\n", "\tToken *string\n", "\tCreatedAt *time.Time\n",
				"\tNIv *time.Duration\n",
			} {
				c.Assert(string(accounts), qt.Contains, want)
			}
		})
	}
}

// A key column YDB reports as nullable has no declaration, so introspect
// refuses it and writes nothing, rather than write a key Ptah would rebuild
// NOT NULL.
func TestYDBIntrospect_FailurePath_NullableKeyColumn(t *testing.T) {
	const directory = "ptah_ydb_introspect_nullable"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, []string{directory})
			c.Cleanup(func() { dropTables(c, conn, []string{directory}) })
			execute(c, conn, "CREATE TABLE `"+directory+"/keys` (k Utf8, v Int64, PRIMARY KEY (k))")
			out := filepath.Join(c.TempDir(), "models")

			stdout, err := runCommand(introspect.NewIntrospectCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", directory, "--out", out)

			c.Assert(err, qt.ErrorMatches, `column "k" of table "`+directory+`.keys" is a nullable key column, `+
				`which no Ptah declaration can represent: Ptah writes NOT NULL on every YDB key column`)
			c.Assert(stdout, qt.Equals, "")
			_, statErr := os.Stat(out)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// TestYDBSchemaStats_CountsWhatTheReaderDescribes counts a directory holding
// tables, indexes, a topic, a changefeed and their consumers. The labels name
// the dialect and the directory the counts came from.
func TestYDBSchemaStats_CountsWhatTheReaderDescribes(t *testing.T) {
	const directory = "ptah_ydb_stats"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, []string{directory})
			c.Cleanup(func() { dropTables(c, conn, []string{directory}) })
			execute(c, conn,
				"CREATE TABLE `"+directory+"/a` (id Int64 NOT NULL, name Utf8, PRIMARY KEY (id), "+
					"INDEX a_name GLOBAL SYNC ON (name))",
				"CREATE TABLE `"+directory+"/b` (x Uint64 NOT NULL, y Utf8 NOT NULL, z Bool, PRIMARY KEY (x, y))",
				"CREATE TOPIC `"+directory+"/events` (CONSUMER `one`, CONSUMER `two`)",
				"ALTER TABLE `"+directory+"/a` ADD CHANGEFEED `changes` WITH (MODE = 'UPDATES', FORMAT = 'JSON')",
				"ALTER TOPIC `"+directory+"/a/changes` ADD CONSUMER `reader`",
				"CREATE COORDINATION NODE `"+directory+"/locks`",
			)

			stdout, err := runCommand(schema.NewSchemaStatsCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", directory)

			c.Assert(err, qt.IsNil)
			labels := `{dialect="ydb",schemas="` + directory + `"}`
			for _, sample := range []string{
				"ptah_schema_tables" + labels + " 2\n",
				"ptah_schema_columns" + labels + " 5\n",
				"ptah_schema_indexes" + labels + " 1\n",
				"ptah_schema_constraints" + labels + " 0\n",
				"ptah_schema_views" + labels + " 0\n",
				"ptah_schema_topics" + labels + " 1\n",
				"ptah_schema_topic_consumers" + labels + " 2\n",
				"ptah_schema_changefeeds" + labels + " 1\n",
				"ptah_schema_changefeed_consumers" + labels + " 1\n",
				"ptah_schema_coordination_nodes" + labels + " 1\n",
				"ptah_schema_streaming_queries" + labels + " 0\n",
			} {
				c.Assert(stdout, qt.Contains, sample)
			}
			c.Assert(stdout, qt.Matches, `(?s).*# EOF\n`)
		})
	}
}

// TestYDBSchemaSecurity_HappyPath_ReadsTheAccessModel analyzes a database
// holding a group nobody is a member of, granted a permission on a table: the
// group is reported as granting to nobody, and the row-level security rule,
// which YDB gives nothing to check, is listed as not checked.
func TestYDBSchemaSecurity_HappyPath_ReadsTheAccessModel(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newAccessNames(c)
			dropTables(c, conn, accessSchemas)
			c.Cleanup(func() {
				dropTables(c, conn, accessSchemas)
				removeAccess(c, conn, names)
			})
			execute(c, conn,
				"CREATE TABLE `"+accessSchema+"/orders` (id Int64 NOT NULL, PRIMARY KEY (id))",
				"CREATE GROUP "+names.group,
				"GRANT 'ydb.generic.read' ON `"+accessSchema+"/orders` TO "+names.group,
			)

			stdout, err := runCommand(schema.NewSchemaSecurityCommand(), "--db-url", dbtarget.URL(c, line.engine),
				"--schemas", accessSchema, "--format", "json")

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Contains, `"code": "ROL04"`)
			c.Assert(stdout, qt.Contains, `"name": "`+names.group+`"`)
			c.Assert(stdout, qt.Matches, `(?s).*"code": "PRV01",\s*"reason": "the target does not model row-level security".*`)
		})
	}
}

// TestYDBSchemaSecurity_FailurePath_RefusesUnreadPrincipals connects as a user
// who may list the database and may not read its rows, a member of
// METADATA-READERS alone. YDB reports its users and groups only through
// .sys/auth_*, which that user may not read, so the command refuses and says
// what the connection needs, rather than report a clean access model it never
// saw. The server answers that read ABORTED, which a retrying client would
// retry until its context ended, so the command is given a minute to answer.
func TestYDBSchemaSecurity_FailurePath_RefusesUnreadPrincipals(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			admin := openYDB(c, line)
			names := newAccessNames(c)
			c.Cleanup(func() { removeAccess(c, admin, names) })
			const password = "Lister1!"
			execute(c, admin,
				"CREATE USER "+names.user+" PASSWORD '"+password+"'",
				"ALTER GROUP `METADATA-READERS` ADD USER "+names.user,
			)
			parsed, err := url.Parse(dbtarget.URL(c, line.engine))
			c.Assert(err, qt.IsNil)
			parsed.User = url.UserPassword(names.user, password)

			ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
			defer cancel()
			command := schema.NewSchemaSecurityCommand()
			command.SetContext(ctx)
			stdout, err := runCommand(command, "--db-url", parsed.String())

			c.Assert(err, qt.ErrorMatches, `the analysis reads YDB's users and groups from .sys/auth_users, `+
				`.sys/auth_groups and .sys/auth_group_members, and this connection may not read them; .*`)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// A lineage traces views and routines. YDB has no routines, so a directory
// holding only tables has nothing to trace, and the command says so.
func TestYDBSchemaLineage_HappyPath_NoViews(t *testing.T) {
	const directory = "ptah_ydb_lineage"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, []string{directory})
			c.Cleanup(func() { dropTables(c, conn, []string{directory}) })
			execute(c, conn, "CREATE TABLE `"+directory+"/t` (id Int64 NOT NULL, PRIMARY KEY (id))")

			stdout, err := runCommand(schema.NewSchemaLineageCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", directory)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, "No view or routine columns to trace.\n")
		})
	}
}

// A lineage of a YDB directory traces its views from the queries the server
// stores: a view's source is its table's Ptah name, a view over a view links
// to the view it reads, a star resolves to the table's columns, and a
// double-quoted YQL string feeds no column.
func TestYDBSchemaLineage_HappyPath_TracesViews(t *testing.T) {
	const directory = "ptah_ydb_lineage_view"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, []string{directory})
			c.Cleanup(func() { dropViewsAndTables(c, conn, []string{directory}) })
			execute(c, conn,
				"CREATE TABLE `"+directory+"/items` (id Int64 NOT NULL, label Utf8, PRIMARY KEY (id))",
				"CREATE VIEW `"+directory+"/named` WITH (security_invoker = TRUE) AS "+
					"SELECT id, label AS name, \"fixed\" AS tag FROM `"+directory+"/items`",
				"CREATE VIEW `"+directory+"/names` WITH (security_invoker = TRUE) AS "+
					"SELECT name FROM `"+directory+"/named`",
				"CREATE VIEW `"+directory+"/everything` WITH (security_invoker = TRUE) AS "+
					"SELECT * FROM `"+directory+"/items`",
			)

			stdout, err := runCommand(schema.NewSchemaLineageCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", directory)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, ""+
				"SOURCE                             FEEDS                                   KIND\n"+
				"ptah_ydb_lineage_view.items.id     ptah_ydb_lineage_view.everything.id     view\n"+
				"ptah_ydb_lineage_view.items.label  ptah_ydb_lineage_view.everything.label  view\n"+
				"ptah_ydb_lineage_view.items.id     ptah_ydb_lineage_view.named.id          view\n"+
				"ptah_ydb_lineage_view.items.label  ptah_ydb_lineage_view.named.name        view\n"+
				"ptah_ydb_lineage_view.named.name   ptah_ydb_lineage_view.names.name        view\n")
		})
	}
}

// The agent surface's read_database takes a YDB target the operator
// configured, and reads the row tables of the directories the caller names.
func TestYDBAgentReadDatabase_ReadsTheNamedDirectory(t *testing.T) {
	const directory = "ptah_ydb_agent"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, []string{directory})
			c.Cleanup(func() { dropTables(c, conn, []string{directory}) })
			execute(c, conn, "CREATE TABLE `"+directory+"/notes` (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id))")
			policy, err := agentpolicy.Assemble()
			c.Assert(err, qt.IsNil)
			target, err := agenttarget.New(agenttarget.Config{
				Name: "events", URL: dbtarget.URL(c, line.engine), Class: agentpolicy.ClassEphemeral,
			})
			c.Assert(err, qt.IsNil)
			targets, err := agenttarget.NewSet(target)
			c.Assert(err, qt.IsNil)
			session, err := agentapi.NewSession(agentapi.SessionConfig{
				Broker: agentpolicy.NewBroker(policy), Targets: targets,
			})
			c.Assert(err, qt.IsNil)

			response, err := session.ReadDatabase(context.Background(),
				agentapi.ReadDatabaseRequest{Schemas: []string{directory}})

			c.Assert(err, qt.IsNil)
			c.Assert(response.Dialect, qt.Equals, "ydb")
			c.Assert(response.Version, qt.Equals, conn.Info().Version)
			c.Assert(response.Objects, qt.DeepEquals, []agentapi.DatabaseObject{
				{Kind: "table", Schema: directory, Name: "notes", Columns: 2},
			})
		})
	}
}
