//go:build integration

package ydb_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

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
	"ptah.run/internal/ydbgap"
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
// two tables, five columns and one index. The labels name the dialect and the
// directory the counts came from.
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
			} {
				c.Assert(stdout, qt.Contains, sample)
			}
			c.Assert(strings.HasSuffix(stdout, "# EOF\n"), qt.IsTrue)
		})
	}
}

// schema security reads the access model, and the YDB reader does not read
// users, groups or permissions yet, so the command refuses after it connects
// in the words of that gap rather than report a clean access model it never
// saw.
func TestYDBSchemaSecurity_FailurePath_ReadsNoAccessModel(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, err := runCommand(schema.NewSchemaSecurityCommand(), "--db-url", dbtarget.URL(c, line.engine))

			c.Assert(err, qt.ErrorMatches,
				`the analysis reads the access model: `+regexp.QuoteMeta(ydbgap.AccessControl.Message()))
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

// The reader records a view and does not read it, so a lineage of a directory
// holding one refuses in the words of the views gap rather than report nothing
// for the view.
func TestYDBSchemaLineage_FailurePath_AView(t *testing.T) {
	const directory = "ptah_ydb_lineage_view"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			execute(c, conn, "DROP VIEW IF EXISTS `"+directory+"/v`")
			c.Cleanup(func() { execute(c, conn, "DROP VIEW IF EXISTS `"+directory+"/v`") })
			execute(c, conn, "CREATE VIEW `"+directory+"/v` WITH (security_invoker = TRUE) AS SELECT 1 AS a")

			stdout, err := runCommand(schema.NewSchemaLineageCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", directory)

			c.Assert(err, qt.ErrorMatches, `view "`+directory+`.v": `+regexp.QuoteMeta(ydbgap.Views.Message()))
			c.Assert(stdout, qt.Equals, "")
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
