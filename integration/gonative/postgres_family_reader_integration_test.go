//go:build integration

package gonative_test

import (
	"bytes"
	"database/sql"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/readdb"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

func TestPostgresFamilyReader_CockroachDBCatalogObjects(t *testing.T) {
	c := qt.New(t)
	dsn := requireReachableTestDSN(t, "COCKROACHDB_TEST_DSN", "pgx", "CockroachDB")
	sourceURL := requireReaderURL(t, "COCKROACHDB_URL", "CockroachDB")
	fixture := cockroachReaderFixture()

	result := exercisePostgresFamilyReader(c, t, dsn, sourceURL, fixture)

	assertPostgresFamilyReaderCatalog(c, result.schema, fixture)
	c.Assert(postgresFamilyCatalogHasSequence(result.schema, fixture.catalogSchema, fixture.sequence), qt.IsTrue)
}

func TestPostgresFamilyReader_YugabyteDBCatalogObjects(t *testing.T) {
	c := qt.New(t)
	dsn := requireReachableTestDSN(t, "YUGABYTEDB_TEST_DSN", "pgx", "YugabyteDB")
	sourceURL := requireReaderURL(t, "YUGABYTEDB_URL", "YugabyteDB")
	fixture := yugabyteReaderFixture()

	result := exercisePostgresFamilyReader(c, t, dsn, sourceURL, fixture)

	assertPostgresFamilyReaderCatalog(c, result.schema, fixture)
	c.Assert(postgresFamilyCatalogHasSequence(result.schema, fixture.catalogSchema, fixture.sequence), qt.IsTrue)
}

type postgresFamilyReaderFixture struct {
	databaseName     string
	schema           string
	catalogSchema    string
	table            string
	index            string
	view             string
	materializedView string
	sequence         string
	policy           string
	cleanup          []string
	setup            []string
}

type postgresFamilyReaderResult struct {
	schema *dbschematypes.DBSchema
}

func cockroachReaderFixture() postgresFamilyReaderFixture {
	const (
		table            = "ptah_1381_crdb_users"
		index            = "idx_ptah_1381_crdb_users_email"
		view             = "ptah_1381_crdb_active_users"
		materializedView = "ptah_1381_crdb_user_emails"
		sequence         = "ptah_1381_crdb_ticket_seq"
		policy           = "ptah_1381_crdb_active_users_policy"
	)
	return postgresFamilyReaderFixture{
		databaseName:     "CockroachDB",
		schema:           "public",
		catalogSchema:    "",
		table:            table,
		index:            index,
		view:             view,
		materializedView: materializedView,
		sequence:         sequence,
		policy:           policy,
		cleanup: []string{
			`DROP MATERIALIZED VIEW IF EXISTS public.` + materializedView,
			`DROP VIEW IF EXISTS public.` + view,
			`DROP TABLE IF EXISTS public.` + table + ` CASCADE`,
			`DROP SEQUENCE IF EXISTS public.` + sequence,
		},
		setup: []string{
			`CREATE SEQUENCE public.` + sequence,
			`CREATE TABLE public.` + table + ` (` +
				`id INT8 PRIMARY KEY, ` +
				`email STRING NOT NULL, ` +
				`active BOOL NOT NULL DEFAULT true` +
				`)`,
			`CREATE INDEX ` + index + ` ON public.` + table + ` (email)`,
			`ALTER TABLE public.` + table + ` ENABLE ROW LEVEL SECURITY`,
			`CREATE POLICY ` + policy + ` ON public.` + table + ` FOR SELECT USING (active)`,
			`CREATE VIEW public.` + view + ` AS ` +
				`SELECT id, email FROM public.` + table + ` WHERE active`,
			`CREATE MATERIALIZED VIEW public.` + materializedView + ` AS ` +
				`SELECT id, email FROM public.` + table,
		},
	}
}

func yugabyteReaderFixture() postgresFamilyReaderFixture {
	const (
		schema           = "ptah_1381_yb"
		table            = "users"
		index            = "idx_ptah_1381_yb_users_email"
		view             = "active_users"
		materializedView = "user_emails"
		sequence         = "ticket_seq"
		policy           = "active_users_policy"
	)
	return postgresFamilyReaderFixture{
		databaseName:     "YugabyteDB",
		schema:           schema,
		catalogSchema:    schema,
		table:            table,
		index:            index,
		view:             view,
		materializedView: materializedView,
		sequence:         sequence,
		policy:           policy,
		cleanup: []string{
			`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`,
		},
		setup: []string{
			`CREATE SCHEMA ` + schema,
			`CREATE SEQUENCE ` + schema + `.` + sequence,
			`CREATE TABLE ` + schema + `.` + table + ` (` +
				`id BIGINT PRIMARY KEY, ` +
				`email TEXT NOT NULL, ` +
				`active BOOLEAN NOT NULL DEFAULT true` +
				`)`,
			`CREATE INDEX ` + index + ` ON ` + schema + `.` + table + ` (email)`,
			`ALTER TABLE ` + schema + `.` + table + ` ENABLE ROW LEVEL SECURITY`,
			`CREATE POLICY ` + policy + ` ON ` + schema + `.` + table + ` FOR SELECT USING (active)`,
			`CREATE VIEW ` + schema + `.` + view + ` AS ` +
				`SELECT id, email FROM ` + schema + `.` + table + ` WHERE active`,
			`CREATE MATERIALIZED VIEW ` + schema + `.` + materializedView + ` AS ` +
				`SELECT id, email FROM ` + schema + `.` + table,
		},
	}
}

func requireReaderURL(t *testing.T, envName, databaseName string) string {
	t.Helper()

	rawURL := os.Getenv(envName)
	if rawURL == "" {
		t.Skipf("Skipping %s reader tests: %s environment variable not set", databaseName, envName)
	}
	return rawURL
}

func exercisePostgresFamilyReader(
	c *qt.C,
	t *testing.T,
	dsn string,
	sourceURL string,
	fixture postgresFamilyReaderFixture,
) postgresFamilyReaderResult {
	c.Helper()
	t.Setenv("PTAH_ATLAS_INSPECT_ALL_BLOCKS", "1")

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	executePostgresFamilyReaderStatements(c, db, fixture.cleanup, "cleanup "+fixture.databaseName+" reader fixture")
	c.Cleanup(func() {
		executePostgresFamilyReaderStatements(c, db, fixture.cleanup, "cleanup "+fixture.databaseName+" reader fixture")
	})
	executePostgresFamilyReaderStatements(c, db, fixture.setup, "setup "+fixture.databaseName+" reader fixture")

	live := readPostgresFamilyReaderSchema(c, t, sourceURL, fixture.schema)
	nativeSQL := runPostgresFamilyReadDB(c, t, sourceURL, fixture)
	compatHCL := runPostgresFamilyCompatInspect(c, t, sourceURL, fixture)
	assertPostgresFamilyReaderOutput(c, nativeSQL, fixture)
	assertPostgresFamilyReaderOutput(c, compatHCL, fixture)

	return postgresFamilyReaderResult{schema: live}
}

func executePostgresFamilyReaderStatements(c *qt.C, db *sql.DB, statements []string, label string) {
	c.Helper()
	for _, statement := range statements {
		_, err := db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s: %s", label, statement))
	}
}

func readPostgresFamilyReaderSchema(
	c *qt.C,
	t *testing.T,
	sourceURL string,
	schema string,
) *dbschematypes.DBSchema {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), sourceURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schema})
	c.Assert(err, qt.IsNil)
	return live
}

func runPostgresFamilyReadDB(c *qt.C, t *testing.T, sourceURL string, fixture postgresFamilyReaderFixture) string {
	c.Helper()
	cmd := readdb.NewReadDBCommand()
	cmd.SetContext(t.Context())
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--db-url", sourceURL, "--schemas", fixture.schema})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
	c.Assert(stderr.String(), qt.Not(qt.Contains), "is not a table")
	return stdout.String()
}

func runPostgresFamilyCompatInspect(
	c *qt.C,
	t *testing.T,
	sourceURL string,
	fixture postgresFamilyReaderFixture,
) string {
	c.Helper()
	cmd := atlas.NewCompatCommand("ptah-compat")
	cmd.SetContext(t.Context())
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", sourceURL,
		"--schema", fixture.schema,
		"--format", "{{ hcl . }}",
		"--exclude", "*[type=role]",
	})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
	c.Assert(stderr.String(), qt.Not(qt.Contains), "is not a table")
	return stdout.String()
}

func assertPostgresFamilyReaderCatalog(
	c *qt.C,
	schema *dbschematypes.DBSchema,
	fixture postgresFamilyReaderFixture,
) {
	c.Helper()
	c.Assert(postgresFamilyCatalogHasTable(schema, fixture.catalogSchema, fixture.table), qt.IsTrue)
	c.Assert(postgresFamilyCatalogHasTable(schema, fixture.catalogSchema, fixture.view), qt.IsFalse)
	c.Assert(postgresFamilyCatalogHasTable(schema, fixture.catalogSchema, fixture.materializedView), qt.IsFalse)
	c.Assert(postgresFamilyCatalogHasView(schema, fixture.catalogSchema, fixture.view), qt.IsTrue)
	c.Assert(postgresFamilyCatalogHasMaterializedView(schema, fixture.catalogSchema, fixture.materializedView), qt.IsTrue)
	c.Assert(postgresFamilyCatalogHasIndex(schema, fixture.catalogSchema, fixture.table, fixture.index), qt.IsTrue)
	c.Assert(postgresFamilyCatalogHasRLSEnabledTable(schema, fixture.catalogSchema, fixture.table), qt.IsTrue)
	c.Assert(postgresFamilyCatalogHasRLSPolicy(schema, fixture.catalogSchema, fixture.table, fixture.policy), qt.IsTrue)
}

func assertPostgresFamilyReaderOutput(c *qt.C, output string, fixture postgresFamilyReaderFixture) {
	c.Helper()
	c.Assert(output, qt.Contains, fixture.table)
	c.Assert(output, qt.Contains, fixture.index)
	c.Assert(output, qt.Contains, fixture.view)
	c.Assert(output, qt.Contains, fixture.materializedView)
}

func postgresFamilyCatalogHasTable(schema *dbschematypes.DBSchema, schemaName, tableName string) bool {
	for _, table := range schema.Tables {
		if table.Schema == schemaName && table.Name == tableName {
			return true
		}
	}
	return false
}

func postgresFamilyCatalogHasView(schema *dbschematypes.DBSchema, schemaName, viewName string) bool {
	for _, view := range schema.Views {
		if view.Schema == schemaName && view.Name == viewName {
			return true
		}
	}
	return false
}

func postgresFamilyCatalogHasMaterializedView(
	schema *dbschematypes.DBSchema,
	schemaName string,
	viewName string,
) bool {
	for _, view := range schema.MatViews {
		if view.Schema == schemaName && view.Name == viewName {
			return true
		}
	}
	return false
}

func postgresFamilyCatalogHasIndex(
	schema *dbschematypes.DBSchema,
	schemaName string,
	tableName string,
	indexName string,
) bool {
	for _, index := range schema.Indexes {
		if index.Schema == schemaName && index.TableName == tableName && index.Name == indexName {
			return true
		}
	}
	return false
}

func postgresFamilyCatalogHasSequence(schema *dbschematypes.DBSchema, schemaName, sequenceName string) bool {
	for _, sequence := range schema.Sequences {
		if sequence.Schema == schemaName && sequence.Name == sequenceName {
			return true
		}
	}
	return false
}

func postgresFamilyCatalogHasRLSEnabledTable(schema *dbschematypes.DBSchema, schemaName, tableName string) bool {
	for _, table := range schema.Tables {
		if table.Schema == schemaName && table.Name == tableName && table.RLSEnabled {
			return true
		}
	}
	return false
}

func postgresFamilyCatalogHasRLSPolicy(
	schema *dbschematypes.DBSchema,
	schemaName string,
	tableName string,
	policyName string,
) bool {
	wantTable := dbschematypes.QualifyTableName(schemaName, tableName)
	for _, policy := range schema.RLSPolicies {
		if policy.Table == wantTable && policy.Name == policyName && policy.ToRoles == "PUBLIC" {
			return true
		}
	}
	return false
}
