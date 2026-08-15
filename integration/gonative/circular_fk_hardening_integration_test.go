//go:build integration

package gonative_test

import (
	"bytes"
	"context"
	"database/sql"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"

	"go.5x5.cz/ptah/cmd/generate"
	"go.5x5.cz/ptah/cmd/readdb"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

func TestPostgreSQLSchemaRenderCircularForeignKeysApplyIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoPostgreSQL(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanupPostgreSQLCycleSchema(c.TB, db)
	c.Cleanup(func() { cleanupPostgreSQLCycleSchema(c.TB, db) })
	_, err = db.Exec(`CREATE SCHEMA ptah_cycle_137`)
	c.Assert(err, qt.IsNil)

	fixtureDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "models.go"), []byte(`package models

//ptah:schema:table schema="ptah_cycle_137" name="left_nodes"
type Left struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="right_id" type="BIGINT" foreign="right_nodes(id)" foreign_key_name="fk_cycle_left_right"
	RightID int64
}

//ptah:schema:table schema="ptah_cycle_137" name="right_nodes"
type Right struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="left_id" type="BIGINT" foreign="left_nodes(id)" foreign_key_name="fk_cycle_right_left"
	LeftID int64
}
`), 0o600), qt.IsNil)

	cmd := generate.NewGenerateCommand()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--root-dir", fixtureDir, "--dialect", "postgres"})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, `REFERENCES "ptah_cycle_137"."right_nodes"("id")`)
	c.Assert(stdout.String(), qt.Contains, `REFERENCES "ptah_cycle_137"."left_nodes"("id")`)
	c.Assert(stdout.String(), qt.Not(qt.Contains), "Found 2 tables")
	c.Assert(stderr.String(), qt.Contains, "Found 2 tables")
	c.Assert(stderr.String(), qt.Not(qt.Contains), "Circular dependency detected")
	_, err = db.Exec(stdout.String())
	c.Assert(err, qt.IsNil, qt.Commentf("rendered SQL:\n%s", stdout.String()))

	var foreignKeyCount int
	err = db.QueryRow(`
SELECT COUNT(*)
FROM pg_constraint AS c
JOIN pg_namespace AS n ON n.oid = c.connamespace
WHERE c.contype = 'f'
  AND n.nspname = 'ptah_cycle_137'
  AND c.conname IN ('fk_cycle_left_right', 'fk_cycle_right_left')`).Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 2)
}

func TestPostgreSQLDBReadCircularForeignKeysRoundTripIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoPostgreSQL(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanupPostgreSQLReadCycleSchema(c.TB, db)
	c.Cleanup(func() { cleanupPostgreSQLReadCycleSchema(c.TB, db) })

	_, err = db.Exec(`
CREATE SCHEMA ptah_cycle_read_137;
CREATE TABLE ptah_cycle_read_137.left_nodes (
    id BIGINT PRIMARY KEY,
    right_id BIGINT
);
CREATE TABLE ptah_cycle_read_137.right_nodes (
    id BIGINT PRIMARY KEY,
    left_id BIGINT
);
ALTER TABLE ptah_cycle_read_137.left_nodes
    ADD CONSTRAINT fk_cycle_read_left_right
    FOREIGN KEY (right_id) REFERENCES ptah_cycle_read_137.right_nodes(id);
ALTER TABLE ptah_cycle_read_137.right_nodes
    ADD CONSTRAINT fk_cycle_read_right_left
    FOREIGN KEY (left_id) REFERENCES ptah_cycle_read_137.left_nodes(id);`)
	c.Assert(err, qt.IsNil)

	cmd := readdb.NewReadDBCommand()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetContext(t.Context())
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--db-url", dsn, "--schemas", "ptah_cycle_read_137"})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, `CREATE TABLE "ptah_cycle_read_137"."left_nodes"`)
	c.Assert(stdout.String(), qt.Contains, "fk_cycle_read_left_right")
	c.Assert(stdout.String(), qt.Contains, "fk_cycle_read_right_left")
	c.Assert(stderr.String(), qt.Contains, "Connected to postgres database successfully!")

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	liveSchema, err := dbschema.ReadSchemaWithSchemas(conn, []string{"ptah_cycle_read_137"})
	c.Assert(err, qt.IsNil)
	// Roles and grants are cluster-global. Replaying them inside the source
	// cluster must fail closed, so this same-cluster assertion renders only the
	// schema-local objects from the structured snapshot.
	liveSchema.Roles = nil
	liveSchema.Grants = nil
	database := dbschematogo.ConvertDBSchemaToGoSchema(liveSchema)
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		database,
		conn.Info().Dialect,
		conn.Info().Capabilities,
	)
	c.Assert(err, qt.IsNil)
	replaySQL := strings.Join(statements, "\n\n")

	cleanupPostgreSQLReadCycleSchema(c.TB, db)
	_, err = db.Exec(replaySQL)
	c.Assert(err, qt.IsNil, qt.Commentf("schema-local db read SQL:\n%s", replaySQL))

	var foreignKeyCount int
	err = db.QueryRow(`
SELECT COUNT(*)
FROM pg_constraint AS c
JOIN pg_namespace AS n ON n.oid = c.connamespace
WHERE c.contype = 'f'
  AND n.nspname = 'ptah_cycle_read_137'
  AND c.conname IN ('fk_cycle_read_left_right', 'fk_cycle_read_right_left')`).Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 2)
}

func TestCockroachDBMutualForeignKeysApplyIntegration(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	testPostgreSQLFamilyMutualForeignKeys(t, "CockroachDB", "cockroachdb", dsn)
}

func TestYugabyteDBMutualForeignKeysApplyIntegration(t *testing.T) {
	dsn := requireReachableTestDSN(t, "YUGABYTEDB_TEST_DSN", "pgx", "YugabyteDB")
	testPostgreSQLFamilyMutualForeignKeys(t, "YugabyteDB", "yugabytedb", dsn)
}

func TestMySQLMutualForeignKeysApplyIntegration(t *testing.T) {
	testMySQLFamilyMutualForeignKeys(t, "MySQL", skipIfNoMySQL(t))
}

func TestMySQLForeignKeysOverrideMyISAMSessionDefaultIntegration(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "mysql://"+skipIfNoMySQL(t))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	prefix := "ptah_cycle_137_mysql_engine"
	cleanupMySQLFamilyCycleTablesWithConnection(c.TB, conn, prefix)
	c.Cleanup(func() { cleanupMySQLFamilyCycleTablesWithConnection(c.TB, conn, prefix) })

	err = conn.WithSession(t.Context(), func(session *dbschema.DatabaseConnection) error {
		_, setErr := session.ExecContext(t.Context(), "SET SESSION default_storage_engine = MyISAM")
		c.Assert(setErr, qt.IsNil)
		info := session.Info()
		statements, renderErr := renderer.GetOrderedCreateStatementsWithCapabilities(
			mutualLiveForeignKeyDatabase(prefix),
			info.Dialect,
			info.Capabilities,
		)
		c.Assert(renderErr, qt.IsNil)
		c.Assert(strings.Count(strings.Join(statements, "\n"), "ENGINE=InnoDB"), qt.Equals, 2)
		execConnectionStatements(c.TB, session, statements)

		var innoDBTableCount int
		engineErr := session.QueryRow(`
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name IN (?, ?)
  AND engine = 'InnoDB'`, prefix+"_left", prefix+"_right").Scan(&innoDBTableCount)
		c.Assert(engineErr, qt.IsNil)
		c.Assert(innoDBTableCount, qt.Equals, 2)

		var foreignKeyCount int
		foreignKeyErr := session.QueryRow(`
SELECT COUNT(*)
FROM information_schema.referential_constraints
WHERE constraint_schema = DATABASE()
  AND constraint_name IN (?, ?)`, "fk_"+prefix+"_left_right", "fk_"+prefix+"_right_left").Scan(&foreignKeyCount)
		c.Assert(foreignKeyErr, qt.IsNil)
		c.Assert(foreignKeyCount, qt.Equals, 2)
		return nil
	})

	c.Assert(err, qt.IsNil)
}

func TestMariaDBMutualForeignKeysApplyIntegration(t *testing.T) {
	testMySQLFamilyMutualForeignKeys(t, "MariaDB", skipIfNoMariaDB(t))
}

func TestMySQLDefaultRejectsNonuniqueReferencedKeyIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoMySQL(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "mysql://"+dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	info := conn.Info()

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		indexedLiveForeignKeyDatabase("ptah_indexed_137_mysql_default"),
		info.Dialect,
		info.Capabilities,
	)

	c.Assert(info.Capabilities.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)
}

func TestMySQLSessionOverrideAllowsNonuniqueReferencedKeyIntegration(t *testing.T) {
	c := qt.New(t)
	dsnConfig, err := mysqldriver.ParseDSN(skipIfNoMySQL(t))
	c.Assert(err, qt.IsNil)
	params := make(map[string]string, len(dsnConfig.Params)+1)
	maps.Copy(params, dsnConfig.Params)
	params["restrict_fk_on_non_standard_key"] = "OFF"
	dsnConfig.Params = params
	dsn := dsnConfig.FormatDSN()
	conn, err := dbschema.ConnectToDatabase(t.Context(), "mysql://"+dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	prefix := "ptah_indexed_137_mysql_override"
	cleanupMySQLFamilyIndexedTables(c.TB, conn, prefix)
	c.Cleanup(func() { cleanupMySQLFamilyIndexedTables(c.TB, conn, prefix) })
	rootInfo := conn.Info()
	c.Assert(rootInfo.Capabilities.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(rootInfo.Capabilities.Has(capability.ForeignKeysRequireIndexedReference), qt.IsFalse)

	err = conn.WithSession(t.Context(), func(session *dbschema.DatabaseConnection) error {
		info := session.Info()
		statements, renderErr := renderer.GetOrderedCreateStatementsWithCapabilities(
			indexedLiveForeignKeyDatabase(prefix),
			info.Dialect,
			info.Capabilities,
		)
		c.Assert(renderErr, qt.IsNil)
		c.Assert(info.Capabilities.Has(capability.ForeignKeysRequireUniqueReference), qt.IsFalse)
		c.Assert(info.Capabilities.Has(capability.ForeignKeysRequireIndexedReference), qt.IsTrue)
		execConnectionStatements(c.TB, session, statements)

		var foreignKeyCount int
		queryErr := session.QueryRow(`SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE() AND constraint_name = ?`,
			"fk_"+prefix+"_child_parent").Scan(&foreignKeyCount)
		c.Assert(queryErr, qt.IsNil)
		c.Assert(foreignKeyCount, qt.Equals, 1)
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(conn.Info().Capabilities, qt.DeepEquals, rootInfo.Capabilities)
}

func TestMariaDBAllowsNonuniqueReferencedKeyIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := skipIfNoMariaDB(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "mariadb://"+dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	prefix := "ptah_indexed_137_mariadb"
	cleanupMySQLFamilyIndexedTables(c.TB, conn, prefix)
	c.Cleanup(func() { cleanupMySQLFamilyIndexedTables(c.TB, conn, prefix) })
	info := conn.Info()

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		indexedLiveForeignKeyDatabase(prefix),
		info.Dialect,
		info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Capabilities.Has(capability.ForeignKeysRequireIndexedReference), qt.IsTrue)
	execConnectionStatements(c.TB, conn, statements)

	var foreignKeyCount int
	err = conn.QueryRow(`SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE() AND constraint_name = ?`,
		"fk_"+prefix+"_child_parent").Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 1)
}

func TestSQLServerMutualForeignKeysApplyIntegration(t *testing.T) {
	c := qt.New(t)
	dsn := requireReachableTestDSN(t, "SQLSERVER_TEST_DSN", "sqlserver", "SQL Server")
	db, err := sql.Open("sqlserver", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanupSQLServerCycleTables(c.TB, db)
	c.Cleanup(func() { cleanupSQLServerCycleTables(c.TB, db) })

	statements, err := renderer.GetOrderedCreateStatements(mutualLiveForeignKeyDatabase("ptah_cycle_137_mssql"), "sqlserver")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 4)
	execStatements(c.TB, db, statements)

	var foreignKeyCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM sys.foreign_keys WHERE name IN ('fk_ptah_cycle_137_mssql_left_right', 'fk_ptah_cycle_137_mssql_right_left')`).Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 2)
}

func testPostgreSQLFamilyMutualForeignKeys(t *testing.T, databaseName, dialect, dsn string) {
	t.Helper()
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	prefix := "ptah_cycle_137_" + strings.ToLower(databaseName)
	cleanupPostgreSQLFamilyCycleTables(c.TB, db, prefix)
	c.Cleanup(func() { cleanupPostgreSQLFamilyCycleTables(c.TB, db, prefix) })

	var version string
	err = db.QueryRow(`SELECT version()`).Scan(&version)
	c.Assert(err, qt.IsNil)
	caps := capability.ForServerVersion(dialect, version)
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(mutualLiveForeignKeyDatabase(prefix), dialect, caps)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 4)
	execStatements(c.TB, db, statements)

	var foreignKeyCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' AND constraint_name IN ($1, $2)`,
		"fk_"+prefix+"_left_right", "fk_"+prefix+"_right_left").Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 2)
}

func testMySQLFamilyMutualForeignKeys(t *testing.T, databaseName, dsn string) {
	t.Helper()
	c := qt.New(t)
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	prefix := "ptah_cycle_137_" + strings.ToLower(databaseName)
	cleanupMySQLFamilyCycleTables(c.TB, db, prefix)
	c.Cleanup(func() { cleanupMySQLFamilyCycleTables(c.TB, db, prefix) })

	var version string
	err = db.QueryRow(`SELECT VERSION()`).Scan(&version)
	c.Assert(err, qt.IsNil)
	caps := capability.ForServerVersion("mysql", version)
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(mutualLiveForeignKeyDatabase(prefix), strings.ToLower(databaseName), caps)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 4)
	execStatements(c.TB, db, statements)

	var foreignKeyCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE() AND constraint_name IN (?, ?)`,
		"fk_"+prefix+"_left_right", "fk_"+prefix+"_right_left").Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 2)
}

func mutualLiveForeignKeyDatabase(prefix string) *goschema.Database {
	leftTable := prefix + "_left"
	rightTable := prefix + "_right"
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Left", Name: leftTable},
			{StructName: "Right", Name: rightTable},
		},
		Fields: []goschema.Field{
			{StructName: "Left", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Left", Name: "right_id", Type: "INTEGER", Foreign: rightTable + "(id)", ForeignKeyName: "fk_" + prefix + "_left_right"},
			{StructName: "Right", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Right", Name: "left_id", Type: "INTEGER", Foreign: leftTable + "(id)", ForeignKeyName: "fk_" + prefix + "_right_left"},
		},
	}
}

func indexedLiveForeignKeyDatabase(prefix string) *goschema.Database {
	parentTable := prefix + "_parent"
	childTable := prefix + "_child"
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: parentTable},
			{StructName: "Child", Name: childTable},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Parent", Name: "code", Type: "INTEGER"},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "parent_code", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{{
			StructName: "Parent",
			Name:       "idx_" + prefix + "_parent_code",
			Fields:     []string{"code"},
		}},
		Constraints: []goschema.Constraint{{
			StructName:    "Child",
			Name:          "fk_" + prefix + "_child_parent",
			Type:          "FOREIGN KEY",
			Columns:       []string{"parent_code"},
			ForeignTable:  parentTable,
			ForeignColumn: "code",
		}},
	}
}

func execStatements(tb testing.TB, db *sql.DB, statements []string) {
	c := qt.New(tb)
	c.Helper()
	for _, statement := range statements {
		_, err := db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statement))
	}
}

func execConnectionStatements(tb testing.TB, conn *dbschema.DatabaseConnection, statements []string) {
	c := qt.New(tb)
	c.Helper()
	for _, statement := range statements {
		_, err := conn.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statement))
	}
}

func cleanupPostgreSQLCycleSchema(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	_, err := db.Exec(`DROP SCHEMA IF EXISTS ptah_cycle_137 CASCADE`)
	c.Assert(err, qt.IsNil)
}

func cleanupPostgreSQLReadCycleSchema(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	_, err := db.Exec(`DROP SCHEMA IF EXISTS ptah_cycle_read_137 CASCADE`)
	c.Assert(err, qt.IsNil)
}

func cleanupPostgreSQLFamilyCycleTables(tb testing.TB, db *sql.DB, prefix string) {
	c := qt.New(tb)
	c.Helper()
	_, err := db.Exec(`DROP TABLE IF EXISTS "` + prefix + `_left", "` + prefix + `_right" CASCADE`)
	c.Assert(err, qt.IsNil)
}

func cleanupMySQLFamilyCycleTables(tb testing.TB, db *sql.DB, prefix string) {
	c := qt.New(tb)
	c.Helper()
	ctx := context.Background()
	session, err := db.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer func() {
		c.Check(session.Close(), qt.IsNil)
	}()
	_, err = session.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 0`)
	c.Check(err, qt.IsNil)
	defer func() {
		_, restoreErr := session.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 1`)
		c.Check(restoreErr, qt.IsNil)
	}()
	_, err = session.ExecContext(ctx, "DROP TABLE IF EXISTS `"+prefix+"_left`")
	c.Check(err, qt.IsNil)
	_, err = session.ExecContext(ctx, "DROP TABLE IF EXISTS `"+prefix+"_right`")
	c.Check(err, qt.IsNil)
}

func cleanupMySQLFamilyIndexedTables(tb testing.TB, conn *dbschema.DatabaseConnection, prefix string) {
	c := qt.New(tb)
	c.Helper()
	err := conn.WithSession(context.Background(), func(session *dbschema.DatabaseConnection) error {
		_, disableErr := session.ExecContext(context.Background(), `SET FOREIGN_KEY_CHECKS = 0`)
		c.Check(disableErr, qt.IsNil)
		defer func() {
			_, restoreErr := session.ExecContext(context.Background(), `SET FOREIGN_KEY_CHECKS = 1`)
			c.Check(restoreErr, qt.IsNil)
		}()
		_, dropChildErr := session.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+prefix+"_child`")
		c.Check(dropChildErr, qt.IsNil)
		_, dropParentErr := session.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+prefix+"_parent`")
		c.Check(dropParentErr, qt.IsNil)
		return nil
	})
	c.Check(err, qt.IsNil)
}

func cleanupMySQLFamilyCycleTablesWithConnection(tb testing.TB, conn *dbschema.DatabaseConnection, prefix string) {
	c := qt.New(tb)
	c.Helper()
	err := conn.WithSession(context.Background(), func(session *dbschema.DatabaseConnection) error {
		_, disableErr := session.ExecContext(context.Background(), `SET FOREIGN_KEY_CHECKS = 0`)
		c.Check(disableErr, qt.IsNil)
		defer func() {
			_, restoreErr := session.ExecContext(context.Background(), `SET FOREIGN_KEY_CHECKS = 1`)
			c.Check(restoreErr, qt.IsNil)
		}()
		_, dropLeftErr := session.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+prefix+"_left`")
		c.Check(dropLeftErr, qt.IsNil)
		_, dropRightErr := session.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+prefix+"_right`")
		c.Check(dropRightErr, qt.IsNil)
		return nil
	})
	c.Check(err, qt.IsNil)
}

func cleanupSQLServerCycleTables(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	_, err := db.Exec(`
IF OBJECT_ID(N'ptah_cycle_137_mssql_left', N'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_ptah_cycle_137_mssql_left_right')
  ALTER TABLE [ptah_cycle_137_mssql_left] DROP CONSTRAINT [fk_ptah_cycle_137_mssql_left_right];
IF OBJECT_ID(N'ptah_cycle_137_mssql_right', N'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_ptah_cycle_137_mssql_right_left')
  ALTER TABLE [ptah_cycle_137_mssql_right] DROP CONSTRAINT [fk_ptah_cycle_137_mssql_right_left];
DROP TABLE IF EXISTS [ptah_cycle_137_mssql_left];
DROP TABLE IF EXISTS [ptah_cycle_137_mssql_right];`)
	c.Assert(err, qt.IsNil)
}
