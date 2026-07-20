package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestSQLServerMigratorHonorsURLSchemaForMetadata(t *testing.T) {
	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live migrator tests")
	}
	c := qt.New(t)
	ctx := context.Background()

	adminConn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(adminConn)

	schemaName := fmt.Sprintf("ptah_mig_%d", time.Now().UnixNano())
	cleanupSQLServerMigratorSchema(t, adminConn, schemaName)
	defer cleanupSQLServerMigratorSchema(t, adminConn, schemaName)
	_, err = adminConn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoteSQLServerIdentifier(schemaName)+"')")
	c.Assert(err, qt.IsNil)

	scopedConn, err := dbschema.ConnectToDatabase(ctx, sqlServerURLWithSchema(dbURL, schemaName))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(scopedConn)

	fsys := fstest.MapFS{
		"0000000001_create_items.up.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE " + quoteSQLServerIdentifier(schemaName) + ".[ptah_issue_149_items] ([id] INT IDENTITY(1,1) PRIMARY KEY);\n" +
				"GO\n" +
				"CREATE INDEX [idx_ptah_issue_149_items_id] ON " + quoteSQLServerIdentifier(schemaName) + ".[ptah_issue_149_items] ([id]);\n" +
				"GO\n",
		)},
		"0000000001_create_items.down.sql": &fstest.MapFile{Data: []byte(
			"DROP TABLE " + quoteSQLServerIdentifier(schemaName) + ".[ptah_issue_149_items];\n",
		)},
	}
	mig, err := migrator.NewFSMigrator(scopedConn, fsys)
	c.Assert(err, qt.IsNil)
	mig = mig.WithMigrationsTable("", "schema_migrations_issue_149")

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(sqlServerTableExists(t, scopedConn, schemaName, "ptah_issue_149_items"), qt.IsTrue)
	c.Assert(sqlServerTableExists(t, scopedConn, schemaName, "schema_migrations_issue_149"), qt.IsTrue)
	c.Assert(sqlServerTableExists(t, scopedConn, "dbo", "schema_migrations_issue_149"), qt.IsFalse)
}

func cleanupSQLServerMigratorSchema(t *testing.T, conn *dbschema.DatabaseConnection, schemaName string) {
	t.Helper()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS " + quoteSQLServerIdentifier(schemaName) + ".[ptah_issue_149_items]",
		"DROP TABLE IF EXISTS " + quoteSQLServerIdentifier(schemaName) + ".[schema_migrations_issue_149]",
		"DROP TABLE IF EXISTS [dbo].[schema_migrations_issue_149]",
		"DROP SCHEMA IF EXISTS " + quoteSQLServerIdentifier(schemaName),
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func sqlServerTableExists(t *testing.T, conn *dbschema.DatabaseConnection, schemaName, tableName string) bool {
	t.Helper()

	var count int
	query := sqlutil.Rebind(conn.Info().Dialect, `
SELECT COUNT(*)
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = ? AND t.name = ?`)
	err := conn.QueryRowContext(context.Background(), query, schemaName, tableName).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count > 0
}

func sqlServerURLWithSchema(dbURL, schemaName string) string {
	parsed, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	query := parsed.Query()
	query.Set("schema", schemaName)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func quoteSQLServerIdentifier(identifier string) string {
	return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
}
