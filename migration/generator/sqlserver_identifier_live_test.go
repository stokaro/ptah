package generator_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/generator"
)

func TestGenerateMigration_SQLServerIndexDirectionRoundTrip(t *testing.T) {
	c := qt.New(t)
	targetURL := provisionSQLServerGeneratorDatabase(t, "target")
	shadowURL := provisionSQLServerGeneratorDatabase(t, "shadow")
	target := connectSQLServerGeneratorDatabase(t, targetURL)
	ctx := t.Context()

	_, err := target.ExecContext(ctx, `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [status] int NOT NULL
);
CREATE INDEX [idx_users_status] ON [dbo].[users] ([status] ASC);`)
	c.Assert(err, qt.IsNil)
	c.Assert(readSQLServerGeneratorIndexDirection(c, target), qt.IsFalse)

	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	writeSQLServerGeneratorInitialMigration(c, migrationsDir)
	targetSchema := sqlServerGeneratorTargetSchema()

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		Generated:         targetSchema,
		DBConn:            target,
		MigrationName:     "index_status_desc",
		OutputDir:         migrationsDir,
		ShadowDatabaseURL: shadowURL,
		Schemas:           []string{"dbo"},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	pair := files.Files[0]

	upSQL, err := os.ReadFile(pair.UpFile)
	c.Assert(err, qt.IsNil)
	downSQL, err := os.ReadFile(pair.DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "([status] DESC)")
	c.Assert(string(downSQL), qt.Contains, "([status])")

	_, err = target.ExecContext(ctx, string(upSQL))
	c.Assert(err, qt.IsNil)
	c.Assert(readSQLServerGeneratorIndexDirection(c, target), qt.IsTrue)

	_, err = target.ExecContext(ctx, string(downSQL))
	c.Assert(err, qt.IsNil)
	c.Assert(readSQLServerGeneratorIndexDirection(c, target), qt.IsFalse)

	_, err = target.ExecContext(ctx, string(upSQL))
	c.Assert(err, qt.IsNil)
	c.Assert(readSQLServerGeneratorIndexDirection(c, target), qt.IsTrue)
}

func sqlServerGeneratorTargetSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INT"},
			{StructName: "User", Name: "status", Type: "INT"},
		},
		Indexes: []goschema.Index{
			{
				StructName: "User",
				Name:       "idx_users_status",
				Fields:     []string{"status"},
				Parts: []goschema.IndexPart{
					{Name: "status", Desc: true},
				},
			},
		},
	}
}

func readSQLServerGeneratorIndexDirection(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
) bool {
	c.Helper()
	schema, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	indexes := make([]dbschematypes.DBIndex, 0, len(schema.Indexes))
	for _, index := range schema.Indexes {
		if index.Name == "idx_users_status" {
			indexes = append(indexes, index)
		}
	}
	c.Assert(indexes, qt.HasLen, 1)
	c.Assert(indexes[0].Parts, qt.HasLen, 1)
	return indexes[0].Parts[0].Desc
}

func writeSQLServerGeneratorInitialMigration(c *qt.C, dir string) {
	c.Helper()
	upSQL := `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [status] int NOT NULL
);
CREATE INDEX [idx_users_status] ON [dbo].[users] ([status] ASC);
`
	downSQL := "DROP TABLE [dbo].[users];\n"
	c.Assert(
		os.WriteFile(
			filepath.Join(dir, "0000000001_initial.up.sql"),
			[]byte(upSQL),
			0600,
		),
		qt.IsNil,
	)
	c.Assert(
		os.WriteFile(
			filepath.Join(dir, "0000000001_initial.down.sql"),
			[]byte(downSQL),
			0600,
		),
		qt.IsNil,
	)
}

func provisionSQLServerGeneratorDatabase(t testing.TB, role string) string {
	t.Helper()
	c := qt.New(t)
	adminURL := sqlServerGeneratorAdminURL(t)
	databaseName := "ptah_777_" + role + "_" +
		time.Now().UTC().Format("20060102150405.000000000")
	admin := connectSQLServerGeneratorDatabase(t, adminURL)
	_, err := admin.ExecContext(
		t.Context(),
		"CREATE DATABASE "+quoteSQLServerGeneratorIdentifier(databaseName)+
			" COLLATE SQL_Latin1_General_CP1_CI_AS",
	)
	c.Assert(err, qt.IsNil)

	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cancelCleanup()
		cleanup, cleanupErr := dbschema.ConnectToDatabase(cleanupCtx, adminURL)
		c.Assert(cleanupErr, qt.IsNil)
		defer dbschema.CloseAndWarn(cleanup)
		_, cleanupErr = cleanup.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quoteSQLServerGeneratorIdentifier(databaseName)+
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+
				quoteSQLServerGeneratorIdentifier(databaseName),
		)
		c.Assert(cleanupErr, qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", databaseName)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func sqlServerGeneratorAdminURL(t testing.TB) string {
	t.Helper()
	adminURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if adminURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live generator tests")
	}
	return adminURL
}

func connectSQLServerGeneratorDatabase(
	t testing.TB,
	databaseURL string,
) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	return conn
}

func quoteSQLServerGeneratorIdentifier(value string) string {
	return "[" + strings.ReplaceAll(value, "]", "]]") + "]"
}
