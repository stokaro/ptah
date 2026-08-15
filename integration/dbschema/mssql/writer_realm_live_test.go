//go:build integration

package mssql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/mssql"
)

func TestWriterDropDatabaseRealm_Live(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")
	t.Cleanup(func() {
		c.Check(writer.DropDatabaseRealm(context.Background()), qt.IsNil)
	})
	err := writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)

	statements := []string{
		"CREATE SCHEMA [app]",
		"CREATE SCHEMA [audit]",
		"CREATE TABLE [app].[parent] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE TABLE [audit].[child] (" +
			"[id] bigint NOT NULL PRIMARY KEY, " +
			"[parent_id] bigint NOT NULL, " +
			"CONSTRAINT [fk_child_parent] FOREIGN KEY ([parent_id]) REFERENCES [app].[parent] ([id]))",
		"CREATE TABLE [app].[z_temporal] (" +
			"[id] bigint NOT NULL PRIMARY KEY, " +
			"[valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL, " +
			"[valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL, " +
			"PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) " +
			"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [audit].[a_temporal_history]))",
		"CREATE FUNCTION [app].[normalize_id] (@value bigint) RETURNS bigint AS BEGIN RETURN @value END",
		"CREATE VIEW [audit].[child_view] AS SELECT [id], [parent_id] FROM [audit].[child]",
		"CREATE VIEW [audit].[temporal_view] AS " +
			"SELECT [id] FROM [app].[z_temporal] FOR SYSTEM_TIME ALL",
		"CREATE SEQUENCE [app].[order_seq] AS bigint START WITH 1 INCREMENT BY 1",
		"CREATE SYNONYM [dbo].[parent_alias] FOR [app].[parent]",
		"CREATE TYPE [app].[identifier] FROM bigint NOT NULL",
		"EXEC sys.sp_addextendedproperty " +
			"@name = N'realm_note', @value = N'test', " +
			"@level0type = N'SCHEMA', @level0name = N'app'",
	}
	for _, statement := range statements {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute SQL Server fixture statement: %s", statement))
	}

	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)

	var objectCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		WHERE o.is_ms_shipped = 0
		  AND s.name NOT IN (
			  N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
	`).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, 0)

	var typeCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM sys.types AS t
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE t.is_user_defined = 1
		  AND s.name NOT IN (N'sys', N'INFORMATION_SCHEMA')
	`).Scan(&typeCount)
	c.Assert(err, qt.IsNil)
	c.Assert(typeCount, qt.Equals, 0)

	var schemaCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM sys.schemas AS s
		WHERE s.name NOT IN (
			  N'dbo', N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
	`).Scan(&schemaCount)
	c.Assert(err, qt.IsNil)
	c.Assert(schemaCount, qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RejectsDatabaseScopedArtifactLive(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")
	err := writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		_, cleanupErr := db.ExecContext(
			context.Background(),
			"DROP TRIGGER IF EXISTS [realm]]guard] ON DATABASE",
		)
		c.Check(cleanupErr, qt.IsNil)
		c.Check(writer.DropDatabaseRealm(context.Background()), qt.IsNil)
	})

	_, err = db.ExecContext(t.Context(), "CREATE TABLE [dbo].[kept_until_safe] ([id] bigint NOT NULL)")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(
		t.Context(),
		"CREATE TRIGGER [realm]]guard] ON DATABASE FOR CREATE_TABLE AS RETURN",
	)
	c.Assert(err, qt.IsNil)

	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`database DDL trigger \[realm\]\]guard\] \(SQL_TRIGGER\)`,
	)

	var tableCount int
	err = db.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sys.tables WHERE object_id = OBJECT_ID(N'dbo.kept_until_safe')",
	).Scan(&tableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 1)

	_, err = db.ExecContext(t.Context(), "DROP TRIGGER IF EXISTS [realm]]guard] ON DATABASE")
	c.Assert(err, qt.IsNil)
	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
}

func TestWriterDropDatabaseRealm_RejectsPreservedSchemaPermissionLive(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")
	err := writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		_, cleanupErr := db.ExecContext(
			context.Background(),
			"REVOKE SELECT ON SCHEMA::[dbo] FROM [public]",
		)
		c.Check(cleanupErr, qt.IsNil)
		c.Check(writer.DropDatabaseRealm(context.Background()), qt.IsNil)
	})

	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON SCHEMA::[dbo] TO [public]")
	c.Assert(err, qt.IsNil)

	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`schema permission \[dbo: GRANT SELECT TO public\] \(SCHEMA_PERMISSION\)`,
	)

	_, err = db.ExecContext(t.Context(), "REVOKE SELECT ON SCHEMA::[dbo] FROM [public]")
	c.Assert(err, qt.IsNil)
	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
}
