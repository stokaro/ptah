package dbschema_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestSQLServerLiveReadSchema(t *testing.T) {
	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live schema tests")
	}
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoteSQLServerIdentifier(schemaName)+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(schemaName)+".[order]")
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(schemaName)+".[users]")
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteSQLServerIdentifier(schemaName))
	}()

	statements := []string{
		`CREATE TABLE ` + quoteSQLServerIdentifier(schemaName) + `.[users] (
			[id] INT IDENTITY(1,1) PRIMARY KEY,
			[email] NVARCHAR(320) NOT NULL,
			[email_lc] AS (LOWER([email])),
			[email_len] AS (LEN([email])) PERSISTED,
			[status] NVARCHAR(255) NOT NULL CONSTRAINT [ck_users_status] CHECK ([status] IN ('active', 'blocked')),
			CONSTRAINT [uk_users_email] UNIQUE ([email])
		);`,
		`CREATE TABLE ` + quoteSQLServerIdentifier(schemaName) + `.[order] (
			[id] INT IDENTITY(1,1) PRIMARY KEY,
			[user_id] INT NOT NULL,
			CONSTRAINT [fk_orders_user] FOREIGN KEY ([user_id]) REFERENCES ` + quoteSQLServerIdentifier(schemaName) + `.[users]([id])
		);`,
		`CREATE INDEX [idx_orders_user_id] ON ` + quoteSQLServerIdentifier(schemaName) + `.[order] ([user_id]);`,
		`CREATE INDEX [idx_users_email_covering] ON ` + quoteSQLServerIdentifier(schemaName) + `.[users] ([email]) INCLUDE ([status]);`,
	}
	for _, stmt := range statements {
		_, err = conn.ExecContext(ctx, stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", stmt))
	}

	schema, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 2)
	c.Assert(schema.Indexes, qt.HasLen, 2)
	c.Assert(schema.Constraints, qt.Not(qt.HasLen), 0)

	users := findSQLServerTable(schema.Tables, schemaName, "users")
	c.Assert(users, qt.IsNotNil)
	id := findSQLServerColumn(users.Columns, "id")
	c.Assert(id, qt.IsNotNil)
	c.Assert(id.IsAutoIncrement, qt.IsTrue)
	c.Assert(id.IsPrimaryKey, qt.IsTrue)
	c.Assert(id.ColumnType, qt.Equals, "INT")

	email := findSQLServerColumn(users.Columns, "email")
	c.Assert(email, qt.IsNotNil)
	c.Assert(email.ColumnType, qt.Equals, "NVARCHAR(320)")
	c.Assert(email.IsNullable, qt.Equals, "NO")
	c.Assert(email.IsUnique, qt.IsTrue)

	emailLC := findSQLServerColumn(users.Columns, "email_lc")
	c.Assert(emailLC, qt.IsNotNil)
	c.Assert(emailLC.GeneratedExpression, qt.IsNotNil)
	c.Assert(strings.ToLower(*emailLC.GeneratedExpression), qt.Contains, "lower")
	c.Assert(emailLC.GeneratedKind, qt.Equals, "")

	emailLen := findSQLServerColumn(users.Columns, "email_len")
	c.Assert(emailLen, qt.IsNotNil)
	c.Assert(emailLen.GeneratedExpression, qt.IsNotNil)
	c.Assert(strings.ToLower(*emailLen.GeneratedExpression), qt.Contains, "len")
	c.Assert(emailLen.GeneratedKind, qt.Equals, "PERSISTED")

	orderTable := findSQLServerTable(schema.Tables, schemaName, "order")
	c.Assert(orderTable, qt.IsNotNil)
	index := findSQLServerIndex(schema.Indexes, schemaName, "order", "idx_orders_user_id")
	c.Assert(index, qt.IsNotNil)
	c.Assert(index.Columns, qt.DeepEquals, []string{"user_id"})
	coveringIndex := findSQLServerIndex(schema.Indexes, schemaName, "users", "idx_users_email_covering")
	c.Assert(coveringIndex, qt.IsNotNil)
	c.Assert(coveringIndex.Columns, qt.DeepEquals, []string{"email"})

	check := findSQLServerConstraint(schema.Constraints, schemaName, "users", "ck_users_status")
	c.Assert(check, qt.IsNotNil)
	c.Assert(check.Type, qt.Equals, "CHECK")
	c.Assert(check.CheckClause, qt.IsNotNil)
	c.Assert(*check.CheckClause, qt.Contains, "[status]")
	c.Assert(*check.CheckClause, qt.Contains, "active")

	unique := findSQLServerConstraint(schema.Constraints, schemaName, "users", "uk_users_email")
	c.Assert(unique, qt.IsNotNil)
	c.Assert(unique.Type, qt.Equals, "UNIQUE")
	c.Assert(unique.ColumnNames, qt.DeepEquals, []string{"email"})

	fk := findSQLServerConstraint(schema.Constraints, schemaName, "order", "fk_orders_user")
	c.Assert(fk, qt.IsNotNil)
	c.Assert(fk.Type, qt.Equals, "FOREIGN KEY")
	c.Assert(fk.ColumnNames, qt.DeepEquals, []string{"user_id"})
	c.Assert(fk.ForeignSchema, qt.Equals, schemaName)
	c.Assert(fk.ForeignTable, qt.IsNotNil)
	c.Assert(*fk.ForeignTable, qt.Equals, "users")
	c.Assert(fk.ForeignColumns, qt.DeepEquals, []string{"id"})
}

func TestSQLServerLiveDropAllTablesDropsForeignKeys(t *testing.T) {
	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live schema tests")
	}
	c := qt.New(t)
	ctx := context.Background()

	adminConn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(adminConn)

	schemaName := fmt.Sprintf("ptah_drop_%d", time.Now().UnixNano())
	_, err = adminConn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoteSQLServerIdentifier(schemaName)+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = adminConn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(schemaName)+".[schema_migrations]")
		_, _ = adminConn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(schemaName)+".[child]")
		_, _ = adminConn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(schemaName)+".[parent]")
		_, _ = adminConn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteSQLServerIdentifier(schemaName))
	}()

	scopedConn, err := dbschema.ConnectToDatabase(ctx, sqlServerURLWithSchema(dbURL, schemaName))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(scopedConn)

	statements := []string{
		`CREATE TABLE ` + quoteSQLServerIdentifier(schemaName) + `.[parent] ([id] INT NOT NULL PRIMARY KEY);`,
		`CREATE TABLE ` + quoteSQLServerIdentifier(schemaName) + `.[child] (
			[id] INT NOT NULL PRIMARY KEY,
			[parent_id] INT NOT NULL,
			CONSTRAINT [fk_child_parent] FOREIGN KEY ([parent_id]) REFERENCES ` + quoteSQLServerIdentifier(schemaName) + `.[parent]([id])
		);`,
		`CREATE TABLE ` + quoteSQLServerIdentifier(schemaName) + `.[schema_migrations] ([version] BIGINT NOT NULL PRIMARY KEY);`,
	}
	for _, stmt := range statements {
		_, err = scopedConn.ExecContext(ctx, stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", stmt))
	}

	c.Assert(scopedConn.SchemaWriter().DropAllTables(), qt.IsNil)

	schema, err := dbschema.ReadSchemaWithSchemas(scopedConn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 0)
	c.Assert(sqlServerLiveTableExists(t, scopedConn, schemaName, "schema_migrations"), qt.IsFalse)
}

func TestSQLServerLiveComputedColumnZeroDiff(t *testing.T) {
	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live schema tests")
	}
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_computed_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoteSQLServerIdentifier(schemaName)+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(schemaName)+".[users]")
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteSQLServerIdentifier(schemaName))
	}()

	_, err = conn.ExecContext(ctx, `CREATE TABLE `+quoteSQLServerIdentifier(schemaName)+`.[users] (
		[id] INT NOT NULL PRIMARY KEY,
		[email] NVARCHAR(320) NOT NULL,
		[email_lc] AS (LOWER([email])) PERSISTED,
		[status] NVARCHAR(32) NOT NULL DEFAULT (N'pending'),
		[retries] INT NOT NULL DEFAULT ((0)),
		[created_at] DATETIME2 NOT NULL DEFAULT (sysdatetime())
	);`)
	c.Assert(err, qt.IsNil)

	liveSchema, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	generated := &goschema.Database{
		Tables: []goschema.Table{{
			Name:       "users",
			Schema:     schemaName,
			StructName: "User",
		}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INT", Primary: true, Nullable: false},
			{StructName: "User", Name: "email", Type: "NVARCHAR(320)", Nullable: false},
			{StructName: "User", Name: "email_lc", Type: "NVARCHAR(320)", Nullable: true, GeneratedExpression: "lower(email)"},
			{StructName: "User", Name: "status", Type: "NVARCHAR(32)", Nullable: false, Default: "pending"},
			{StructName: "User", Name: "retries", Type: "INT", Nullable: false, Default: "0"},
			{StructName: "User", Name: "created_at", Type: "DATETIME2", Nullable: false, DefaultExpr: "sysdatetime()"},
		},
	}

	diff := schemadiff.CompareWithDialect(generated, liveSchema, "sqlserver")
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

func TestSQLServerLiveDropAllTablesRejectsExternalForeignKeys(t *testing.T) {
	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live schema tests")
	}
	c := qt.New(t)
	ctx := context.Background()

	adminConn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(adminConn)

	schemaName := fmt.Sprintf("ptah_block_%d", time.Now().UnixNano())
	externalSchemaName := schemaName + "_ext"
	_, err = adminConn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoteSQLServerIdentifier(schemaName)+"')")
	c.Assert(err, qt.IsNil)
	_, err = adminConn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoteSQLServerIdentifier(externalSchemaName)+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = adminConn.ExecContext(ctx, "ALTER TABLE "+quoteSQLServerIdentifier(externalSchemaName)+".[external_child] DROP CONSTRAINT [fk_external_child_parent]")
		_, _ = adminConn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(externalSchemaName)+".[external_child]")
		_, _ = adminConn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(schemaName)+".[parent]")
		_, _ = adminConn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteSQLServerIdentifier(externalSchemaName))
		_, _ = adminConn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteSQLServerIdentifier(schemaName))
	}()

	scopedConn, err := dbschema.ConnectToDatabase(ctx, sqlServerURLWithSchema(dbURL, schemaName))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(scopedConn)

	statements := []string{
		`CREATE TABLE ` + quoteSQLServerIdentifier(schemaName) + `.[parent] ([id] INT NOT NULL PRIMARY KEY);`,
		`CREATE TABLE ` + quoteSQLServerIdentifier(externalSchemaName) + `.[external_child] (
			[id] INT NOT NULL PRIMARY KEY,
			[parent_id] INT NOT NULL,
			CONSTRAINT [fk_external_child_parent] FOREIGN KEY ([parent_id]) REFERENCES ` + quoteSQLServerIdentifier(schemaName) + `.[parent]([id])
		);`,
	}
	for _, stmt := range statements {
		_, err = scopedConn.ExecContext(ctx, stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", stmt))
	}

	err = scopedConn.SchemaWriter().DropAllTables()
	c.Assert(err, qt.ErrorMatches, `sqlserver: cannot drop schema .* tables because external foreign keys reference them: .*fk_external_child_parent.*`)

	schema, err := dbschema.ReadSchemaWithSchemas(scopedConn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)

	externalSchema, err := dbschema.ReadSchemaWithSchemas(scopedConn, []string{externalSchemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(externalSchema.Tables, qt.HasLen, 1)
	externalFK := findSQLServerConstraint(externalSchema.Constraints, externalSchemaName, "external_child", "fk_external_child_parent")
	c.Assert(externalFK, qt.IsNotNil)
	c.Assert(externalFK.ForeignSchema, qt.Equals, schemaName)
	c.Assert(externalFK.ForeignTable, qt.IsNotNil)
	c.Assert(*externalFK.ForeignTable, qt.Equals, "parent")
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

func sqlServerLiveTableExists(t *testing.T, conn *dbschema.DatabaseConnection, schemaName, tableName string) bool {
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = @p1 AND t.name = @p2
`, schemaName, tableName).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count > 0
}

func findSQLServerTable(tables []dbschematypes.DBTable, schemaName, tableName string) *dbschematypes.DBTable {
	for i := range tables {
		if tables[i].Schema == schemaName && tables[i].Name == tableName {
			return &tables[i]
		}
	}
	return nil
}

func findSQLServerColumn(columns []dbschematypes.DBColumn, columnName string) *dbschematypes.DBColumn {
	for i := range columns {
		if columns[i].Name == columnName {
			return &columns[i]
		}
	}
	return nil
}

func findSQLServerIndex(indexes []dbschematypes.DBIndex, schemaName, tableName, indexName string) *dbschematypes.DBIndex {
	for i := range indexes {
		if indexes[i].Schema == schemaName && indexes[i].TableName == tableName && indexes[i].Name == indexName {
			return &indexes[i]
		}
	}
	return nil
}

func findSQLServerConstraint(constraints []dbschematypes.DBConstraint, schemaName, tableName, constraintName string) *dbschematypes.DBConstraint {
	for i := range constraints {
		if constraints[i].Schema == schemaName && constraints[i].TableName == tableName && constraints[i].Name == constraintName {
			return &constraints[i]
		}
	}
	return nil
}
