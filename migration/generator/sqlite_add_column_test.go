package generator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestGenerateMigration_SQLiteAddColumnHasApplicableDownMigration(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	tempDir := t.TempDir()
	modelsDir := filepath.Join(tempDir, "models")
	migrationsDir := filepath.Join(tempDir, "migrations")
	dbURL := "sqlite://" + filepath.Join(tempDir, "app.db")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(sqliteAddColumnModel), 0o600), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE INDEX idx_users_email ON users (email)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE TRIGGER trg_users_email AFTER UPDATE ON users FOR EACH ROW BEGIN SELECT NEW.email; END`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `INSERT INTO users (id, email) VALUES (1, 'a@example.test')`)
	c.Assert(err, qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: modelsDir,
		DBConn:        conn,
		MigrationName: "add_name",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	downSQL, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downSQL), qt.Contains, `CREATE TABLE "__ptah_rebuild_users"`)
	c.Assert(string(downSQL), qt.Not(qt.Contains), "DROP COLUMN")

	mig, err := migrator.NewFSMigrator(conn, os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(sqliteColumnCount(c, conn, "users", "name"), qt.Equals, 1)

	c.Assert(mig.MigrateDownTo(ctx, 0), qt.IsNil)
	c.Assert(sqliteColumnCount(c, conn, "users", "name"), qt.Equals, 0)
	c.Assert(sqliteSchemaObjectCount(c, conn, "index", "idx_users_email", "users"), qt.Equals, 1)
	c.Assert(sqliteSchemaObjectCount(c, conn, "trigger", "trg_users_email", "users"), qt.Equals, 1)
	c.Assert(sqliteUserEmail(c, conn), qt.Equals, "a@example.test")
}

func TestGenerateMigration_SQLiteAddColumnPreservesStrictWithoutRowID(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	tempDir := t.TempDir()
	modelsDir := filepath.Join(tempDir, "models")
	migrationsDir := filepath.Join(tempDir, "migrations")
	dbURL := "sqlite://" + filepath.Join(tempDir, "app.db")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(sqliteStrictAddColumnModel), 0o600), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(ctx, `CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT NOT NULL) WITHOUT ROWID, STRICT`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `INSERT INTO users (id, email) VALUES ('u1', 'strict@example.test')`)
	c.Assert(err, qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: modelsDir,
		DBConn:        conn,
		MigrationName: "add_name",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)

	mig, err := migrator.NewFSMigrator(conn, os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(mig.MigrateDownTo(ctx, 0), qt.IsNil)
	c.Assert(sqliteTableSQL(c, conn, "users"), qt.Contains, "WITHOUT ROWID")
	c.Assert(sqliteTableSQL(c, conn, "users"), qt.Contains, "STRICT")
	c.Assert(sqliteUserEmailByID(c, conn, "u1"), qt.Equals, "strict@example.test")
}

func TestGenerateMigration_SQLiteAddColumnRejectsInboundForeignKeys(t *testing.T) {
	tests := []struct {
		name     string
		childDDL string
		model    string
	}{
		{
			name:     "no action child",
			childDDL: `CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id))`,
			model:    sqliteAddColumnWithChildModel,
		},
		{
			name:     "cascade child",
			childDDL: `CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE)`,
			model:    sqliteAddColumnWithCascadeChildModel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			tempDir := t.TempDir()
			modelsDir := filepath.Join(tempDir, "models")
			migrationsDir := filepath.Join(tempDir, "migrations")
			dbURL := "sqlite://" + filepath.Join(tempDir, "app.db")
			c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
			c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(modelsDir, "schema.go"), []byte(tt.model), 0o600), qt.IsNil)

			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`)
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(ctx, tt.childDDL)
			c.Assert(err, qt.IsNil)

			files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
				GoEntitiesDir: modelsDir,
				DBConn:        conn,
				MigrationName: "add_name",
				OutputDir:     migrationsDir,
			})
			c.Assert(files, qt.IsNil)
			c.Assert(err, qt.ErrorMatches, `.*sqlite: rebuilding table users with inbound foreign keys requires a manual rebuild plan.*`)
		})
	}
}

func TestGenerateMigration_SQLiteAddColumnRejectsUnsupportedTriggerSyntax(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	tempDir := t.TempDir()
	modelsDir := filepath.Join(tempDir, "models")
	migrationsDir := filepath.Join(tempDir, "migrations")
	dbURL := "sqlite://" + filepath.Join(tempDir, "app.db")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "user.go"), []byte(sqliteAddColumnWithTriggerModel), 0o600), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE TRIGGER trg_users_email AFTER UPDATE OF email ON users FOR EACH ROW BEGIN SELECT NEW.email; END`)
	c.Assert(err, qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: modelsDir,
		DBConn:        conn,
		MigrationName: "add_name",
		OutputDir:     migrationsDir,
	})
	c.Assert(files, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `.*sqlite: rebuilding table users with trigger trg_users_email requires a manual rebuild plan.*`)
}

const sqliteAddColumnModel = `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:index name="idx_users_email" fields="email"
	//migrator:schema:trigger name="trg_users_email" table="users" timing="after" event="update" body="BEGIN SELECT NEW.email; END"

	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//migrator:schema:field name="name" type="TEXT"
	Name string
}
`

const sqliteStrictAddColumnModel = `package models

//migrator:schema:table name="users" platform.sqlite.strict="true" platform.sqlite.without_rowid="true"
type User struct {
	//migrator:schema:field name="id" type="TEXT" primary="true"
	ID string

	//migrator:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//migrator:schema:field name="name" type="TEXT"
	Name string
}
`

const sqliteAddColumnWithChildModel = `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//migrator:schema:field name="name" type="TEXT"
	Name string
}

//migrator:schema:table name="posts"
type Post struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
	UserID int64
}
`

const sqliteAddColumnWithCascadeChildModel = `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//migrator:schema:field name="name" type="TEXT"
	Name string
}

//migrator:schema:table name="posts"
type Post struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)" on_delete="CASCADE"
	UserID int64
}
`

const sqliteAddColumnWithTriggerModel = `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:trigger name="trg_users_email" table="users" timing="after" event="update" body="BEGIN SELECT NEW.email; END"

	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//migrator:schema:field name="name" type="TEXT"
	Name string
}
`

func sqliteColumnCount(c *qt.C, conn *dbschema.DatabaseConnection, table, column string) int {
	c.Helper()
	query := fmt.Sprintf("SELECT COUNT(*) FROM pragma_table_info(%s) WHERE name = ?", quoteSQLiteString(table))
	var count int
	err := conn.QueryRowContext(context.Background(), query, column).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func sqliteSchemaObjectCount(c *qt.C, conn *dbschema.DatabaseConnection, objectType, name, table string) int {
	c.Helper()
	var count int
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_schema WHERE type = ? AND name = ? AND tbl_name = ?",
		objectType,
		name,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func sqliteTableSQL(c *qt.C, conn *dbschema.DatabaseConnection, table string) string {
	c.Helper()
	var sql string
	err := conn.QueryRowContext(context.Background(), "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?", table).Scan(&sql)
	c.Assert(err, qt.IsNil)
	return sql
}

func sqliteUserEmail(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var email string
	err := conn.QueryRowContext(context.Background(), "SELECT email FROM users WHERE id = 1").Scan(&email)
	c.Assert(err, qt.IsNil)
	return email
}

func sqliteUserEmailByID(c *qt.C, conn *dbschema.DatabaseConnection, id string) string {
	c.Helper()
	var email string
	err := conn.QueryRowContext(context.Background(), "SELECT email FROM users WHERE id = ?", id).Scan(&email)
	c.Assert(err, qt.IsNil)
	return email
}

func quoteSQLiteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
