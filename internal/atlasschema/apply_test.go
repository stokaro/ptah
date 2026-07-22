package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestPlanApply_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
`), 0o600), qt.IsNil)
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasschema.PlanApply(conn, atlasschema.ApplyOptions{
		ToURLs: []string{"file://" + schemaPath},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsTrue)
	c.Assert(plan.SQL(), qt.Contains, "CREATE TABLE")
	c.Assert(plan.SQL(), qt.Contains, "users")
}

func TestPlanApply_Synced(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "synced.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	schemaSQL := `
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`
	c.Assert(os.WriteFile(schemaPath, []byte(schemaSQL), 0o600), qt.IsNil)
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, schemaSQL), qt.IsNil)

	plan, err := atlasschema.PlanApply(conn, atlasschema.ApplyOptions{
		ToURLs: []string{"file://" + schemaPath},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsFalse)
	c.Assert(plan.SQL(), qt.Equals, "")
}

func TestPlanApply_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("nil connection", func(c *qt.C) {
		plan, err := atlasschema.PlanApply(nil, atlasschema.ApplyOptions{
			ToURLs: []string{"file:///schema.sql"},
		})
		c.Assert(err, qt.ErrorMatches, "schema apply planning requires database connection")
		c.Assert(plan.HasChanges(), qt.IsFalse)
		c.Assert(plan.SQL(), qt.Equals, "")
	})

	c.Run("empty desired schema URLs", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(t.TempDir(), "empty-to.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasschema.PlanApply(conn, atlasschema.ApplyOptions{})
		c.Assert(err, qt.ErrorMatches, "schema apply planning requires desired schema URLs")
		c.Assert(plan.HasChanges(), qt.IsFalse)
		c.Assert(plan.SQL(), qt.Equals, "")
	})
}

func TestApplySQL_TxModeAllRollsBackOnFailure(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "tx-mode-all.db")
	conn := connectSQLite(c, dbPath)
	sqlText := `
CREATE TABLE tx_mode_first (id INTEGER PRIMARY KEY);
CREATE TABLE tx_mode_first (id INTEGER PRIMARY KEY);
`

	err := atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, sqlText)
	dbschema.CloseAndWarn(conn)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to execute SQL statement")
	c.Assert(sqliteTableExists(c, dbPath, "tx_mode_first"), qt.IsFalse)
}

func TestApplySQL_TxModeNoneKeepsPriorStatementsOnFailure(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "tx-mode-none.db")
	conn := connectSQLite(c, dbPath)
	sqlText := `
CREATE TABLE tx_mode_first (id INTEGER PRIMARY KEY);
CREATE TABLE tx_mode_first (id INTEGER PRIMARY KEY);
`

	err := atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeNone, sqlText)
	dbschema.CloseAndWarn(conn)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to execute SQL statement")
	c.Assert(sqliteTableExists(c, dbPath, "tx_mode_first"), qt.IsTrue)
}

func TestApplySQL_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("nil connection", func(c *qt.C) {
		err := atlasschema.ApplySQL(context.Background(), nil, migrator.MigrationTxModeAll, "SELECT 1;")
		c.Assert(err, qt.ErrorMatches, "schema apply execution requires database connection")
	})
}

func TestSplitApplyStatements_UsesDialect(t *testing.T) {
	c := qt.New(t)
	sqlText := `
CREATE TABLE tx_mode_batch_one (id INT);
GO
CREATE TABLE tx_mode_batch_two (id INT);
GO
`

	statements := atlasschema.SplitApplyStatements(sqlText, "sqlserver")

	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE tx_mode_batch_one (id INT)",
		"CREATE TABLE tx_mode_batch_two (id INT)",
	})
}

func TestFormatMigrationSQL_HappyPath(t *testing.T) {
	c := qt.New(t)

	sqlText := atlasschema.FormatMigrationSQL([]string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY);",
		" ",
		"CREATE INDEX users_id_idx ON users (id)",
	})

	c.Assert(sqlText, qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE INDEX users_id_idx ON users (id);\n")
}

func connectSQLite(c *qt.C, dbPath string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	return conn
}

func sqliteTableExists(c *qt.C, dbPath, table string) bool {
	c.Helper()
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)

	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	return slices.ContainsFunc(schema.Tables, func(dbTable types.DBTable) bool {
		return dbTable.Name == table
	})
}
