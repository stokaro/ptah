//go:build integration

package generator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

func openShadowTestPostgres(c *qt.C) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Assert(platform.NormalizeDialect(conn.Info().Dialect), qt.Equals, platform.Postgres)
	return dbURL, conn
}

func createShadowTestPostgres(
	c *qt.C,
	admin *dbschema.DatabaseConnection,
	baseURL string,
) (shadowURL, database string) {
	c.Helper()
	database = fmt.Sprintf("ptah_generator_shadow_%d", time.Now().UnixNano())
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE "+quoteShadowTestPostgresIdentifier(database))
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(baseURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.RawPath = ""
	return parsed.String(), database
}

func dropShadowTestPostgres(c *qt.C, admin *dbschema.DatabaseConnection, database string) {
	c.Helper()
	_, _ = admin.ExecContext(
		context.Background(),
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
		database,
	)
	_, err := admin.ExecContext(
		context.Background(),
		"DROP DATABASE IF EXISTS "+quoteShadowTestPostgresIdentifier(database),
	)
	c.Assert(err, qt.IsNil)
}

func quoteShadowTestPostgresIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func acquireShadowTestLock(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) func() {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(156156156)")
	c.Assert(err, qt.IsNil)

	return func() {
		_, unlockErr := conn.ExecContext(ctx, "SELECT pg_advisory_unlock(156156156)")
		c.Assert(unlockErr, qt.IsNil)
	}
}

func writeShadowEntities(c *qt.C, dir string) string {
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0o755), qt.IsNil)

	content := `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT"
	Name string

	//ptah:schema:field name="email" type="TEXT"
	Email string
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0o600), qt.IsNil)
	return entitiesDir
}

func writePriorMigration(c *qt.C, dir, upSQL string) {
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0o600), qt.IsNil)
	c.Assert(
		os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE IF EXISTS users;\n"), 0o600),
		qt.IsNil,
	)
}

func prepareShadowTargetDB(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	_, err := conn.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}
