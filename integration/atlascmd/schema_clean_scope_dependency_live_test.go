//go:build integration

package atlas_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

func TestSchemaCleanScopeRefusesUnselectedPostgresDependents(t *testing.T) {
	c := qt.New(t)
	dbURL := strictCompatPostgresTestURL(t)
	schemaName := fmt.Sprintf("ptah_clean_dependency_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	admin, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schemaName)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	scopedURL := parsed.String()
	conn, err := dbschema.ConnectToDatabase(t.Context(), scopedURL)
	c.Assert(err, qt.IsNil)
	for _, statement := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY)",
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))",
		"CREATE VIEW active_users AS SELECT id FROM users",
	} {
		_, err = conn.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	dbschema.CloseAndWarn(conn)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "go.5x5.cz/ptah/cmd/ptah-compat")
	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "clean",
		"--url", scopedURL,
		"--include", "users[type=table]",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `DROP TABLE IF EXISTS "`+schemaName+`"."users" RESTRICT`)
	c.Assert(stdout, qt.Not(qt.Contains), "CASCADE")
	c.Assert(stderr, qt.Contains, "other objects depend on it")
	c.Assert(postgresScopedDependencyObjectCount(t, scopedURL), qt.Equals, 4)
}

func postgresScopedDependencyObjectCount(t *testing.T, dbURL string) int {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(t.Context(), `
		SELECT
			(SELECT count(*) FROM information_schema.tables
			 WHERE table_schema = current_schema() AND table_name IN ('users', 'posts')) +
			(SELECT count(*) FROM information_schema.views
			 WHERE table_schema = current_schema() AND table_name = 'active_users') +
			(SELECT count(*) FROM information_schema.table_constraints
			 WHERE constraint_schema = current_schema() AND constraint_name = 'posts_user_id_fkey')`,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
