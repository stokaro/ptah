//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// Exercise both public command trees against replayed history. Catalog checks
// use PostgreSQL directly so a second Ptah omission cannot make the test pass.
func TestPostgresRoleBootstrapGenerationE2E(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		format   migrationfile.DirFormat
		generate func(dir, source, dev string) (string, error)
	}{
		{name: "compat", pattern: "*.sql", format: migrationfile.DirFormatAtlas, generate: func(dir, source, dev string) (string, error) {
			return runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+source, "--dev-url", dev)
		}},
		{name: "native", pattern: "*.up.sql", format: migrationfile.DirFormatPtah, generate: func(dir, source, dev string) (string, error) {
			return runPtahNativeOutcome("migrations", "generate", "--replay", "--dir-format", "ptah", "--migrations-dir", dir, "--schema-file", source, "--dev-url", dev)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := fmt.Sprintf("bootstrap_reader_%d", time.Now().UnixNano())
			writer := reader + "_w"
			devURL := devReplayDockerURL
			targetURL, admin := scratchReplayDatabase(c)
			target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(target) })
			c.Cleanup(func() {
				rows, queryErr := target.QueryContext(context.Background(), "SELECT rolname FROM pg_roles WHERE rolname IN ($1, $2)", reader, writer)
				c.Assert(queryErr, qt.IsNil)
				defer rows.Close()
				var roles []string
				for rows.Next() {
					var role string
					c.Assert(rows.Scan(&role), qt.IsNil)
					roles = append(roles, role)
				}
				c.Assert(rows.Err(), qt.IsNil)
				for _, role := range roles {
					_, cleanupErr := target.ExecContext(context.Background(), "DROP OWNED BY "+quoteE2EIdent(role))
					c.Check(cleanupErr, qt.IsNil)
					_, cleanupErr = admin.ExecContext(context.Background(), "DROP ROLE "+quoteE2EIdent(role))
					c.Check(cleanupErr, qt.IsNil)
				}
			})
			root := c.TempDir()
			dir := filepath.Join(root, "migrations")
			c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
			source := filepath.Join(root, "schema.sql")
			initial := bootstrapRoleSQL(reader) + fmt.Sprintf(`
 CREATE TABLE documents (id bigint PRIMARY KEY, body text NOT NULL);
 GRANT USAGE ON SCHEMA public TO %s;
 GRANT SELECT ON TABLE documents TO %s;
 `, reader, reader)
			c.Assert(os.WriteFile(source, []byte(initial), 0o600), qt.IsNil)
			out, err := test.generate(dir, source, devURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			migrations, err := filepath.Glob(filepath.Join(dir, test.pattern))
			c.Assert(err, qt.IsNil)
			c.Assert(migrations, qt.HasLen, 1)
			first, err := os.ReadFile(migrations[0])
			c.Assert(err, qt.IsNil)
			c.Assert(string(first), qt.Contains, "CREATE ROLE")
			// Generation must not execute desired SQL on the dev server.
			assertBootstrapRoleAbsent(c, admin, reader)
			_, err = target.ExecContext(c.Context(), string(first))
			c.Assert(err, qt.IsNil)
			assertBootstrapRoleCatalog(c, target, reader, false, false)

			// Simulate existing procedural history instead of rewriting it to match
			// the newly generated spelling. The next run must retain these bytes.
			c.Assert(os.WriteFile(migrations[0], []byte(initial), 0o600), qt.IsNil)
			_, err = migratesum.WriteWithFormat(dir, test.format)
			c.Assert(err, qt.IsNil)
			desired := initial + bootstrapRoleSQL(writer) + fmt.Sprintf(`
 GRANT USAGE ON SCHEMA public TO %s;
 GRANT SELECT, INSERT ON TABLE documents TO %s;
 `, writer, writer)
			c.Assert(os.WriteFile(source, []byte(desired), 0o600), qt.IsNil)
			out, err = test.generate(dir, source, devURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			history, err := os.ReadFile(migrations[0])
			c.Assert(err, qt.IsNil)
			c.Assert(string(history), qt.Equals, initial)
			migrations, err = filepath.Glob(filepath.Join(dir, test.pattern))
			c.Assert(err, qt.IsNil)
			c.Assert(migrations, qt.HasLen, 2)
			next, err := os.ReadFile(migrations[1])
			c.Assert(err, qt.IsNil)
			assertBootstrapRoleAbsent(c, admin, writer)
			_, err = target.ExecContext(c.Context(), string(next))
			c.Assert(err, qt.IsNil)
			assertBootstrapRoleCatalog(c, target, reader, false, false)
			assertBootstrapRoleCatalog(c, target, writer, true, false)

			// Native generation leaves checksum publication to the caller.
			_, err = migratesum.WriteWithFormat(dir, test.format)
			c.Assert(err, qt.IsNil)
			// A change inside the selected branch follows the existing role diff.
			changed := strings.Replace(desired, "NOLOGIN NOINHERIT", "NOLOGIN NOINHERIT CREATEDB", 1)
			c.Assert(os.WriteFile(source, []byte(changed), 0o600), qt.IsNil)
			out, err = test.generate(dir, source, devURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			migrations, err = filepath.Glob(filepath.Join(dir, test.pattern))
			c.Assert(err, qt.IsNil)
			c.Assert(migrations, qt.HasLen, 3)
			alter, err := os.ReadFile(migrations[2])
			c.Assert(err, qt.IsNil)
			c.Assert(string(alter), qt.Contains, "ALTER ROLE")
			_, err = target.ExecContext(c.Context(), string(alter))
			c.Assert(err, qt.IsNil)
			assertBootstrapRoleCatalog(c, target, reader, false, true)
			_, err = migratesum.WriteWithFormat(dir, test.format)
			c.Assert(err, qt.IsNil)
			before := bootstrapHistory(c, dir)
			out, err = test.generate(dir, source, devURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(bootstrapHistory(c, dir), qt.DeepEquals, before)
			// A partial bootstrap must not publish an incremental migration.
			hidden := reader + "_hidden"
			invalid := changed + fmt.Sprintf("DO $$ BEGIN CREATE ROLE %s; EXECUTE 'SELECT 1'; END $$;", hidden)
			c.Assert(os.WriteFile(source, []byte(invalid), 0o600), qt.IsNil)
			out, err = test.generate(dir, source, devURL)
			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Contains, "desired-state DO block")
			c.Assert(bootstrapHistory(c, dir), qt.DeepEquals, before)
			assertBootstrapRoleAbsent(c, admin, hidden)
		})
	}
}

func bootstrapRoleSQL(role string) string {
	return fmt.Sprintf(`DO $bootstrap$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '%[1]s') THEN
 CREATE ROLE %[1]s NOLOGIN NOINHERIT;
 END IF;
 END; $bootstrap$;
 `, role)
}

func assertBootstrapRoleAbsent(c *qt.C, db *dbschema.DatabaseConnection, role string) {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_roles WHERE rolname = $1", role).Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func assertBootstrapRoleCatalog(c *qt.C, db *dbschema.DatabaseConnection, role string, insert, createDB bool) {
	c.Helper()
	var login, inherit, selectPrivilege, insertPrivilege, createdb bool
	c.Assert(db.QueryRowContext(c.Context(), `SELECT rolcanlogin, rolinherit, rolcreatedb,
 has_table_privilege(oid, 'public.documents', 'SELECT'),
 has_table_privilege(oid, 'public.documents', 'INSERT')
 FROM pg_roles WHERE rolname = $1`, role).Scan(&login, &inherit, &createdb, &selectPrivilege, &insertPrivilege), qt.IsNil)
	c.Assert(login, qt.IsFalse)
	c.Assert(inherit, qt.IsFalse)
	c.Assert(createdb, qt.Equals, createDB)
	c.Assert(selectPrivilege, qt.IsTrue)
	c.Assert(insertPrivilege, qt.Equals, insert)
}

func bootstrapHistory(c *qt.C, dir string) map[string]string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	snapshot := make(map[string]string, len(entries))
	for _, entry := range entries {
		data, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		c.Assert(readErr, qt.IsNil)
		snapshot[entry.Name()] = string(data)
	}
	return snapshot
}
