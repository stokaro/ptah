//go:build integration

package nestedcomments_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/dbtarget"
)

// scratchDatabase creates an empty database of its own on engine and returns
// its address and a connection to it. dropSuffix ends the DROP DATABASE that
// removes it, in the spelling the engine takes.
func scratchDatabase(c *qt.C, engine dbtarget.Engine, dropSuffix string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	random := make([]byte, 6)
	_, err = rand.Read(random)
	c.Assert(err, qt.IsNil)
	name := "ptah_nested_" + hex.EncodeToString(random)
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+dropSuffix)
		c.Check(dropErr, qt.IsNil)
	})
	address, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return address, conn
}

// ptah runs the native command tree.
func ptah(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// nestedMigration holds a block comment with another inside it, and a
// semicolon and SQL-looking text in both. The server reads all of it as one
// comment, so the migration is the CREATE TABLE alone. Read with the
// standard's rule, the comment would end at the first */ and leave
// `still commented; */` to run as a statement.
const nestedMigration = `/* outer /* inner; DROP TABLE nothing; */ still commented; */
CREATE TABLE nested_comment (id int PRIMARY KEY);
`

// TestANestedCommentInAMigrationIsOneCommentE2E applies a migration with a
// nested block comment on each PostgreSQL-family engine and reads the catalog
// back: the table exists and the migration is recorded applied.
func TestANestedCommentInAMigrationIsOneCommentE2E(t *testing.T) {
	for _, test := range []struct {
		name       string
		engine     dbtarget.Engine
		dropSuffix string
	}{
		{name: "postgres", engine: dbtarget.PostgreSQL, dropSuffix: " WITH (FORCE)"},
		{name: "cockroachdb", engine: dbtarget.CockroachDB, dropSuffix: " CASCADE"},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, dropSuffix: " WITH (FORCE)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			address, conn := scratchDatabase(c, test.engine, test.dropSuffix)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_nested.up.sql"), []byte(nestedMigration), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_nested.down.sql"), []byte("DROP TABLE nested_comment;\n"), 0o600), qt.IsNil)

			out, err := ptah("migrations", "up", "--db-url", address, "--migrations-dir", dir)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var tables int
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT count(*) FROM information_schema.tables WHERE table_name = 'nested_comment'").Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 1)
		})
	}
}
