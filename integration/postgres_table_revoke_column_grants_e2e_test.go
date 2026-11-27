//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const columnPrivilegeTable = "CREATE TABLE items (id integer PRIMARY KEY, a integer, b integer);\n"

// columnPrivilegeRows reads the column ACL itself: table privileges do not
// become column privileges in this read.
func columnPrivilegeRows(c *qt.C, database devDialectDatabase, role string) []string {
	c.Helper()
	rows, err := database.conn.QueryContext(c.Context(), `
		SELECT attname || ':' || privilege_type || ':' || is_grantable::text
		FROM pg_attribute, LATERAL aclexplode(attacl)
		WHERE attrelid = 'items'::regclass AND grantee = $1::regrole
		ORDER BY 1`, role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var result []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		result = append(result, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return result
}

// Removing the table grant keeps the declared column grant in the same
// apply. The next comparison is empty, without a second repair apply.
func TestSchemaApplyPostgresTableRevokeKeepsColumnPrivilegesE2E(t *testing.T) {
	c := qt.New(t)
	role := postgresGrantOptionRole(c)
	database := postgresUnloggedDatabase(c)
	columnGrant := "GRANT SELECT (a) ON items TO " + role + " WITH GRANT OPTION;\n"
	database.exec(c, []string{columnPrivilegeTable, columnGrant, "GRANT SELECT ON items TO " + role})
	held := columnPrivilegeRows(c, database, role)
	c.Assert(held, qt.DeepEquals, []string{"a:SELECT:true"})
	desired := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(desired, []byte(columnPrivilegeTable+"CREATE ROLE "+role+";\n"+columnGrant), 0o600), qt.IsNil)

	applied, err := runPtahNativeWithError("schema", "apply", "--db-url", database.url,
		"--schema-file", desired, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(columnPrivilegeRows(c, database, role), qt.DeepEquals, held)
	var wholeTable bool
	c.Assert(database.conn.QueryRowContext(c.Context(), "SELECT has_table_privilege($1, 'items', 'SELECT')", role).Scan(&wholeTable), qt.IsNil)
	c.Assert(wholeTable, qt.IsFalse)
	again, err := runPtahNativeWithError("schema", "apply", "--db-url", database.url, "--schema-file", desired, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
	c.Assert(again, qt.Contains, "Schema is synced")
}

// Rolling back an added table grant restores the column ACL the database
// held before the migration, including its grant option.
func TestMigrationsDownPostgresTableGrantKeepsColumnPrivilegesE2E(t *testing.T) {
	c := qt.New(t)
	role := postgresGrantOptionRole(c)
	database := postgresUnloggedDatabase(c)
	columnGrant := "GRANT SELECT (a) ON items TO " + role + " WITH GRANT OPTION;\n"
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.up.sql"), []byte(columnPrivilegeTable+columnGrant), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.down.sql"), []byte("DROP TABLE items;\n"), 0o600), qt.IsNil)
	initialized, err := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", initialized))
	held := columnPrivilegeRows(c, database, role)
	c.Assert(held, qt.DeepEquals, []string{"a:SELECT:true"})
	desired := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(desired, []byte(columnPrivilegeTable+columnGrant+"GRANT SELECT ON items TO "+role+";\n"), 0o600), qt.IsNil)
	generated, err := runPtahNativeWithError("migrations", "generate", "--db-url", database.url,
		"--migrations-dir", dir, "--schema-file", desired, "--name", "table_grant")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))
	applied, err := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(columnPrivilegeRows(c, database, role), qt.DeepEquals, held)

	rolledBack, err := runPtahNativeWithError("migrations", "down", "--db-url", database.url,
		"--migrations-dir", dir, "--limit", "1", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", rolledBack))
	c.Assert(columnPrivilegeRows(c, database, role), qt.DeepEquals, held)
	var wholeTable bool
	c.Assert(database.conn.QueryRowContext(c.Context(), "SELECT has_table_privilege($1, 'items', 'SELECT')", role).Scan(&wholeTable), qt.IsNil)
	c.Assert(wholeTable, qt.IsFalse)
}
