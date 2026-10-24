//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// The desired schema these tests compare with: a key written the way an ORM
// export writes it, without NOT VALID, beside a file that may allow it to stay
// NOT VALID.
const (
	allowanceTables = `CREATE TABLE owners (id bigint PRIMARY KEY);
CREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint,
  CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id)%s);
`
	allowanceDirective = `-- items_owner_fk is added NOT VALID and validated after a data check.
-- ptah:allow-not-valid CONSTRAINT items_owner_fk ON items
`
	// orphanedItems is the database the history left: a row whose owner does
	// not exist, which the key added NOT VALID did not check.
	orphanedItems = `CREATE TABLE owners (id bigint PRIMARY KEY);
CREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint);
INSERT INTO items VALUES (1, 42);
`
)

// desiredDir writes the desired schema into a directory: the key, with
// onDelete after its reference.
func desiredDir(c *qt.C, onDelete string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_tables.sql"), []byte(fmt.Sprintf(allowanceTables, onDelete)), 0o600), qt.IsNil)
	return dir
}

// withDirective adds the file that allows the key to stay NOT VALID to dir.
func withDirective(c *qt.C, dir string) string {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, "2_unvalidated.sql"), []byte(allowanceDirective), 0o600), qt.IsNil)
	return dir
}

// databaseWith is a scratch database the statements built.
func databaseWith(c *qt.C, statements string) string {
	c.Helper()
	url, _ := scratchReplayDatabase(c)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), statements)
	c.Assert(err, qt.IsNil)
	return url
}

// keyState is how the database holds items_owner_fk: its definition and
// whether the server validated it, or "" when there is no such key.
func keyState(c *qt.C, url string) (definition string, validated bool) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(),
		`SELECT pg_get_constraintdef(oid), convalidated FROM pg_constraint WHERE conname = 'items_owner_fk'`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	for rows.Next() {
		c.Assert(rows.Scan(&definition, &validated), qt.IsNil)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return definition, validated
}

// diffBothBinaries compares the database at url with dir through native ptah
// and through ptah-compat, and returns the two outputs.
func diffBothBinaries(c *qt.C, url, dir string) (native, compat string) {
	c.Helper()
	native = runPtahNative(c, "schema", "diff", "--from", url, "--to", "file://"+filepath.ToSlash(dir))
	devURL, _ := scratchReplayDatabase(c)
	compat, err := runCompatVerb("schema", "diff", "--from", url+"&search_path=public", "--to", "file://"+filepath.ToSlash(dir), "--dev-url", devURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", compat))
	return native, compat
}

// A key the database holds NOT VALID is validated where the desired schema
// requires it validated, and left as it is where a directive allows it to
// stay NOT VALID. Both binaries read the directive the same way.
func TestNotValidAllowanceKeepsAnUnvalidatedKeyE2E(t *testing.T) {
	c := qt.New(t)
	url := databaseWith(c, orphanedItems+`ALTER TABLE items ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) NOT VALID;`)

	native, compat := diffBothBinaries(c, url, desiredDir(c, ""))
	c.Assert(native, qt.Contains, `ALTER TABLE "items" VALIDATE CONSTRAINT "items_owner_fk"`)
	c.Assert(compat, qt.Contains, `ALTER TABLE "items" VALIDATE CONSTRAINT "items_owner_fk"`)

	native, compat = diffBothBinaries(c, url, withDirective(c, desiredDir(c, "")))
	c.Assert(native, qt.Contains, "Schemas are synced")
	c.Assert(compat, qt.Contains, "Schemas are synced")
	_, validated := keyState(c, url)
	c.Assert(validated, qt.IsFalse)
}

// The life of an allowed key on a table that holds a row the key would
// refuse: it is added NOT VALID, so the apply succeeds; without the directive
// the plan validates it, which fails until the row is fixed; once validated,
// the directive is still satisfied and nothing is planned.
func TestNotValidAllowanceAddsAKeyNotValidE2E(t *testing.T) {
	c := qt.New(t)
	url := databaseWith(c, orphanedItems)
	allowed, required := withDirective(c, desiredDir(c, "")), desiredDir(c, "")

	runPtahNative(c, "schema", "apply", "--schema-file", filepath.Join(allowed, "1_tables.sql"),
		"--schema-file", filepath.Join(allowed, "2_unvalidated.sql"), "--db-url", url, "--auto-approve")
	definition, validated := keyState(c, url)
	c.Assert(definition, qt.Equals, "FOREIGN KEY (owner_id) REFERENCES owners(id) NOT VALID")
	c.Assert(validated, qt.IsFalse)

	out, err := runPtahNativeWithError("schema", "apply", "--schema-file", filepath.Join(required, "1_tables.sql"),
		"--db-url", url, "--auto-approve")
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `violates foreign key constraint "items_owner_fk"`)

	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `DELETE FROM items WHERE owner_id = 42`)
	dbschema.CloseAndWarn(conn)
	c.Assert(err, qt.IsNil)
	runPtahNative(c, "schema", "apply", "--schema-file", filepath.Join(required, "1_tables.sql"), "--db-url", url, "--auto-approve")
	_, validated = keyState(c, url)
	c.Assert(validated, qt.IsTrue)

	native, compat := diffBothBinaries(c, url, allowed)
	c.Assert(native, qt.Contains, "Schemas are synced")
	c.Assert(compat, qt.Contains, "Schemas are synced")
}

// The directive allows a key to stay NOT VALID and nothing else: a change to
// the key's definition is still planned, and the key comes back NOT VALID, so
// the rows it never checked do not stop the apply.
func TestNotValidAllowanceStillPlansAChangedKeyE2E(t *testing.T) {
	c := qt.New(t)
	url := databaseWith(c, orphanedItems+`ALTER TABLE items ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) NOT VALID;`)
	cascading := withDirective(c, desiredDir(c, " ON DELETE CASCADE"))

	native, compat := diffBothBinaries(c, url, cascading)
	c.Assert(native, qt.Contains, `DROP CONSTRAINT`)
	c.Assert(native, qt.Contains, `ON DELETE CASCADE NOT VALID`)
	c.Assert(compat, qt.Contains, `ON DELETE CASCADE NOT VALID`)

	runPtahNative(c, "schema", "apply", "--schema-file", filepath.Join(cascading, "1_tables.sql"),
		"--schema-file", filepath.Join(cascading, "2_unvalidated.sql"), "--db-url", url, "--auto-approve")
	definition, validated := keyState(c, url)
	c.Assert(definition, qt.Equals, "FOREIGN KEY (owner_id) REFERENCES owners(id) ON DELETE CASCADE NOT VALID")
	c.Assert(validated, qt.IsFalse)
}

// historyAddingTheKeyNotValid is a hashed migration directory whose one
// migration adds the key NOT VALID over a row it would refuse.
func historyAddingTheKeyNotValid(c *qt.C) string {
	c.Helper()
	migrations := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrations, "20260101000000_init.sql"), []byte(orphanedItems+
		"ALTER TABLE items ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) NOT VALID;\n"), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(migrations))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return migrations
}

// migrateDiff diffs the history with dir through ptah-compat, as a versioned
// workflow generates its next migration.
func migrateDiff(c *qt.C, migrations, dir string) string {
	c.Helper()
	devURL, _ := scratchReplayDatabase(c)
	out, err := runCompatVerb("migrate", "diff", "check", "--dir", "file://"+filepath.ToSlash(migrations),
		"--to", "file://"+filepath.ToSlash(dir), "--dev-url", devURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return out
}

// A migration history that adds the key NOT VALID is synced with a desired
// schema that allows it.
func TestNotValidAllowanceInMigrateDiffE2E(t *testing.T) {
	c := qt.New(t)
	migrations := historyAddingTheKeyNotValid(c)

	out := migrateDiff(c, migrations, withDirective(c, desiredDir(c, "")))

	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
	written, err := filepath.Glob(filepath.Join(migrations, "*_check.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

// The same history gets a migration that validates the key from a desired
// schema that requires it validated.
func TestNotValidAllowanceAbsentInMigrateDiffValidatesE2E(t *testing.T) {
	c := qt.New(t)
	migrations := historyAddingTheKeyNotValid(c)

	migrateDiff(c, migrations, desiredDir(c, ""))

	written, err := filepath.Glob(filepath.Join(migrations, "*_check.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	migration, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(migration), qt.Contains, `ALTER TABLE "items" VALIDATE CONSTRAINT "items_owner_fk"`)
}
