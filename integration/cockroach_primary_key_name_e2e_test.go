//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// cockroachPrimaryKeyNames names a primary key in each spelling a name takes:
// a quoted name on the table and on the column, and an unquoted name in mixed
// case, which CockroachDB folds as PostgreSQL does. CockroachDB renames a
// primary key in place, so a name the comparison reads wrong is planned as a
// rename.
const cockroachPrimaryKeyNames = `CREATE TABLE "Line" ("Id" bigint NOT NULL, CONSTRAINT "Line_Key" PRIMARY KEY ("Id"));
CREATE TABLE "Col" ("Id" bigint CONSTRAINT "Col_PK" PRIMARY KEY);
CREATE TABLE plain_mixed (id integer, CONSTRAINT Plain_Mixed_PK PRIMARY KEY (id));
`

// TestCockroachPrimaryKeyNamesCompareSyncedE2E builds the tables and compares
// them with a file declaring the same statements. No key is renamed, to its
// own name or to the case the file writes.
func TestCockroachPrimaryKeyNamesCompareSyncedE2E(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_pkname_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	database := replaceDatabaseName(c, adminURL, name)
	execPostgresFamilySQL(c, c.Context(), database, cockroachPrimaryKeyNames)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(cockroachPrimaryKeyNames), 0o600), qt.IsNil)

	compared := runPtahIn(c, "schema", "compare", "--db-url", database, "--schema-file", schema, "--exit-code")

	c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
	c.Assert(compared.Stdout, qt.Contains, "No schema differences detected.\n")
}
