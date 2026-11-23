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
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The tests in this file run the `ptah seed --protected-table` fence against
// Oracle. The fence lists the tables of the target's schema, and Oracle has no
// information_schema: without the seed table query of the Oracle descriptor
// every fenced run fails with ORA-00942 before it seeds or refuses.

// oracleSeedFenceFile is the seed file [oracleFencedTable] writes. The tracker
// row it leaves is deleted after the test, since the tracker is shared.
const oracleSeedFenceFile = "001_protected_fence.dev.sql"

// oracleDeleteFenceRow deletes the tracker row of [oracleSeedFenceFile], and
// does nothing where no run created the tracker: ORA-00942 is table or view
// does not exist.
const oracleDeleteFenceRow = `BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM schema_seeds WHERE seed_path = ''` + oracleSeedFenceFile + `''';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;`

// oracleFencedTable creates a table of this test's own on the Oracle server
// and a dev seed that inserts one row into it. It returns the seed directory,
// the database URL, the table name and the schema the connection reports.
func oracleFencedTable(c *qt.C) (dir, dbURL, table, schema string) {
	c.Helper()
	dbURL = dbtarget.URL(c, dbtarget.Oracle)
	table = fmt.Sprintf("seed_fence_%d", time.Now().UnixNano()%1_000_000_000)

	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema = conn.Info().Schema
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE "+table+" (id INT PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	// The test's context is canceled before a cleanup runs, so the cleanup
	// connects with a context that is not.
	c.Cleanup(func() {
		ctx := context.WithoutCancel(c.Context())
		cleanup, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(cleanup)
		for _, statement := range []string{fmt.Sprintf(oracleDropTableIfExists, table), oracleDeleteFenceRow} {
			_, err := cleanup.ExecContext(ctx, statement)
			c.Check(err, qt.IsNil, qt.Commentf("%s", statement))
		}
	})

	dir = c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "seeds"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "seeds", oracleSeedFenceFile),
		[]byte("INSERT INTO "+table+" (id) VALUES (1);\n"), 0o600), qt.IsNil)
	return dir, dbURL, table, schema
}

// oracleFenceEntry spells a --protected-table entry or a refusal, with {table}
// and {schema} standing for the table of [oracleFencedTable] and the
// connection's schema in lower case, {SCHEMA} for the schema as the connection
// reports it, and {entry} for entry, once it is spelled.
func oracleFenceEntry(text, table, schema, entry string) string {
	return strings.NewReplacer(
		"{table}", table, "{schema}", strings.ToLower(schema), "{SCHEMA}", schema, "{entry}", entry,
	).Replace(text)
}

// oracleFencedSeed runs `ptah seed` over the seeds of [oracleFencedTable] with
// one --protected-table entry.
func oracleFencedSeed(c *qt.C, dir, dbURL, entry string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"seed", "--db-url", dbURL, "--seeds-dir", "seeds", "--env", "dev", "--protected-table", entry)
}

// oracleSeededRows counts the rows of table.
func oracleSeededRows(c *qt.C, dbURL, table string) []string {
	c.Helper()
	return limitHistoryColumn(c, dbURL, "SELECT TO_CHAR(COUNT(*)) FROM "+table)
}

func TestSeedProtectedTableOracleE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		entry string
	}{
		{name: "a table the schema does not hold", entry: "{table}_absent"},
		{name: "a table the schema does not hold, by the schema", entry: "{schema}.{table}_absent"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbURL, table, schema := oracleFencedTable(c)

			got := oracleFencedSeed(c, dir, dbURL, oracleFenceEntry(tc.entry, table, schema, ""))

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(oracleSeededRows(c, dbURL, table), qt.DeepEquals, []string{"1"})
		})
	}
}

func TestSeedProtectedTableOracleE2E_FailurePath(t *testing.T) {
	// fencedRefusal is the refusal of an entry that fences a table.
	const fencedRefusal = "error: error applying seeds: refusing to seed target database because protected tables exist: " +
		"{entry}; pass --allow-prod to override\n"
	tests := []struct {
		name       string
		entry      string
		wantStderr string
	}{
		// The catalog holds the unquoted name in upper case; the entry matches
		// it case-insensitively.
		{name: "the table by its bare name", entry: "{table}", wantStderr: fencedRefusal},
		{name: "the table by the connection's schema", entry: "{schema}.{table}", wantStderr: fencedRefusal},
		// The fence lists the tables of the connection's schema alone, so an
		// entry qualified by another schema could never fence one.
		{
			name:  "a table of another schema",
			entry: "other.{table}",
			wantStderr: `error: error applying seeds: refusing to seed target database: the protected table entry "{entry}" ` +
				`names a schema other than "{SCHEMA}", and the fence checks the tables of "{SCHEMA}" alone; name the table ` +
				`without a schema, or qualified by "{SCHEMA}"` + "\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbURL, table, schema := oracleFencedTable(c)
			entry := oracleFenceEntry(tc.entry, table, schema, "")

			got := oracleFencedSeed(c, dir, dbURL, entry)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, oracleFenceEntry(tc.wantStderr, table, schema, entry))
			c.Assert(oracleSeededRows(c, dbURL, table), qt.DeepEquals, []string{"0"})
		})
	}
}
