//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// cockroachVisibilitySchema is a table with two secondary indexes: k_a, a
// partial one, and k_b. aHidden and bHidden follow each index's definition, so
// a row hides either one with " NOT VISIBLE". CockroachDB takes the clause
// after WHERE, which is where k_a carries it.
func cockroachVisibilitySchema(aHidden, bHidden string) string {
	return "CREATE TABLE ic (id INT8 PRIMARY KEY, a INT8, b INT8);\n" +
		"CREATE INDEX k_a ON ic (a) WHERE a > 0" + aHidden + ";\n" +
		"CREATE INDEX k_b ON ic (b)" + bHidden + ";\n"
}

// cockroachVisibilityDatabase builds a database from ddl, which may be empty,
// on the CockroachDB server and returns its URL and a read of the secondary indexes of ic, each
// with whether the optimizer uses it as information_schema.statistics records
// it. The database is dropped when the test ends.
func cockroachVisibilityDatabase(c *qt.C, ctx context.Context, ddl string) (string, func() []string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_vis_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	dbURL := replaceDatabaseName(c, adminURL, name)

	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	for statement := range strings.SplitSeq(strings.TrimSpace(ddl), ";\n") {
		if statement == "" {
			continue
		}
		_, err := db.ExecContext(ctx, strings.TrimSuffix(statement, ";"))
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return dbURL, func() []string {
		rows, err := db.QueryContext(ctx, `SELECT DISTINCT index_name || ' visible=' || is_visible
FROM information_schema.statistics WHERE table_name = 'ic' AND index_name <> 'ic_pkey' ORDER BY 1`)
		c.Assert(err, qt.IsNil)
		defer rows.Close()
		var described []string
		for rows.Next() {
			var line string
			c.Assert(rows.Scan(&line), qt.IsNil)
			described = append(described, line)
		}
		c.Assert(rows.Err(), qt.IsNil)
		return described
	}
}

// writeCockroachVisibilitySchema writes ddl as the schema file an apply reads.
func writeCockroachVisibilitySchema(c *qt.C, ddl string) string {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(ddl), 0o600), qt.IsNil)
	return file
}

// TestSchemaApplyKeepsCockroachDBIndexVisibilityE2E applies a schema file to a
// database whose indexes are shown and hidden otherwise, or that lacks the
// table, and reads the visibility back: it is what the file's own SQL builds.
// Without the capability the render refuses the hidden index; without the
// reader the next apply plans the change again; without the planner arm the
// change is never made. The next apply is synced.
func TestSchemaApplyKeepsCockroachDBIndexVisibilityE2E(t *testing.T) {
	for _, direction := range []struct {
		name     string
		from, to string
		planned  string
	}{
		{
			name: "a table created with hidden indexes",
			from: "",
			to:   cockroachVisibilitySchema(" NOT VISIBLE", " NOT VISIBLE"),
			// The partial index carries the clause after its condition.
			planned: "WHERE a > 0 NOT VISIBLE",
		},
		{
			name:    "an index hides in place",
			from:    cockroachVisibilitySchema("", ""),
			to:      cockroachVisibilitySchema("", " NOT VISIBLE"),
			planned: `ALTER INDEX "ic"@"k_b" NOT VISIBLE;`,
		},
		{
			name:    "an index shows in place",
			from:    cockroachVisibilitySchema(" NOT VISIBLE", " NOT VISIBLE"),
			to:      cockroachVisibilitySchema("", " NOT VISIBLE"),
			planned: `ALTER INDEX "ic"@"k_a" VISIBLE;`,
		},
	} {
		t.Run(direction.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			_, built := cockroachVisibilityDatabase(c, ctx, direction.to)
			want := built()
			target, readBack := cockroachVisibilityDatabase(c, ctx, direction.from)
			schema := writeCockroachVisibilitySchema(c, direction.to)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(plan, qt.Contains, direction.planned)
			c.Assert(plan, qt.Not(qt.Contains), "DROP INDEX")
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(readBack(), qt.DeepEquals, want)
			plan = runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestDBReadCarriesCockroachDBIndexVisibilityE2E reads a database holding
// hidden indexes, applies the description to an empty database, and reads that
// one back: the same indexes are hidden, and the description compares synced
// with both databases.
func TestDBReadCarriesCockroachDBIndexVisibilityE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	source, sourceBack := cockroachVisibilityDatabase(c, ctx, cockroachVisibilitySchema(" NOT VISIBLE", ""))
	target, targetBack := cockroachVisibilityDatabase(c, ctx, "")

	read, readErr, err := runPtahSplitStreams(ctx, []string{"db", "read", "--db-url", source})
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
	rendered := writeCockroachVisibilitySchema(c, read)
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", rendered, "--auto-approve")

	c.Assert(targetBack(), qt.DeepEquals, sourceBack())
	c.Assert(targetBack(), qt.DeepEquals, []string{"k_a visible=NO", "k_b visible=YES"})
	for _, dbURL := range []string{source, target} {
		compared, comparedErr, err := runPtahSplitStreams(ctx, []string{
			"schema", "compare", "--schema-file", rendered, "--db-url", dbURL, "--exit-code",
		})
		c.Assert(err, qt.IsNil, qt.Commentf("%s\nstdout:\n%s\nstderr:\n%s", dbURL, compared, comparedErr))
	}
}

// TestDBReadRefusesAPartiallyVisibleCockroachDBIndexE2E reads a database whose
// index the optimizer uses for half the queries. The model holds a shown or a
// hidden index, and either reading would move it on the next apply, so the read
// is refused and names the index.
func TestDBReadRefusesAPartiallyVisibleCockroachDBIndexE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	source, _ := cockroachVisibilityDatabase(c, ctx,
		"CREATE TABLE ic (id INT8 PRIMARY KEY, a INT8);\nCREATE INDEX k_a ON ic (a) VISIBILITY 0.5;\n")

	_, readErr, err := runPtahSplitStreams(ctx, []string{"db", "read", "--db-url", source})

	c.Assert(err, qt.IsNotNil)
	c.Assert(readErr, qt.Contains,
		`index "k_a" is partially visible (VISIBILITY 0.50), which Ptah does not model; make it VISIBLE or NOT VISIBLE`)
}
