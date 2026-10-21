//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// spannerNotNullBase is the table each test starts from, and the column the
// tests add to it.
const (
	spannerNotNullBase = "tables:\n  items:\n    fields:\n      id:\n        type: BIGINT\n        primary: true\n"
	spannerNotNullName = "      name:\n        type: TEXT\n        nullable: false\n"
)

// TestSpannerNotNullColumnE2E_HappyPath adds a NOT NULL column to a table that
// exists and plans the same schema again.
//
// Spanner refuses `ADD COLUMN c TEXT NOT NULL` with no default, on an empty
// table too: "Cannot add NOT NULL column items.name to existing table items",
// measured on the emulator behind PGAdapter v0.56.1. It accepts the column
// added nullable and made NOT NULL next, and it accepts a NOT NULL column that
// declares a default on a table holding rows, which the default fills.
func TestSpannerNotNullColumnE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		seed     []string
		column   string
		wantPlan string
		wantRows []string
	}{
		{
			name:   "no default, on an empty table",
			column: spannerNotNullName,
			wantPlan: "ALTER TABLE \"items\" ADD COLUMN \"name\" TEXT;\n" +
				"-- SPANNER: SET NOT NULL fails if any row of \"items\" holds NULL in \"name\"; " +
				"the column declares no default to fill it with.\n" +
				"ALTER TABLE \"items\" ALTER COLUMN \"name\" SET NOT NULL;\n",
			wantRows: nil,
		},
		{
			name:     "a default, on a table holding a row",
			seed:     []string{"INSERT INTO items (id) VALUES (1)"},
			column:   spannerNotNullName + "        default: \"'x'\"\n",
			wantPlan: "ALTER TABLE \"items\" ADD COLUMN \"name\" TEXT NOT NULL DEFAULT 'x';\n",
			wantRows: []string{"1 x"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL, conn := spannerNotNullDatabase(c, ctx, test.seed)
			schema := writeSpannerNotNullSchema(c, spannerNotNullBase+test.column)

			applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", schema, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
			c.Assert(applied, qt.Contains, test.wantPlan)
			c.Assert(spannerColumnNullable(c, ctx, conn, "name"), qt.Equals, "NO")
			c.Assert(spannerItemRows(c, ctx, conn), qt.DeepEquals, test.wantRows)

			reapplied, reappliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", schema, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied, reappliedErr))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSpannerNotNullColumnE2E_FailurePath adds a NOT NULL column without a
// default to a table holding a row. The row has no value for the column, so
// Spanner refuses the SET NOT NULL, as PostgreSQL refuses the same column in
// one statement. Spanner runs each statement on its own, so the column stays,
// nullable, and the next plan is the SET NOT NULL alone.
func TestSpannerNotNullColumnE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL, conn := spannerNotNullDatabase(c, ctx, []string{"INSERT INTO items (id) VALUES (1)"})
	schema := writeSpannerNotNullSchema(c, spannerNotNullBase+spannerNotNullName)

	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", schema, "--db-url", dbURL, "--auto-approve",
	})
	// Apply runs the plan's statements as planned, so the failed one is
	// reported with the notice the plan wrote above it.
	c.Assert(err, qt.ErrorMatches, `(?s).*SQL: (?:--[^\n]*\n)*ALTER TABLE "items" ALTER COLUMN "name" SET NOT NULL`,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	c.Assert(spannerColumnNullable(c, ctx, conn, "name"), qt.Equals, "YES")

	planned, plannedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", schema, "--db-url", dbURL, "--dry-run",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned, plannedErr))
	c.Assert(planned, qt.Contains, "-- SPANNER: SET NOT NULL fails if any row of \"items\" holds NULL in \"name\"; "+
		"the column declares no default to fill it with.\n"+
		"ALTER TABLE \"items\" ALTER COLUMN \"name\" SET NOT NULL;\n")
	c.Assert(planned, qt.Not(qt.Contains), "ADD COLUMN")
}

// spannerNotNullDatabase is a scratch database on the Spanner emulator, which
// creates the database the URL names when a client first connects to it,
// holding the base table and the seed rows. The table is dropped when the test
// ends, since the database cannot be.
func spannerNotNullDatabase(c *qt.C, ctx context.Context, seed []string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	dbURL := renameInPath(c, dbtarget.URL(c, dbtarget.Spanner), devDialectScratchName())
	conn := connectDevDialect(c, dbURL)
	_, err := conn.ExecContext(ctx, "CREATE TABLE items (id bigint PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS items")
		c.Check(dropErr, qt.IsNil)
	})
	for _, statement := range seed {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return dbURL, conn
}

func writeSpannerNotNullSchema(c *qt.C, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// spannerColumnNullable reads is_nullable for one column of items.
func spannerColumnNullable(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, column string) string {
	c.Helper()
	var nullable string
	err := conn.QueryRowContext(ctx,
		"SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'items' AND column_name = $1",
		column).Scan(&nullable)
	c.Assert(err, qt.IsNil)
	return nullable
}

// spannerItemRows lists the rows of items as "id name".
func spannerItemRows(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT id::text || ' ' || name FROM items ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		got = append(got, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return got
}
