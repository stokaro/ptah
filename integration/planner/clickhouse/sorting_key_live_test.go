//go:build integration

package clickhouse_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/schemafile"
)

// A ClickHouse table compared with the declaration that describes it, read
// from a SQL or YAML file the way `ptah schema diff --to` reads one
// (stokaro/ptah#4104).
//
// The server marks every column its primary key uses (is_in_primary_key), and
// a declaration states the key as an engine clause. Compared column by column,
// a table identical to its declaration planned `MODIFY COLUMN id Int32` on every
// run. A key that really changes cannot be applied by any ALTER, and is refused.

// createTable runs the statements that build the live table.
func createTable(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// planFromFile plans conn against the declaration in a file named name holding
// body.
func planFromFile(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, name, body string) (atlasschema.ApplyPlan, error) {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	desired, err := schemafile.LoadPath(path, schemafile.Options{Dialect: platform.ClickHouse})
	c.Assert(err, qt.IsNil)
	return atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired})
}

// A table identical to its declaration plans nothing, whichever clause states
// the key and whichever format the declaration is in.
func TestSortingKeyIdenticalToTheDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		file   string
		source string
	}{
		{
			name:   "ORDER BY one column, from SQL",
			table:  "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY id",
			file:   "schema.sql",
			source: "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY id;\n",
		},
		{
			name:  "ORDER BY one column, from YAML",
			table: "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY id",
			file:  "schema.yaml",
			source: "tables:\n  asn:\n    columns:\n      id: {type: Int32, nullable: false}\n      n: {type: Int32, nullable: false}\n" +
				"    overrides:\n      clickhouse:\n        engine: MergeTree\n        order_by: id\n",
		},
		{
			name:   "PRIMARY KEY narrower than ORDER BY",
			table:  "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree PRIMARY KEY id ORDER BY (id, n)",
			file:   "schema.sql",
			source: "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree PRIMARY KEY id ORDER BY (id, n);\n",
		},
		{
			name:   "a key over an expression",
			table:  "CREATE TABLE asn (ts DateTime, id Int32, n Int32) ENGINE = MergeTree ORDER BY (toDate(ts), id)",
			file:   "schema.sql",
			source: "CREATE TABLE asn (ts DateTime, id Int32, n Int32) ENGINE = MergeTree ORDER BY (toDate(ts), id);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			createTable(c, ctx, conn, test.table)

			plan, err := planFromFile(c, ctx, conn, test.file, test.source)

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Statements(), qt.HasLen, 0)
		})
	}
}

// A declaration whose key moves a column into or out of the table's primary
// key is refused before anything runs: ClickHouse has no ALTER that changes a
// MergeTree table's primary key, so a MODIFY COLUMN would apply, change
// nothing, and be planned again. The table keeps its key.
func TestSortingKeyChange_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		source string
		column string
	}{
		{name: "a column joins the key", source: "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY (id, n);\n", column: "n"},
		{name: "the key moves to another column", source: "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY n;\n", column: "(id|n)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			createTable(c, ctx, conn, "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY id")

			plan, err := planFromFile(c, ctx, conn, "schema.sql", test.source)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*the primary key of asn changes \(column `+test.column+`: .*`)
			c.Assert(plan.Statements(), qt.HasLen, 0)
			var sortingKey string
			c.Assert(conn.QueryRowContext(ctx,
				"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'asn'",
			).Scan(&sortingKey), qt.IsNil)
			c.Assert(sortingKey, qt.Equals, "id")
		})
	}
}
