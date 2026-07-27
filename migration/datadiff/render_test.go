package datadiff_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/migration/datadiff"
)

// TestRender_SplitterRoundTrip proves the renderer and ptah's own dialect-aware
// statement splitter agree: a rendered statement whose value embeds quotes,
// backslashes, and semicolons stays exactly one statement when re-split, so a
// hostile value cannot leak an extra executed statement into a migration.
func TestRender_SplitterRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		value   string
	}{
		{name: "postgres backslash-quote payload", dialect: "postgres", value: `\'; DROP TABLE regions; --`},
		{name: "postgres quote payload", dialect: "postgres", value: `'); DROP TABLE regions; --`},
		{name: "sqlite backslash-quote payload", dialect: "sqlite", value: `\'; DROP TABLE regions; --`},
		{name: "mysql backslash-quote payload", dialect: "mysql", value: `\'; DROP TABLE regions; --`},
	}

	c := qt.New(t)
	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			diff := &datadiff.DataDiff{
				Table: "regions",
				Keys:  []string{"code"},
				Updates: []datadiff.RowUpdate{{
					Key:     map[string]any{"code": "US"},
					Desired: datadiff.Row{"code": "US", "name": tt.value},
					Live:    datadiff.Row{"code": "US", "name": "United States"},
				}},
			}
			up, down, err := datadiff.Render(diff, tt.dialect)
			c.Assert(err, qt.IsNil)
			// Each script is a single UPDATE; re-splitting must not free the
			// embedded "; DROP TABLE ..." into its own statement.
			c.Assert(sqlutil.SplitSQLStatementsForDialect(up, tt.dialect), qt.HasLen, 1)
			c.Assert(sqlutil.SplitSQLStatementsForDialect(down, tt.dialect), qt.HasLen, 1)
		})
	}
}

// TestRenderShapesAndRoundTrip renders a diff carrying every kind of change and
// asserts the exact up script and its exact inverse. down must undo every
// statement in fully reversed order: re-insert the deleted rows, restore the
// updated rows from their Live values, then delete the inserted rows.
func TestRenderShapesAndRoundTrip(t *testing.T) {
	c := qt.New(t)

	diff := &datadiff.DataDiff{
		Table: "regions",
		Keys:  []string{"code"},
		Inserts: []datadiff.Row{
			{"code": "AT", "name": "Austria"},
			{"code": "CZ", "name": "Czechia"},
		},
		Updates: []datadiff.RowUpdate{
			{
				Key:     map[string]any{"code": "US"},
				Desired: datadiff.Row{"code": "US", "name": "USA"},
				Live:    datadiff.Row{"code": "US", "name": "United States"},
			},
		},
		Deletes: []datadiff.Row{
			{"code": "DE", "name": "Germany"},
			{"code": "ZZ", "name": "Zeta"},
		},
	}

	wantUp := `INSERT INTO "regions" ("code", "name") VALUES ('AT', 'Austria');
INSERT INTO "regions" ("code", "name") VALUES ('CZ', 'Czechia');
UPDATE "regions" SET "name" = 'USA' WHERE "code" = 'US';
DELETE FROM "regions" WHERE "code" = 'DE';
DELETE FROM "regions" WHERE "code" = 'ZZ';
`

	wantDown := `INSERT INTO "regions" ("code", "name") VALUES ('ZZ', 'Zeta');
INSERT INTO "regions" ("code", "name") VALUES ('DE', 'Germany');
UPDATE "regions" SET "name" = 'United States' WHERE "code" = 'US';
DELETE FROM "regions" WHERE "code" = 'CZ';
DELETE FROM "regions" WHERE "code" = 'AT';
`

	up, down, err := datadiff.Render(diff, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, wantUp)
	c.Assert(down, qt.Equals, wantDown)
}

// TestRenderCompositeKeys checks that a composite key produces an AND-joined,
// key-sorted WHERE clause in both DELETE and UPDATE, and that the UPDATE SET
// clause omits the key columns.
func TestRenderCompositeKeys(t *testing.T) {
	c := qt.New(t)

	diff := &datadiff.DataDiff{
		Table: "prices",
		Keys:  []string{"tenant", "code"},
		Updates: []datadiff.RowUpdate{
			{
				Key:     map[string]any{"tenant": 1, "code": "A"},
				Desired: datadiff.Row{"tenant": 1, "code": "A", "val": "x"},
				Live:    datadiff.Row{"tenant": 1, "code": "A", "val": "old"},
			},
		},
		Deletes: []datadiff.Row{
			{"tenant": 2, "code": "B", "val": "z"},
		},
	}

	wantUp := `UPDATE "prices" SET "val" = 'x' WHERE "code" = 'A' AND "tenant" = 1;
DELETE FROM "prices" WHERE "code" = 'B' AND "tenant" = 2;
`
	wantDown := `INSERT INTO "prices" ("code", "tenant", "val") VALUES ('B', 2, 'z');
UPDATE "prices" SET "val" = 'old' WHERE "code" = 'A' AND "tenant" = 1;
`

	up, down, err := datadiff.Render(diff, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, wantUp)
	c.Assert(down, qt.Equals, wantDown)
}

// TestRenderEmptyDiff confirms a no-op diff renders to two empty scripts rather
// than an error.
func TestRenderEmptyDiff(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		diff *datadiff.DataDiff
	}{
		{
			name: "no changes with keys",
			diff: &datadiff.DataDiff{Table: "regions", Keys: []string{"code"}},
		},
		{
			name: "no changes and no keys",
			diff: &datadiff.DataDiff{Table: "regions"},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			up, down, err := datadiff.Render(tt.diff, "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(up, qt.Equals, "")
			c.Assert(down, qt.Equals, "")
		})
	}
}

// TestRenderLiterals exercises renderLiteral through a single-row INSERT. Each
// row has a clean string key ("id") plus one value column ("v") under test, so
// the asserted up script pins down the exact literal produced for each value
// kind and dialect.
func TestRenderLiterals(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		value   any
		wantUp  string
	}{
		{
			name:    "string",
			dialect: "postgres",
			value:   "United States",
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 'United States');\n",
		},
		{
			name:    "string with single quote is doubled",
			dialect: "postgres",
			value:   "O'Brien",
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 'O''Brien');\n",
		},
		{
			name:    "nil renders NULL",
			dialect: "postgres",
			value:   nil,
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', NULL);\n",
		},
		{
			name:    "bool true postgres is TRUE",
			dialect: "postgres",
			value:   true,
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', TRUE);\n",
		},
		{
			name:    "bool false postgres is FALSE",
			dialect: "postgres",
			value:   false,
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', FALSE);\n",
		},
		{
			name:    "bool true mysql is 1",
			dialect: "mysql",
			value:   true,
			wantUp:  "INSERT INTO `t` (`id`, `v`) VALUES ('r1', 1);\n",
		},
		{
			name:    "bool false mariadb is 0",
			dialect: "mariadb",
			value:   false,
			wantUp:  "INSERT INTO `t` (`id`, `v`) VALUES ('r1', 0);\n",
		},
		{
			name:    "int",
			dialect: "postgres",
			value:   42,
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 42);\n",
		},
		{
			name:    "negative int64",
			dialect: "postgres",
			value:   int64(-7),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', -7);\n",
		},
		{
			name:    "uint64 max",
			dialect: "postgres",
			value:   uint64(18446744073709551615),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 18446744073709551615);\n",
		},
		{
			name:    "float64",
			dialect: "postgres",
			value:   1.5,
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 1.5);\n",
		},
		{
			name:    "float32 uses shortest 32-bit form",
			dialect: "postgres",
			value:   float32(0.1),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 0.1);\n",
		},
		{
			name:    "byte slice treated as text",
			dialect: "postgres",
			value:   []byte("hi"),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 'hi');\n",
		},
		{
			name:    "backslash not escaped for postgres",
			dialect: "postgres",
			value:   `a\b`,
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', 'a\\b');\n",
		},
		{
			name:    "backslash escaped for mysql",
			dialect: "mysql",
			value:   `a\b`,
			wantUp:  "INSERT INTO `t` (`id`, `v`) VALUES ('r1', 'a\\\\b');\n",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			diff := &datadiff.DataDiff{
				Table:   "t",
				Keys:    []string{"id"},
				Inserts: []datadiff.Row{{"id": "r1", "v": tt.value}},
			}
			up, _, err := datadiff.Render(diff, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(up, qt.Equals, tt.wantUp)
		})
	}
}

// TestRenderStringInjectionIsEscaped is the core security assertion: a value
// crafted to break out of its literal is emitted as one safely-quoted literal.
// Note that a naive "must not contain '); " check is wrong — the byte sequence
// '); legitimately appears when an escaped quote precedes a paren. The real
// breakout marker is an opening paren immediately followed by a quote and paren,
// i.e. ('); which must be absent.
func TestRenderStringInjectionIsEscaped(t *testing.T) {
	c := qt.New(t)

	diff := &datadiff.DataDiff{
		Table:   "users",
		Keys:    []string{"id"},
		Inserts: []datadiff.Row{{"id": "r1", "v": `'); DROP TABLE users;--`}},
	}

	up, _, err := datadiff.Render(diff, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals,
		"INSERT INTO \"users\" (\"id\", \"v\") VALUES ('r1', '''); DROP TABLE users;--');\n")
	c.Assert(up, qt.Contains, "''")
	c.Assert(up, qt.Not(qt.Contains), "('); ")
}

// TestRenderLiteralErrors verifies that values with no safe SQL literal cause
// Render to fail rather than emit anything, and that no partial output leaks.
func TestRenderLiteralErrors(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		value any
	}{
		{name: "nul byte in string", value: "a\x00b"},
		{name: "nan float", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
		{name: "unsupported struct type", value: struct{ X int }{X: 1}},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			diff := &datadiff.DataDiff{
				Table:   "t",
				Keys:    []string{"id"},
				Inserts: []datadiff.Row{{"id": "r1", "v": tt.value}},
			}
			up, down, err := datadiff.Render(diff, "postgres")
			c.Assert(err, qt.IsNotNil)
			c.Assert(up, qt.Equals, "")
			c.Assert(down, qt.Equals, "")
		})
	}
}

// TestRenderErrors covers the structural validation failures: a nil diff, a
// non-empty diff without key columns, rows missing a key column (including an
// insert whose down statement cannot target it), and updates with nothing to
// set.
func TestRenderErrors(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		diff *datadiff.DataDiff
	}{
		{
			name: "nil diff",
			diff: nil,
		},
		{
			name: "non-empty diff without keys",
			diff: &datadiff.DataDiff{
				Table:   "t",
				Inserts: []datadiff.Row{{"code": "US"}},
			},
		},
		{
			name: "insert row with no columns",
			diff: &datadiff.DataDiff{
				Table:   "t",
				Keys:    []string{"code"},
				Inserts: []datadiff.Row{{}},
			},
		},
		{
			name: "insert row missing key column cannot be reversed",
			diff: &datadiff.DataDiff{
				Table:   "t",
				Keys:    []string{"code"},
				Inserts: []datadiff.Row{{"name": "United States"}},
			},
		},
		{
			name: "delete row missing key column",
			diff: &datadiff.DataDiff{
				Table:   "t",
				Keys:    []string{"code"},
				Deletes: []datadiff.Row{{"name": "Germany"}},
			},
		},
		{
			name: "update with empty desired row",
			diff: &datadiff.DataDiff{
				Table: "t",
				Keys:  []string{"code"},
				Updates: []datadiff.RowUpdate{
					{Key: map[string]any{"code": "US"}, Desired: datadiff.Row{}, Live: datadiff.Row{"code": "US", "name": "x"}},
				},
			},
		},
		{
			name: "update with only key columns has nothing to set",
			diff: &datadiff.DataDiff{
				Table: "t",
				Keys:  []string{"code"},
				Updates: []datadiff.RowUpdate{
					{Key: map[string]any{"code": "US"}, Desired: datadiff.Row{"code": "US"}, Live: datadiff.Row{"code": "US", "name": "x"}},
				},
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			up, down, err := datadiff.Render(tt.diff, "postgres")
			c.Assert(err, qt.IsNotNil)
			c.Assert(up, qt.Equals, "")
			c.Assert(down, qt.Equals, "")
		})
	}
}
