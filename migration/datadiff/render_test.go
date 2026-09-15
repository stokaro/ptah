package datadiff_test

import (
	"math"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/migration/datadiff"
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
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

// TestRender_SchemaQualifiedTableName proves diff.Schema qualifies the rendered
// table name per dialect, and that an empty schema stays a bare, unqualified
// identifier (backward compatible).
func TestRender_SchemaQualifiedTableName(t *testing.T) {
	c := qt.New(t)

	qualified := &datadiff.DataDiff{
		Schema:  "app",
		Table:   "regions",
		Keys:    []string{"code"},
		Inserts: []datadiff.Row{{"code": "US", "name": "United States"}},
		Deletes: []datadiff.Row{{"code": "XX", "name": "Old"}},
	}

	// PostgreSQL: double-quoted schema.table in both directions.
	up, down, err := datadiff.Render(qualified, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `INSERT INTO "app"."regions" `)
	c.Assert(up, qt.Contains, `DELETE FROM "app"."regions" `)
	c.Assert(down, qt.Contains, `INSERT INTO "app"."regions" `)

	// MySQL: backtick-quoted schema.table.
	upMy, _, err := datadiff.Render(qualified, "mysql")
	c.Assert(err, qt.IsNil)
	c.Assert(upMy, qt.Contains, "INSERT INTO `app`.`regions` ")

	// Empty schema renders a bare table name, unchanged from the pre-schema
	// behavior.
	unqualified := &datadiff.DataDiff{
		Table:   "regions",
		Keys:    []string{"code"},
		Inserts: []datadiff.Row{{"code": "US", "name": "United States"}},
	}
	upBare, _, err := datadiff.Render(unqualified, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(upBare, qt.Contains, `INSERT INTO "regions" `)
	c.Assert(upBare, qt.Not(qt.Contains), `"."`)

	// A schema containing the quote character is escaped by the identifier
	// quoter (doubled), so it cannot break out of the identifier.
	weird := &datadiff.DataDiff{
		Schema:  `a"b`,
		Table:   "regions",
		Keys:    []string{"code"},
		Inserts: []datadiff.Row{{"code": "US", "name": "United States"}},
	}
	upWeird, _, err := datadiff.Render(weird, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(upWeird, qt.Contains, `INSERT INTO "a""b"."regions" `)
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
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
		{
			// Drivers scan timestamp columns as time.Time; postgres timestamp
			// literals carry the numeric UTC offset so timestamptz round-trips.
			name:    "time postgres includes offset",
			dialect: "postgres",
			value:   time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', '2024-03-05 06:07:08+00:00');\n",
		},
		{
			name:    "time postgres keeps fractional seconds",
			dialect: "postgres",
			value:   time.Date(2024, 3, 5, 6, 7, 8, 123456789, time.UTC),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', '2024-03-05 06:07:08.123456789+00:00');\n",
		},
		{
			name:    "time postgres renders non-utc offset",
			dialect: "postgres",
			value:   time.Date(2024, 3, 5, 6, 7, 8, 0, time.FixedZone("", 2*3600)),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', '2024-03-05 06:07:08+02:00');\n",
		},
		{
			// MySQL/MariaDB DATETIME literals do not accept a trailing offset, so
			// the wall-clock form is emitted instead.
			name:    "time mysql omits offset",
			dialect: "mysql",
			value:   time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC),
			wantUp:  "INSERT INTO `t` (`id`, `v`) VALUES ('r1', '2024-03-05 06:07:08');\n",
		},
		{
			name:    "time sqlite includes offset",
			dialect: "sqlite",
			value:   time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC),
			wantUp:  "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', '2024-03-05 06:07:08+00:00');\n",
		},
		{
			// Oracle creates a BOOLEAN column as NUMBER(1), and the literal
			// follows the type the column became.
			name:    "bool true oracle is 1",
			dialect: "oracle",
			value:   true,
			wantUp:  "INSERT INTO t (id, v) VALUES ('r1', 1);\n",
		},
		{
			name:    "bool false oracle is 0",
			dialect: "oracle",
			value:   false,
			wantUp:  "INSERT INTO t (id, v) VALUES ('r1', 0);\n",
		},
		{
			// Oracle reads a bare string through NLS_TIMESTAMP_FORMAT and
			// refuses it with ORA-01843; the typed literal is accepted.
			name:    "time oracle is a typed timestamp literal",
			dialect: "oracle",
			value:   time.Date(2024, 3, 5, 6, 7, 8, 123456789, time.FixedZone("", 2*3600)),
			wantUp:  "INSERT INTO t (id, v) VALUES ('r1', TIMESTAMP '2024-03-05 06:07:08.123456789+02:00');\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			up, down, err := datadiff.Render(tt.diff, "postgres")
			c.Assert(err, qt.IsNotNil)
			c.Assert(up, qt.Equals, "")
			c.Assert(down, qt.Equals, "")
		})
	}
}

// TestRenderNullKeyPredicate pins the predicate a key column holding NULL gets.
// Written as `= NULL` it is UNKNOWN for every row, so the statement succeeds
// and matches nothing.
func TestRenderNullKeyPredicate(t *testing.T) {
	c := qt.New(t)

	diff := &datadiff.DataDiff{
		Table: "scoped",
		Keys:  []string{"code", "tenant"},
		Updates: []datadiff.RowUpdate{
			{
				Key:     map[string]any{"code": "one", "tenant": nil},
				Desired: datadiff.Row{"code": "one", "tenant": nil, "label": "new"},
				Live:    datadiff.Row{"code": "one", "tenant": nil, "label": "old"},
			},
		},
		Deletes: []datadiff.Row{
			{"code": "two", "tenant": nil, "label": "gone"},
		},
	}

	wantUp := `UPDATE "scoped" SET "label" = 'new' WHERE "code" = 'one' AND "tenant" IS NULL;
DELETE FROM "scoped" WHERE "code" = 'two' AND "tenant" IS NULL;
`
	wantDown := `INSERT INTO "scoped" ("code", "label", "tenant") VALUES ('two', 'gone', NULL);
UPDATE "scoped" SET "label" = 'old' WHERE "code" = 'one' AND "tenant" IS NULL;
`

	up, down, err := datadiff.Render(diff, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, wantUp)
	c.Assert(down, qt.Equals, wantDown)
}

// TestRenderClickHouseUpdateIsAlterTable pins the statement a row change takes
// on ClickHouse, which answers a plain UPDATE with "Lightweight updates are not
// supported" unless the table carries a materialized _block_number column. The
// INSERT and the DELETE keep their ordinary spelling, which the server accepts.
func TestRenderClickHouseUpdateIsAlterTable(t *testing.T) {
	c := qt.New(t)

	diff := &datadiff.DataDiff{
		Table:   "settings",
		Keys:    []string{"code"},
		Inserts: []datadiff.Row{{"code": "two", "label": "second"}},
		Updates: []datadiff.RowUpdate{
			{
				Key:     map[string]any{"code": "one"},
				Desired: datadiff.Row{"code": "one", "label": "new"},
				Live:    datadiff.Row{"code": "one", "label": "old"},
			},
		},
		Deletes: []datadiff.Row{{"code": "three", "label": "gone"}},
	}

	wantUp := "INSERT INTO `settings` (`code`, `label`) VALUES ('two', 'second');\n" +
		"ALTER TABLE `settings` UPDATE `label` = 'new' WHERE `code` = 'one';\n" +
		"DELETE FROM `settings` WHERE `code` = 'three';\n"
	wantDown := "INSERT INTO `settings` (`code`, `label`) VALUES ('three', 'gone');\n" +
		"ALTER TABLE `settings` UPDATE `label` = 'old' WHERE `code` = 'one';\n" +
		"DELETE FROM `settings` WHERE `code` = 'two';\n"

	up, down, err := datadiff.Render(diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, wantUp)
	c.Assert(down, qt.Equals, wantDown)
}

// TestRenderStatements_ANewlineStaysInsideItsStatement pins the boundary a
// caller reads statements by: one element per row, whole, however many lines
// its literal spans. The script form joins statements with the same byte a
// value may carry, so it cannot be cut at line breaks (stokaro/ptah#3278).
func TestRenderStatements_ANewlineStaysInsideItsStatement(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{
		Table:   "notices",
		Keys:    []string{"code"},
		Inserts: []datadiff.Row{{"code": "a", "body": "first line\nsecond line"}},
		Updates: []datadiff.RowUpdate{{
			Key:     map[string]any{"code": "b"},
			Desired: datadiff.Row{"code": "b", "body": "one\ntwo"},
			Live:    datadiff.Row{"code": "b", "body": "plain"},
		}},
		Deletes: []datadiff.Row{{"code": "c\nd", "body": "gone"}},
	}

	up, down, err := datadiff.RenderStatements(diff, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.DeepEquals, []string{
		"INSERT INTO \"notices\" (\"body\", \"code\") VALUES ('first line\nsecond line', 'a');",
		"UPDATE \"notices\" SET \"body\" = 'one\ntwo' WHERE \"code\" = 'b';",
		"DELETE FROM \"notices\" WHERE \"code\" = 'c\nd';",
	})
	c.Assert(down, qt.DeepEquals, []string{
		"INSERT INTO \"notices\" (\"body\", \"code\") VALUES ('gone', 'c\nd');",
		"UPDATE \"notices\" SET \"body\" = 'plain' WHERE \"code\" = 'b';",
		"DELETE FROM \"notices\" WHERE \"code\" = 'a';",
	})
}

// TestRenderStatements_RenderIsTheirJoin keeps the two forms one rendering: the
// scripts Render returns are the statements joined with a newline and ended
// with one.
func TestRenderStatements_RenderIsTheirJoin(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{
		Table:   "regions",
		Keys:    []string{"code"},
		Inserts: []datadiff.Row{{"code": "AT", "name": "Aus\ntria"}},
		Deletes: []datadiff.Row{{"code": "ZZ", "name": "Zeta"}},
	}

	upStatements, downStatements, err := datadiff.RenderStatements(diff, "mysql")
	c.Assert(err, qt.IsNil)
	up, down, err := datadiff.Render(diff, "mysql")
	c.Assert(err, qt.IsNil)

	c.Assert(up, qt.Equals, strings.Join(upStatements, "\n")+"\n")
	c.Assert(down, qt.Equals, strings.Join(downStatements, "\n")+"\n")
}

// TestRenderStatements_EmptyDiff is the no-op: a diff with nothing to change
// renders no statement in either direction.
func TestRenderStatements_EmptyDiff(t *testing.T) {
	c := qt.New(t)

	up, down, err := datadiff.RenderStatements(&datadiff.DataDiff{Table: "regions"}, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.IsNil)
	c.Assert(down, qt.IsNil)
}

func TestRenderStatements_FailurePath(t *testing.T) {
	t.Run("nil diff", func(t *testing.T) {
		c := qt.New(t)
		up, down, err := datadiff.RenderStatements(nil, "postgres")
		c.Assert(err, qt.ErrorMatches, `datadiff: nil diff`)
		c.Assert(up, qt.IsNil)
		c.Assert(down, qt.IsNil)
	})
	t.Run("non-empty diff without keys", func(t *testing.T) {
		c := qt.New(t)
		up, down, err := datadiff.RenderStatements(&datadiff.DataDiff{
			Table:   "t",
			Inserts: []datadiff.Row{{"code": "US"}},
		}, "postgres")
		c.Assert(err, qt.ErrorMatches, `datadiff: keys must be non-empty to render a non-empty diff`)
		c.Assert(up, qt.IsNil)
		c.Assert(down, qt.IsNil)
	})
	t.Run("a value with no literal", func(t *testing.T) {
		c := qt.New(t)
		up, down, err := datadiff.RenderStatements(&datadiff.DataDiff{
			Table:   "t",
			Keys:    []string{"code"},
			Inserts: []datadiff.Row{{"code": "US", "name": "a\x00b"}},
		}, "postgres")
		c.Assert(err, qt.ErrorMatches, `datadiff: column "name": datadiff: string value contains a NUL byte.*`)
		c.Assert(up, qt.IsNil)
		c.Assert(down, qt.IsNil)
	})
}
