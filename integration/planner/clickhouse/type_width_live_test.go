//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// A ClickHouse column whose type changes width, compared with a SQL file the
// way `ptah schema diff --to` reads one. The comparison folded every integer
// to one family, so a live Int32 declared Int64 recorded no change and nothing
// was planned (stokaro/ptah#4105). Only the server's system.columns can say
// what type the column has afterwards, and only a second plan can say that the
// comparison agrees with it.

// widColumnType reads d's type from system.columns.
func widColumnType(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var columnType string
	c.Assert(conn.QueryRowContext(ctx,
		"SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'wid' AND name = 'd'",
	).Scan(&columnType), qt.IsNil)
	return columnType
}

// widValues reads d from every row in id order, NULL spelled out.
func widValues(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT ifNull(toString(d), 'NULL') FROM wid ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// A width change is planned as MODIFY COLUMN, applied, read back as the
// declared type with every value kept, and not planned again. The narrowing row
// holds values the narrower type can hold; one it cannot is stored wrapped
// (30000000000 as -64771072), which is why the safety report calls a narrowing
// destructive.
func TestTypeWidthChange_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		live       string
		rows       string
		declared   string
		want       string
		wantType   string
		wantValues []string
	}{
		{name: "Int32 to Int64", live: "Int32", rows: "(1, 5), (2, -7)", declared: "Int64",
			want: "ALTER TABLE wid MODIFY COLUMN d Int64", wantType: "Int64", wantValues: []string{"5", "-7"}},
		{name: "Int8 to Int16", live: "Int8", rows: "(1, -100)", declared: "Int16",
			want: "ALTER TABLE wid MODIFY COLUMN d Int16", wantType: "Int16", wantValues: []string{"-100"}},
		{name: "UInt8 to UInt16", live: "UInt8", rows: "(1, 200)", declared: "UInt16",
			want: "ALTER TABLE wid MODIFY COLUMN d UInt16", wantType: "UInt16", wantValues: []string{"200"}},
		{name: "Float32 to Float64", live: "Float32", rows: "(1, 1.5)", declared: "Float64",
			want: "ALTER TABLE wid MODIFY COLUMN d Float64", wantType: "Float64", wantValues: []string{"1.5"}},
		{name: "decimal precision", live: "Decimal(9, 2)", rows: "(1, 12.34)", declared: "Decimal(18, 2)",
			want: "ALTER TABLE wid MODIFY COLUMN d Decimal(18, 2)", wantType: "Decimal(18, 2)", wantValues: []string{"12.34"}},
		{name: "DateTime to DateTime64", live: "DateTime", rows: "(1, '2020-01-01 00:00:00')", declared: "DateTime64(3)",
			want: "ALTER TABLE wid MODIFY COLUMN d DateTime64(3)", wantType: "DateTime64(3)",
			wantValues: []string{"2020-01-01 00:00:00.000"}},
		{name: "nullable width", live: "Nullable(Int32)", rows: "(1, NULL), (2, 5)", declared: "Nullable(Int64)",
			want: "ALTER TABLE wid MODIFY COLUMN d Nullable(Int64)", wantType: "Nullable(Int64)", wantValues: []string{"NULL", "5"}},
		{name: "low cardinality removed", live: "LowCardinality(String)", rows: "(1, 'a')", declared: "String",
			want: "ALTER TABLE wid MODIFY COLUMN d String", wantType: "String", wantValues: []string{"a"}},
		{name: "Int64 to Int32 in range", live: "Int64", rows: "(1, 5), (2, -7)", declared: "Int32",
			want: "ALTER TABLE wid MODIFY COLUMN d Int32", wantType: "Int32", wantValues: []string{"5", "-7"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			createTable(c, ctx, conn,
				"CREATE TABLE wid (id Int32, d "+test.live+") ENGINE = MergeTree ORDER BY id",
				"INSERT INTO wid VALUES "+test.rows,
			)
			source := "CREATE TABLE wid (id Int32, d " + test.declared + ") ENGINE = MergeTree ORDER BY id;\n"

			plan, err := planFromFile(c, ctx, conn, "schema.sql", source)
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Statements(), qt.DeepEquals, []string{test.want})
			applyPlan(c, ctx, conn, plan.Statements())

			c.Assert(widColumnType(c, ctx, conn), qt.Equals, test.wantType)
			c.Assert(widValues(c, ctx, conn), qt.DeepEquals, test.wantValues)
			again, err := planFromFile(c, ctx, conn, "schema.sql", source)
			c.Assert(err, qt.IsNil)
			c.Assert(again.Statements(), qt.HasLen, 0)
		})
	}
}

// A table created from a declaration and compared with that declaration plans
// nothing, whichever spelling the declaration uses. The server stores most of
// these under another name -- Decimal32(2) as Decimal(9, 2), Enum('a', 'b') as
// Enum8('a' = 1, 'b' = 2), SMALLINT UNSIGNED as UInt16 -- and the type it stores
// is read back too, so a renderer and a comparison that agree on a wrong type
// do not pass: ClickHouse's own Int8 and DateTime were written as Int64 and
// DateTime64(3). A nullable low-cardinality or array type is not wrapped in a
// second Nullable, which the server refuses.
func TestTypeSpelledAnotherWay_PlansNothing(t *testing.T) {
	tests := []struct {
		declared string
		wantType string
	}{
		{declared: "Int8", wantType: "Int8"},
		{declared: "BIGINT", wantType: "Int64"},
		{declared: "SMALLINT UNSIGNED", wantType: "UInt16"},
		{declared: "Decimal32(2)", wantType: "Decimal(9, 2)"},
		{declared: "Nullable(Decimal32(2))", wantType: "Nullable(Decimal(9, 2))"},
		{declared: "Boolean", wantType: "Bool"},
		{declared: "DOUBLE", wantType: "Float64"},
		{declared: "TEXT", wantType: "String"},
		{declared: "DateTime", wantType: "DateTime"},
		{declared: "DateTime('UTC')", wantType: "DateTime('UTC')"},
		{declared: "DateTime64", wantType: "DateTime64(3)"},
		{declared: "Enum('a', 'b')", wantType: "Enum8('a' = 1, 'b' = 2)"},
		{declared: "Map(String,UInt64)", wantType: "Map(String, UInt64)"},
		{declared: "Tuple(a INT, b String)", wantType: "Tuple(a Int32, b String)"},
		{declared: "LowCardinality(Nullable(String))", wantType: "LowCardinality(Nullable(String))"},
		{declared: "Array(Nullable(Int32))", wantType: "Array(Nullable(Int32))"},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			source := "CREATE TABLE wid (id Int32, d " + test.declared + ") ENGINE = MergeTree ORDER BY id;\n"
			created, err := planFromFile(c, ctx, conn, "schema.sql", source)
			c.Assert(err, qt.IsNil)
			applyPlan(c, ctx, conn, created.Statements())
			c.Assert(widColumnType(c, ctx, conn), qt.Equals, test.wantType)

			plan, err := planFromFile(c, ctx, conn, "schema.sql", source)

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Statements(), qt.HasLen, 0)
		})
	}
}
