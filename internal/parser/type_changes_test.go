package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/parser"
)

// Type reads keep the exact modifiers the server applies, without returning a
// schema in which MODIFY could clear properties the statement leaves alone.
func TestReadColumnTypeChanges_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
		want    []parser.ColumnTypeChange
	}{
		{
			name: "PostgreSQL type and using", dialect: "postgres",
			sql:  `ALTER TABLE "sales"."prices" ALTER COLUMN "amount" TYPE numeric(8,1) USING "amount"::numeric(8,1), ALTER COLUMN "amount" DROP DEFAULT;`,
			want: []parser.ColumnTypeChange{{Table: `"sales"."prices"`, Column: `"amount"`, Type: "numeric(8,1)"}},
		},
		{
			name: "SQL Server whole definition", dialect: "sqlserver",
			sql:  "ALTER TABLE [prices] ALTER COLUMN [amount] decimal(8,1) NULL;",
			want: []parser.ColumnTypeChange{{Table: "[prices]", Column: "[amount]", Type: "decimal(8,1)"}},
		},
		{
			name: "MySQL signedness", dialect: "mysql",
			sql:  "ALTER TABLE `prices` MODIFY COLUMN `amount` INT UNSIGNED NOT NULL DEFAULT 0;",
			want: []parser.ColumnTypeChange{{Table: "`prices`", Column: "`amount`", Type: "INT UNSIGNED"}},
		},
		{
			name: "MariaDB fractional seconds", dialect: "mariadb",
			sql:  "ALTER TABLE prices MODIFY COLUMN at timestamp(3);",
			want: []parser.ColumnTypeChange{{Table: "prices", Column: "at", Type: "timestamp(3)"}},
		},
		{
			name: "ClickHouse time zone argument", dialect: "clickhouse",
			sql:  "ALTER TABLE `prices` MODIFY COLUMN `at` DateTime64(3, 'Asia/Kolkata');",
			want: []parser.ColumnTypeChange{{Table: "`prices`", Column: "`at`", Type: "DateTime64(3, 'Asia/Kolkata')"}},
		},
		{
			name: "time zone qualifier", dialect: "postgres",
			sql:  "ALTER TABLE prices ALTER COLUMN at TYPE timestamp(3) with time zone;",
			want: []parser.ColumnTypeChange{{Table: "prices", Column: "at", Type: "timestamp(3) with time zone"}},
		},
		{
			name: "multiple conversions in source order", dialect: "postgres",
			sql:  "ALTER TABLE prices ALTER COLUMN a TYPE numeric(8,1), ALTER COLUMN b TYPE integer; ALTER TABLE other ALTER COLUMN a TYPE numeric(8,2);",
			want: []parser.ColumnTypeChange{{Table: "prices", Column: "a", Type: "numeric(8,1)"}, {Table: "prices", Column: "b", Type: "integer"}, {Table: "other", Column: "a", Type: "numeric(8,2)"}},
		},
		{
			name: "a type mentioned outside a conversion", dialect: "postgres",
			sql: "CREATE TABLE prices (amount numeric(8,1)); ALTER TABLE prices ALTER COLUMN amount SET DEFAULT CAST(1 AS numeric(8,1));",
		},
		{name: "empty input", dialect: "postgres"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parser.ReadColumnTypeChanges(row.sql, parser.WithDialect(row.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}

func TestReadColumnTypeChanges_FailurePath(t *testing.T) {
	rows := []struct {
		name string
		sql  string
		want string
	}{
		{"missing type", "ALTER TABLE prices ALTER COLUMN amount TYPE;", ".*expected column type.*"},
		{"a dialect without MODIFY", "ALTER TABLE prices MODIFY COLUMN amount numeric(8,1);", ".*postgres has no MODIFY clause.*"},
		{"unsupported operation after a conversion", "ALTER TABLE prices ALTER COLUMN amount TYPE numeric(8,1), ALTER COLUMN amount SET STATISTICS 100;", ".*unsupported ALTER COLUMN.*"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parser.ReadColumnTypeChanges(row.sql, parser.WithDialect("postgres"))

			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// The type-only read must not make ClickHouse MODIFY available to schema
// parsing, where omitted properties would be lost as a full replacement.
func TestReadColumnTypeChanges_LeavesSchemaParsingStrict(t *testing.T) {
	c := qt.New(t)
	const sql = "ALTER TABLE prices MODIFY COLUMN amount Decimal(8,1);"

	changes, readErr := parser.ReadColumnTypeChanges(sql, parser.WithDialect("clickhouse"))
	schema, parseErr := parser.NewParser(sql, parser.WithDialect("clickhouse")).Parse()

	c.Assert(readErr, qt.IsNil)
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(parseErr, qt.ErrorMatches, ".*clickhouse's MODIFY is not MySQL's.*")
	c.Assert(schema, qt.IsNil)
}
