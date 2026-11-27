package roundingscan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/roundingscan"
)

func TestCheckStatements_HappyPath(t *testing.T) {
	rows := []struct {
		name      string
		dialect   string
		statement string
		change    roundingscan.Change
	}{
		{"SQL Server precision and scale", "sqlserver", "ALTER TABLE [prices] ALTER COLUMN [amount] DECIMAL(8,1) NULL;",
			roundingscan.Change{Table: "prices", Column: "amount", DeclaredType: "decimal (8, 1)"}},
		{"PostgreSQL quoted names and qualifier", "postgres", `ALTER TABLE "sales"."pr""ices" ALTER COLUMN "am""ount" TYPE NUMERIC(8,1);`,
			roundingscan.Change{Table: `sales."pr""ices"`, Column: `am"ount`, DeclaredType: "numeric(8,1)"}},
		{"MySQL quoted names", "mysql", "ALTER TABLE `pr``ices` MODIFY COLUMN `am``ount` decimal(8,1);",
			roundingscan.Change{Table: "`pr``ices`", Column: "am`ount", DeclaredType: "decimal(8,1)"}},
		{"MariaDB qualified record", "mariadb", "ALTER TABLE `prices` MODIFY COLUMN `amount` decimal(8,1);",
			roundingscan.Change{Table: "sales.prices", Column: "amount", DeclaredType: "decimal(8,1)"}},
		{"ClickHouse escaped names", "clickhouse", "ALTER TABLE `pr\\\\ices` MODIFY COLUMN `am\\\\ount` Decimal(8,1);",
			roundingscan.Change{Table: `pr\ices`, Column: `am\ount`, DeclaredType: "Decimal(8,1)"}},
		{"time zone argument preserves case", "clickhouse", "ALTER TABLE prices MODIFY COLUMN amount DateTime64(3, 'Asia/Kolkata');",
			roundingscan.Change{Table: "prices", Column: "amount", DeclaredType: "DateTime64(3,'Asia/Kolkata')"}},
		{"time zone qualifier", "postgres", "ALTER TABLE prices ALTER COLUMN amount TYPE timestamp(3) with time zone;",
			roundingscan.Change{Table: "prices", Column: "amount", DeclaredType: "TIMESTAMP (3) WITH TIME ZONE"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			err := roundingscan.CheckStatements(row.dialect, []string{row.statement}, row.change)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestCheckStatements_FailurePath(t *testing.T) {
	rows := []struct {
		name       string
		dialect    string
		statements []string
		declared   string
		want       string
	}{
		{"scale mismatch", "postgres", []string{"ALTER TABLE sales.prices ALTER COLUMN amount TYPE numeric(8,1);"}, "numeric(8,2)", ".*declared type .* differs from the statement's type .*"},
		{"precision mismatch", "postgres", []string{"ALTER TABLE sales.prices ALTER COLUMN amount TYPE numeric(6,1);"}, "numeric(8,1)", ".*declared type .* differs from the statement's type .*"},
		{"signedness mismatch", "mysql", []string{"ALTER TABLE sales.prices MODIFY COLUMN amount INT;"}, "INT UNSIGNED", ".*declared type .* differs from the statement's type .*"},
		{"time zone mismatch", "postgres", []string{"ALTER TABLE sales.prices ALTER COLUMN amount TYPE timestamp(3) without time zone;"}, "timestamp(3) with time zone", ".*declared type .* differs from the statement's type .*"},
		{"literal case mismatch", "clickhouse", []string{"ALTER TABLE sales.prices MODIFY COLUMN amount DateTime64(3, 'UTC');"}, "DateTime64(3, 'utc')", ".*declared type .* differs from the statement's type .*"},
		{"a matching conversion before a different one", "postgres", []string{"ALTER TABLE sales.prices ALTER COLUMN amount TYPE numeric(8,2);", "ALTER TABLE sales.prices ALTER COLUMN amount TYPE numeric(8,1);"}, "numeric(8,2)", "statements apply different types .*"},
		{"another schema", "postgres", []string{"ALTER TABLE other.prices ALTER COLUMN amount TYPE numeric(8,1);"}, "numeric(8,1)", "no statement of the plan changes the type.*"},
		{"another quoted column", "postgres", []string{`ALTER TABLE sales.prices ALTER COLUMN "Amount" TYPE numeric(8,1);`}, "numeric(8,1)", "no statement of the plan changes the type.*"},
		{"a column name in a using expression", "postgres", []string{"ALTER TABLE sales.prices ALTER COLUMN other TYPE numeric(8,1) USING amount;"}, "numeric(8,1)", "no statement of the plan changes the type.*"},
		{"only a default changes", "postgres", []string{"ALTER TABLE sales.prices ALTER COLUMN amount SET DEFAULT CAST(1 AS numeric(8,1));"}, "numeric(8,1)", "no statement of the plan changes the type.*"},
		{"unsupported candidate syntax", "postgres", []string{"ALTER TABLE sales.prices MODIFY COLUMN amount numeric(8,1);"}, "numeric(8,1)", "cannot read the type applied.*"},
		{"no statements", "postgres", nil, "numeric(8,1)", "no statement of the plan changes the type.*"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			change := roundingscan.Change{Table: "sales.prices", Column: "amount", DeclaredType: row.declared}

			err := roundingscan.CheckStatements(row.dialect, row.statements, change)

			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}
