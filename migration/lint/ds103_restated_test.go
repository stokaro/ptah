package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
)

// sqlServerColumn is one baseline column of orders, as SQL Server's read spells it.
func sqlServerColumn(name, columnType string) lintext.BaselineColumn {
	return costColumn(name, columnType, "", "")
}

// TestColumnTypeChangedRule_ReadsEveryEnginesTypeChange holds the rule to each engine's
// spelling of a type change, across the type families a plan rates. SQL Server's clause
// names the type with no word before it, and Ptah's own plans quote the name.
func TestColumnTypeChangedRule_ReadsEveryEnginesTypeChange(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
	}{
		{"SQL Server a temporal precision", "sqlserver", "ALTER TABLE orders ALTER COLUMN total datetime2(0) NULL;"},
		{"SQL Server a name in brackets", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [total] datetime2(0) NULL;"},
		{"SQL Server a qualified table", "sqlserver", "ALTER TABLE [dbo].[orders] ALTER COLUMN [total] datetime2(0) NULL;"},
		{"SQL Server a name with a space", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [the total] int NULL;"},
		{"SQL Server a length", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [note] NVARCHAR(50) NULL;"},
		{"SQL Server MAX", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [note] nvarchar(max) NULL;"},
		{"SQL Server bytes", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [note] varbinary(50) NULL;"},
		{"SQL Server a decimal", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [total] decimal(10,2) NULL;"},
		{"SQL Server a float", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [total] real NULL;"},
		{"SQL Server an integer", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [total] smallint NOT NULL;"},
		{"SQL Server no nullability", "sqlserver", "ALTER TABLE orders ALTER COLUMN total money;"},

		{"PostgreSQL a float", "postgres", `ALTER TABLE "orders" ALTER COLUMN "total" TYPE real;`},
		{"PostgreSQL a decimal", "postgres", `ALTER TABLE "orders" ALTER COLUMN "total" TYPE numeric(10,2);`},
		{"PostgreSQL an array", "postgres", `ALTER TABLE "orders" ALTER COLUMN "total" TYPE numeric(10,2)[];`},
		{"PostgreSQL SET DATA TYPE", "postgres", `ALTER TABLE "orders" ALTER COLUMN "total" SET DATA TYPE bit(4);`},
		{"CockroachDB a STRING", "cockroachdb", `ALTER TABLE "orders" ALTER COLUMN "note" TYPE string(5);`},
		{"YugabyteDB a float", "yugabytedb", `ALTER TABLE "orders" ALTER COLUMN "total" TYPE real;`},
		{"Spanner a length", "spanner", `ALTER TABLE "orders" ALTER COLUMN "note" TYPE varchar(5);`},
		{"MySQL a text tier", "mysql", "ALTER TABLE `orders` MODIFY COLUMN `body` text;"},
		{"MySQL a blob tier", "mysql", "ALTER TABLE `orders` MODIFY COLUMN `body` tinyblob;"},
		{"MySQL a float", "mysql", "ALTER TABLE `orders` MODIFY COLUMN `total` float;"},
		{"MySQL a bare decimal", "mysql", "ALTER TABLE `orders` MODIFY COLUMN `total` decimal;"},
		{"MariaDB a CHANGE", "mariadb", "ALTER TABLE `orders` CHANGE COLUMN `total` `amount` float;"},
		{"ClickHouse a float", "clickhouse", "ALTER TABLE orders MODIFY COLUMN total Float32;"},
		{"ClickHouse a fixed length", "clickhouse", "ALTER TABLE orders MODIFY COLUMN note FixedString(2);"},
		{"ClickHouse a decimal", "clickhouse", "ALTER TABLE orders MODIFY COLUMN total Decimal32(2);"},
		{"no dialect named", "", "ALTER TABLE orders ALTER COLUMN total nvarchar(50) NULL;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTypeChange(c, test.dialect, test.alter)
			c.Assert(ds103Codes(rulesOf(analysis.Findings())), qt.HasLen, 1)
		})
	}
}

// TestColumnTypeChangedRule_LeavesOtherColumnClausesAlone holds the other direction: each
// of these begins as SQL Server's clause does, and none names a type.
func TestColumnTypeChangedRule_LeavesOtherColumnClausesAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
	}{
		{"SQL Server adds a mask", "sqlserver", "ALTER TABLE orders ALTER COLUMN total ADD MASKED WITH (FUNCTION = 'default()');"},
		{"SQL Server drops a mask", "sqlserver", "ALTER TABLE orders ALTER COLUMN total DROP MASKED;"},
		{"SQL Server adds SPARSE", "sqlserver", "ALTER TABLE orders ALTER COLUMN total ADD SPARSE;"},
		{"a clause that names no type", "sqlserver", "ALTER TABLE orders ALTER COLUMN total;"},
		{"PostgreSQL SET NOT NULL", "postgres", "ALTER TABLE orders ALTER COLUMN total SET NOT NULL;"},
		{"PostgreSQL DROP NOT NULL", "postgres", "ALTER TABLE orders ALTER COLUMN total DROP NOT NULL;"},
		{"PostgreSQL SET DEFAULT", "postgres", "ALTER TABLE orders ALTER COLUMN total SET DEFAULT 1;"},
		{"PostgreSQL DROP DEFAULT", "postgres", "ALTER TABLE orders ALTER COLUMN total DROP DEFAULT;"},
		{"PostgreSQL an identity", "postgres", "ALTER TABLE orders ALTER COLUMN total ADD GENERATED ALWAYS AS IDENTITY;"},
		{"PostgreSQL RESTART", "postgres", "ALTER TABLE orders ALTER COLUMN total RESTART WITH 10;"},
		{"PostgreSQL RESET", "postgres", "ALTER TABLE orders ALTER COLUMN total RESET (n_distinct);"},
		{"PostgreSQL SET STATISTICS", "postgres", "ALTER TABLE orders ALTER COLUMN total SET STATISTICS 100;"},
		{"MySQL SET DEFAULT", "mysql", "ALTER TABLE orders ALTER COLUMN total SET DEFAULT 1;"},
		{"MySQL SET VISIBLE", "mysql", "ALTER TABLE orders ALTER COLUMN total SET VISIBLE;"},
		{"MariaDB IF EXISTS", "mariadb", "ALTER TABLE orders ALTER COLUMN IF EXISTS total SET DEFAULT 1;"},
		{"MariaDB IF EXISTS without COLUMN's type", "mariadb", "ALTER TABLE orders ALTER COLUMN IF EXISTS total DROP DEFAULT;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTypeChange(c, test.dialect, test.alter)
			c.Assert(ds103Codes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestColumnTypeChangedRule_RestatedTypeOnSQLServer reads SQL Server's clause against the
// state the version starts from. The clause has to name a type to change anything else,
// so a type the column already holds is no change, and any spelling the two do not share
// leaves the finding standing.
func TestColumnTypeChangedRule_RestatedTypeOnSQLServer(t *testing.T) {
	tests := []struct {
		name    string
		alter   string
		columns []lintext.BaselineColumn
		want    int
	}{
		{"a nullability change", "ALTER TABLE orders ALTER COLUMN total int NOT NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "INT")}, 0},
		{"a nullability change in brackets", "ALTER TABLE [orders] ALTER COLUMN [total] INT NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "INT")}, 0},
		{"a length restated", "ALTER TABLE orders ALTER COLUMN note NVARCHAR(10) NOT NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("note", "NVARCHAR(10)")}, 0},
		{"a space the spelling does not count", "ALTER TABLE orders ALTER COLUMN total decimal(10, 2) NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "DECIMAL(10,2)")}, 0},
		{"a collation is not a type", "ALTER TABLE orders ALTER COLUMN note varchar(10) COLLATE Latin1_General_BIN NOT NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("note", "VARCHAR(10)")}, 0},
		{"SPARSE is not a type", "ALTER TABLE orders ALTER COLUMN total int SPARSE NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "INT")}, 0},
		{"no word behind the type", "ALTER TABLE orders ALTER COLUMN total int;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "INT")}, 0},
		{"another type", "ALTER TABLE orders ALTER COLUMN total bigint NOT NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "INT")}, 1},
		{"another length", "ALTER TABLE orders ALTER COLUMN note NVARCHAR(20) NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("note", "NVARCHAR(10)")}, 1},
		{"a synonym stands", "ALTER TABLE orders ALTER COLUMN total integer NOT NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "INT")}, 1},
		{"a column the state does not know", "ALTER TABLE orders ALTER COLUMN other int NOT NULL;",
			[]lintext.BaselineColumn{sqlServerColumn("total", "INT")}, 1},
		{"no state", "ALTER TABLE orders ALTER COLUMN total int NOT NULL;", nil, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTypeChange(c, "sqlserver", test.alter, test.columns...)
			c.Assert(ds103Codes(rulesOf(analysis.Findings())), qt.HasLen, test.want)
		})
	}
}
