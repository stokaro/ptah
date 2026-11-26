package roundingscan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/roundingscan"
)

// rulesOf is the rounding a bundled descriptor states.
func rulesOf(c *qt.C, dialect string) descriptor.Rounding {
	for _, described := range builtindialects.Descriptors() {
		if described.Name == dialect {
			return described.Rounding
		}
	}
	c.Fatalf("no bundled descriptor %q", dialect)
	return descriptor.Rounding{}
}

// quotedColumn is the column c as each engine's scan quotes it.
var quotedColumn = map[string]string{
	"postgres": `"c"`, "cockroachdb": `"c"`, "yugabytedb": `"c"`, "mysql": "`c`", "mariadb": "`c`",
	"sqlserver": "[c]", "clickhouse": "`c`", "oracle": `"c"`, "sqlite": `"c"`,
}

// TestCondition_HappyPath renders the condition each engine's descriptor states: the
// server's own CAST to the declared type, spelled as that CAST takes it. PostgreSQL
// compares a decimal as an unlimited numeric, and a temporal value in the declared type
// with no precision, so a dropped time zone compares the time of day the new column
// keeps. SQL Server compares a temporal value at seven digits in the declared type's
// family, and ClickHouse lets the CAST read a NULL.
func TestCondition_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		change  roundingscan.Change
		want    string
	}{
		{"PostgreSQL a scale", "postgres",
			roundingscan.Change{HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`CAST("c" AS numeric) IS DISTINCT FROM CAST("c" AS numeric(8,1))`},
		{"PostgreSQL money", "postgres",
			roundingscan.Change{HeldType: "money", DeclaredType: "numeric(12,1)"},
			`CAST("c" AS numeric) IS DISTINCT FROM CAST("c" AS numeric(12,1))`},
		{"PostgreSQL an interval's fields", "postgres",
			roundingscan.Change{HeldType: "interval", DeclaredType: "interval hour", Temporal: true},
			`CAST("c" AS interval) IS DISTINCT FROM CAST("c" AS interval hour)`},
		{"PostgreSQL an interval's precision after its fields", "postgres",
			roundingscan.Change{HeldType: "interval day to second(6)", DeclaredType: "interval day to second(0)", Temporal: true},
			`CAST("c" AS interval) IS DISTINCT FROM CAST("c" AS interval day to second(0))`},
		{"PostgreSQL a time zone dropped", "postgres",
			roundingscan.Change{HeldType: "timestamptz", DeclaredType: "timestamp(0)", Temporal: true},
			`CAST("c" AS timestamp) IS DISTINCT FROM CAST("c" AS timestamp(0))`},
		{"PostgreSQL a precision before the time zone", "postgres",
			roundingscan.Change{HeldType: "timestamp(6) with time zone", DeclaredType: "timestamp(3) with time zone", Temporal: true},
			`CAST("c" AS timestamp with time zone) IS DISTINCT FROM CAST("c" AS timestamp(3) with time zone)`},
		{"PostgreSQL a serial", "postgres",
			roundingscan.Change{HeldType: "numeric(8,3)", DeclaredType: "bigserial"},
			`CAST("c" AS numeric) IS DISTINCT FROM CAST("c" AS bigint)`},
		{"CockroachDB whole seconds", "cockroachdb",
			roundingscan.Change{HeldType: "TIMESTAMP(6)", DeclaredType: "timestamp(0)", Temporal: true},
			`CAST("c" AS timestamp) IS DISTINCT FROM CAST("c" AS timestamp(0))`},
		{"CockroachDB a time zone dropped", "cockroachdb",
			roundingscan.Change{HeldType: "timetz", DeclaredType: "TIME(0)", Temporal: true},
			`CAST("c" AS TIME) IS DISTINCT FROM CAST("c" AS TIME(0))`},
		{"YugabyteDB an integer", "yugabytedb",
			roundingscan.Change{HeldType: "numeric(8,3)", DeclaredType: "integer"},
			`CAST("c" AS numeric) IS DISTINCT FROM CAST("c" AS integer)`},
		{"MySQL a scale", "mysql",
			roundingscan.Change{HeldType: "decimal(8,3)", DeclaredType: "NUMERIC(8,1)"},
			"`c` <> CAST(`c` AS DECIMAL(8,1))"},
		{"MySQL an integer with its display width", "mysql",
			roundingscan.Change{HeldType: "decimal(8,3)", DeclaredType: "INT(11)"},
			"`c` <> CAST(`c` AS SIGNED)"},
		{"MySQL an unsigned integer", "mysql",
			roundingscan.Change{HeldType: "decimal(8,3)", DeclaredType: "bigint unsigned"},
			"`c` <> CAST(`c` AS UNSIGNED)"},
		{"MySQL a timestamp", "mysql",
			roundingscan.Change{HeldType: "timestamp(6)", DeclaredType: "TIMESTAMP(3)", Temporal: true},
			"`c` <> CAST(`c` AS DATETIME(3))"},
		{"MariaDB a bare time", "mariadb",
			roundingscan.Change{HeldType: "time(6)", DeclaredType: "time", Temporal: true},
			"`c` <> CAST(`c` AS TIME)"},
		{"SQL Server a scale", "sqlserver",
			roundingscan.Change{HeldType: "decimal(8,3)", DeclaredType: "decimal(8,1)"},
			"[c] <> CAST([c] AS decimal(8,1))"},
		{"SQL Server datetime", "sqlserver",
			roundingscan.Change{HeldType: "datetime2(3)", DeclaredType: "datetime", Temporal: true},
			"CAST([c] AS datetime2(7)) <> CAST(CAST([c] AS datetime) AS datetime2(7))"},
		{"SQL Server smalldatetime", "sqlserver",
			roundingscan.Change{HeldType: "DATETIME2(0)", DeclaredType: "SMALLDATETIME", Temporal: true},
			"CAST([c] AS datetime2(7)) <> CAST(CAST([c] AS SMALLDATETIME) AS datetime2(7))"},
		{"SQL Server an offset dropped", "sqlserver",
			roundingscan.Change{HeldType: "DATETIMEOFFSET", DeclaredType: "DATETIME2(0)", Temporal: true},
			"CAST([c] AS datetime2(7)) <> CAST(CAST([c] AS DATETIME2(0)) AS datetime2(7))"},
		{"SQL Server an offset kept", "sqlserver",
			roundingscan.Change{HeldType: "DATETIMEOFFSET", DeclaredType: "DATETIMEOFFSET(3)", Temporal: true},
			"CAST([c] AS datetimeoffset(7)) <> CAST(CAST([c] AS DATETIMEOFFSET(3)) AS datetimeoffset(7))"},
		{"SQL Server a time", "sqlserver",
			roundingscan.Change{HeldType: "time", DeclaredType: "time(0)", Temporal: true},
			"CAST([c] AS time(7)) <> CAST(CAST([c] AS time(0)) AS time(7))"},
		{"ClickHouse a decimal", "clickhouse",
			roundingscan.Change{HeldType: "Decimal(8, 3)", DeclaredType: "Decimal(8, 1)"},
			"`c` != CAST(assumeNotNull(`c`) AS Decimal(8, 1))"},
		{"ClickHouse milliseconds in a time zone", "clickhouse",
			roundingscan.Change{HeldType: "DateTime64(6, 'UTC')", DeclaredType: "DateTime64(3, 'Asia/Kolkata')", Temporal: true},
			"`c` != CAST(assumeNotNull(`c`) AS DateTime64(3, 'Asia/Kolkata'))"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := roundingscan.Condition(rulesOf(c, test.dialect), quotedColumn[test.dialect], test.change)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestCondition_FailurePath holds the engines no condition is rendered for, because the
// server refuses the conversion itself or a declared size is not enforced.
func TestCondition_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		change  roundingscan.Change
	}{
		{"Oracle a scale", "oracle", roundingscan.Change{HeldType: "NUMBER(8,3)", DeclaredType: "NUMBER(8,1)"}},
		{"Oracle fractional seconds", "oracle", roundingscan.Change{HeldType: "TIMESTAMP(6)", DeclaredType: "TIMESTAMP(0)", Temporal: true}},
		{"SQLite a scale", "sqlite", roundingscan.Change{HeldType: "decimal(8,3)", DeclaredType: "decimal(8,1)"}},
		{"SQLite fractional seconds", "sqlite", roundingscan.Change{HeldType: "datetime", DeclaredType: "datetime", Temporal: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := roundingscan.Condition(rulesOf(c, test.dialect), quotedColumn[test.dialect], test.change)

			c.Assert(err, qt.ErrorMatches, "no condition reads the values of such a change")
			c.Assert(got, qt.Equals, "")
		})
	}
}

// notTypes are spellings outside the type grammar. The first is the declared type
// that, spliced into the scan, ended its query and created a table on SQL Server.
var notTypes = []string{
	"DECIMAL(8,1)) THEN 1 END) FROM t; CREATE TABLE pwned (i int); SELECT COUNT(CASE WHEN 1 <> CAST(1 AS int",
	"numeric(8,1); DROP TABLE prices",
	"int -- a comment",
	"int/**/",
	`"numeric"(8,1)`,
	"DateTime64(3, 'UTC'')",
	"numeric(8,1",
	"Nullable(Decimal(8, 1))",
	"numeric(8.1)",
	"",
}

// TestCondition_RefusesASpellingOutsideTheTypeGrammar renders nothing for a declared
// type outside the grammar, on an engine that would otherwise render the condition.
func TestCondition_RefusesASpellingOutsideTheTypeGrammar(t *testing.T) {
	for _, declared := range notTypes {
		t.Run(declared, func(t *testing.T) {
			c := qt.New(t)
			change := roundingscan.Change{HeldType: "DECIMAL(8,3)", DeclaredType: declared}

			got, err := roundingscan.Condition(rulesOf(c, "sqlserver"), "[c]", change)

			c.Assert(err, qt.ErrorIs, roundingscan.ErrNotAType)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// castRules list a spelling for some names, with and without their arguments.
var castRules = descriptor.Rounding{CastTypes: map[string]string{
	"int": "SIGNED", "int unsigned": "UNSIGNED", "timestamp": "DATETIME{args}", "numeric": "DECIMAL{args}",
}}

// TestCastType_HappyPath holds the cast spellings to what a declared type turns into,
// and a type no spelling is listed for to itself.
func TestCastType_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		typ  string
		want string
	}{
		{"a name with its arguments dropped", "INT(11)", "SIGNED"},
		{"an unsigned name", "int(10) unsigned", "UNSIGNED"},
		{"the arguments kept", "timestamp(3)", "DATETIME(3)"},
		{"no arguments to keep", "TIMESTAMP", "DATETIME"},
		{"arguments after a space", "numeric (8, 1)", "DECIMAL(8, 1)"},
		{"a type not listed", " datetime(6) ", "datetime(6)"},
		{"trailing words", "timestamp(3) with time zone", "DATETIME(3)"},
		{"a quoted time zone", "DateTime64(3, 'America/New_York')", "DateTime64(3, 'America/New_York')"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := roundingscan.CastType(castRules, test.typ)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestCastType_FailurePath refuses a spelling outside the type grammar rather than
// writing it into a CAST unchanged.
func TestCastType_FailurePath(t *testing.T) {
	for _, typ := range notTypes {
		t.Run(typ, func(t *testing.T) {
			c := qt.New(t)

			got, err := roundingscan.CastType(castRules, typ)

			c.Assert(err, qt.ErrorIs, roundingscan.ErrNotAType)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestCheck_HappyPath accepts the records the comparison writes, as plans measured on
// each engine hold them: the held type as the catalog spells it, the declared type as
// the renderer reads it.
func TestCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		change  roundingscan.Change
	}{
		{"PostgreSQL a scale", "postgres", roundingscan.Change{Table: "public.prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"}},
		{"PostgreSQL a time zone dropped", "postgres", roundingscan.Change{Table: "public.prices", Column: "clock", HeldType: "timetz", DeclaredType: "time(0)", Temporal: true}},
		{"PostgreSQL an interval's fields", "postgres", roundingscan.Change{Table: "public.prices", Column: "hours", HeldType: "interval(0)", DeclaredType: "interval hour", Temporal: true}},
		{"CockroachDB an integer", "cockroachdb", roundingscan.Change{Table: "public.prices", Column: "whole", HeldType: "numeric(8,3)", DeclaredType: "int8"}},
		{"MySQL an unsigned integer", "mysql", roundingscan.Change{Table: "prices", Column: "whole", HeldType: "decimal(8,3) unsigned", DeclaredType: "INT UNSIGNED"}},
		{"MariaDB a datetime", "mariadb", roundingscan.Change{Table: "prices", Column: "at", HeldType: "datetime(6)", DeclaredType: "DATETIME", Temporal: true}},
		{"SQL Server money", "sqlserver", roundingscan.Change{Table: "prices", Column: "cash", HeldType: "MONEY", DeclaredType: "DECIMAL(12,1)"}},
		{"SQL Server an offset dropped", "sqlserver", roundingscan.Change{Table: "prices", Column: "local", HeldType: "DATETIMEOFFSET", DeclaredType: "DATETIME2(0)", Temporal: true}},
		{"ClickHouse a time zone", "clickhouse", roundingscan.Change{Table: "prices", Column: "at", HeldType: "DateTime64(6, 'UTC')", DeclaredType: "DateTime64(3, 'UTC')", Temporal: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(roundingscan.Check(test.dialect, test.change), qt.IsNil)
		})
	}
}

// TestCheck_FailurePath refuses a record the comparison could not have written: one
// edited in a plan file to carry SQL, to name a pair of types that keeps every digit,
// or to claim the other kind of type.
func TestCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		change  roundingscan.Change
		want    string
	}{
		{"no table", "postgres", roundingscan.Change{Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`it names no table or column`},
		{"no column", "postgres", roundingscan.Change{Table: "prices", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`it names no table or column`},
		{"SQL in the declared type", "sqlserver", roundingscan.Change{Table: "prices", Column: "amount", HeldType: "DECIMAL(8,3)",
			DeclaredType: "DECIMAL(8,1)) THEN 1 END) FROM t; CREATE TABLE pwned (i int); SELECT COUNT(CASE WHEN 1 <> CAST(1 AS int"},
			`the declared type is not a type spelling: .*`},
		{"SQL in the held type", "sqlserver", roundingscan.Change{Table: "prices", Column: "at", HeldType: "DATETIME2(7)); CREATE TABLE pwned (i int); --",
			DeclaredType: "DATETIME2(0)", Temporal: true},
			`the held type is not a type spelling: .*`},
		{"no held type", "postgres", roundingscan.Change{Table: "prices", Column: "amount", DeclaredType: "numeric(8,1)"},
			`the held type is not a type spelling: ""`},
		{"a scale raised", "postgres", roundingscan.Change{Table: "prices", Column: "amount", HeldType: "numeric(8,1)", DeclaredType: "numeric(8,3)"},
			`numeric\(8,1\) under numeric\(8,3\) is not a change that keeps fewer digits after the point`},
		{"a text type", "postgres", roundingscan.Change{Table: "prices", Column: "amount", HeldType: "varchar(10)", DeclaredType: "integer"},
			`varchar\(10\) under integer is not a change that keeps fewer digits after the point`},
		{"a decimal called temporal", "postgres", roundingscan.Change{Table: "prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)", Temporal: true},
			`numeric\(8,3\) under numeric\(8,1\) is not a change that keeps fewer digits after the point`},
		{"a temporal type called decimal", "postgres", roundingscan.Change{Table: "prices", Column: "at", HeldType: "timestamp(6)", DeclaredType: "timestamp(0)"},
			`timestamp\(6\) under timestamp\(0\) is not a change that keeps fewer digits after the point`},
		{"an engine that reads no value", "oracle", roundingscan.Change{Table: "PRICES", Column: "AMOUNT", HeldType: "NUMBER(8,3)", DeclaredType: "NUMBER(8,1)"},
			`dialect "oracle" reads no value of such a change`},
		{"a dialect nobody registered", "nosuchdialect", roundingscan.Change{Table: "prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`dialect "nosuchdialect" is not registered`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(roundingscan.Check(test.dialect, test.change), qt.ErrorMatches, test.want)
		})
	}
}

// TestNames_HappyPath holds a record to the statement that converts its column, as each
// engine's plan writes it, and reads a quoted name by the engine's own quoting: a
// doubled quote on every engine, and a backslash escape on ClickHouse.
func TestNames_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		change  roundingscan.Change
	}{
		{"PostgreSQL", "postgres", `ALTER TABLE "public"."prices" ALTER COLUMN "amount" TYPE numeric(8,1)`,
			roundingscan.Change{Table: "public.prices", Column: "amount"}},
		{"SQL Server", "sqlserver", "ALTER TABLE [prices] ALTER COLUMN [amount] DECIMAL(8,1) NULL",
			roundingscan.Change{Table: "prices", Column: "amount"}},
		{"MySQL", "mysql", "ALTER TABLE `prices` MODIFY COLUMN `amount` DECIMAL(8,1)",
			roundingscan.Change{Table: "prices", Column: "amount"}},
		{"ClickHouse", "clickhouse", "ALTER TABLE `prices` MODIFY COLUMN `amount` Decimal(8, 1)",
			roundingscan.Change{Table: "prices", Column: "amount"}},
		{"ClickHouse unquoted", "clickhouse", "ALTER TABLE prices MODIFY COLUMN amount Decimal(8, 1)",
			roundingscan.Change{Table: "prices", Column: "amount"}},
		{"a name in another case", "postgres", `ALTER TABLE "public"."Prices" ALTER COLUMN "Amount" TYPE numeric(8,1)`,
			roundingscan.Change{Table: "public.prices", Column: "amount"}},
		{"a doubled double quote", "postgres", `ALTER TABLE "public"."prices" ALTER COLUMN "a""b" TYPE numeric(8,1)`,
			roundingscan.Change{Table: "public.prices", Column: `a"b`}},
		{"a doubled backquote", "mysql", "ALTER TABLE `prices` MODIFY COLUMN `a``b` DECIMAL(8,1)",
			roundingscan.Change{Table: "prices", Column: "a`b"}},
		{"a doubled bracket", "sqlserver", "ALTER TABLE [prices] ALTER COLUMN [a]]b] DECIMAL(8,1) NULL",
			roundingscan.Change{Table: "prices", Column: "a]b"}},
		{"a MySQL backslash is a character", "mysql", "ALTER TABLE `prices` MODIFY COLUMN `a\\b` DECIMAL(8,1)",
			roundingscan.Change{Table: "prices", Column: `a\b`}},
		{"a ClickHouse backslash escape", "clickhouse", "ALTER TABLE `p``q` MODIFY COLUMN `a\\\\b` Decimal(8, 1)",
			roundingscan.Change{Table: `"p` + "`" + `q"`, Column: `a\b`}},
		{"a ClickHouse comment marker", "clickhouse", "ALTER TABLE `prices` MODIFY COLUMN `c -- d` Decimal(8, 1)",
			roundingscan.Change{Table: "prices", Column: "c -- d"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(roundingscan.Names(test.dialect, test.sql, test.change), qt.IsTrue)
		})
	}
}

// TestNames_FailurePath refuses a statement that does not name the record's column and
// table: a column of its own, a name that only starts with the column's, a column named
// in a literal or a comment, another table, and a name read by another engine's quoting.
func TestNames_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		change  roundingscan.Change
	}{
		{"another column", "postgres", `ALTER TABLE "public"."prices" ALTER COLUMN "amount" TYPE numeric(8,1)`,
			roundingscan.Change{Table: "public.prices", Column: "id"}},
		{"a longer name", "postgres", `ALTER TABLE "public"."prices" ALTER COLUMN "amounts" TYPE numeric(8,1)`,
			roundingscan.Change{Table: "public.prices", Column: "amount"}},
		{"a literal", "postgres", `COMMENT ON TABLE "public"."prices" IS 'amount'`,
			roundingscan.Change{Table: "public.prices", Column: "amount"}},
		{"a comment", "clickhouse", "ALTER TABLE prices MODIFY COLUMN c -- amount\n Decimal(8, 1)",
			roundingscan.Change{Table: "prices", Column: "amount"}},
		{"another table", "sqlserver", "ALTER TABLE [orders] ALTER COLUMN [amount] DECIMAL(8,1) NULL",
			roundingscan.Change{Table: "prices", Column: "amount"}},
		{"a table name that does not parse", "sqlserver", "ALTER TABLE [prices] ALTER COLUMN [amount] DECIMAL(8,1) NULL",
			roundingscan.Change{Table: "", Column: "amount"}},
		{"a part of a doubled quote", "mysql", "ALTER TABLE `prices` MODIFY COLUMN `a``b` DECIMAL(8,1)",
			roundingscan.Change{Table: "prices", Column: "a"}},
		{"an escaped backslash read as two", "clickhouse", "ALTER TABLE `prices` MODIFY COLUMN `a\\\\b` Decimal(8, 1)",
			roundingscan.Change{Table: "prices", Column: `a\\b`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(roundingscan.Names(test.dialect, test.sql, test.change), qt.IsFalse)
		})
	}
}

// TestChanges_ListsEveryScannableRoundingInDiffOrder reads the columns the comparison
// marked, with the type the database holds and the type the plan converts to, and leaves
// out a type change that keeps every digit after the point.
func TestChanges_ListsEveryScannableRoundingInDiffOrder(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		{TableName: "public.prices", ColumnsModified: []difftypes.ColumnDiff{
			{ColumnName: "amount", Changes: map[string]string{"type": "numeric -> numeric(8,1)"},
				Rounding: &difftypes.Rounding{HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"}},
			{ColumnName: "name", Changes: map[string]string{"type": "varchar(10) -> VARCHAR(3)"}, TypeNarrows: true},
		}},
		{TableName: "events", ColumnsModified: []difftypes.ColumnDiff{
			{ColumnName: "at", Changes: map[string]string{"type": "timestamp -> timestamp(3)"},
				Rounding: &difftypes.Rounding{Temporal: true, HeldType: "timestamp(6) without time zone", DeclaredType: "timestamp(3)"}},
		}},
	}}

	got := roundingscan.Changes(diff, "postgres")

	c.Assert(got, qt.DeepEquals, []roundingscan.Change{
		{Table: "public.prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
		{Table: "events", Column: "at", HeldType: "timestamp(6) without time zone", DeclaredType: "timestamp(3)", Temporal: true},
	})
}

// TestChanges_HoldsNoneWhereNothingIsRead holds the nil diff a plan with nothing to
// compare has, and the engines whose descriptor states no condition: Oracle refuses the
// conversion itself, so a plan for it reads no row and warns of nothing.
func TestChanges_HoldsNoneWhereNothingIsRead(t *testing.T) {
	marked := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "PRICES", ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "AMOUNT", Rounding: &difftypes.Rounding{HeldType: "NUMBER(8,3)", DeclaredType: "NUMBER(8,1)"},
		}},
	}}}
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		dialect string
	}{
		{"a nil diff", nil, "postgres"},
		{"Oracle", marked, "oracle"},
		{"SQLite", marked, "sqlite"},
		{"a dialect nobody registered", marked, "nosuchdialect"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(roundingscan.Changes(test.diff, test.dialect), qt.HasLen, 0)
		})
	}
}

// TestAssertion_HappyPath writes the SELECT a migration file carries for a change: the
// condition the scan counts, asked through EXISTS on the table as the scan names it,
// answering 1 while no row would change. A table the comparison names without a schema
// is read without one, where the connection resolves it.
func TestAssertion_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		change  roundingscan.Change
		want    string
	}{
		{"PostgreSQL a scale", "postgres",
			roundingscan.Change{Table: "public.prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`SELECT CASE WHEN EXISTS (SELECT 1 FROM "public"."prices" WHERE CAST("amount" AS numeric) IS DISTINCT FROM ` +
				`CAST("amount" AS numeric(8,1))) THEN 0 ELSE 1 END`},
		{"PostgreSQL a quoted name", "postgres",
			roundingscan.Change{Table: `public."odd ""name"""`, Column: `a"b`, HeldType: "timestamp(6)", DeclaredType: "timestamp(0)", Temporal: true},
			`SELECT CASE WHEN EXISTS (SELECT 1 FROM "public"."odd ""name""" WHERE CAST("a""b" AS timestamp) IS DISTINCT FROM ` +
				`CAST("a""b" AS timestamp(0))) THEN 0 ELSE 1 END`},
		{"MySQL an integer", "mysql",
			roundingscan.Change{Table: "prices", Column: "whole", HeldType: "decimal(8,3)", DeclaredType: "INT"},
			"SELECT CASE WHEN EXISTS (SELECT 1 FROM `prices` WHERE `whole` <> CAST(`whole` AS SIGNED)) THEN 0 ELSE 1 END"},
		{"SQL Server datetime", "sqlserver",
			roundingscan.Change{Table: "dbo.prices", Column: "at", HeldType: "datetime2(3)", DeclaredType: "datetime", Temporal: true},
			"SELECT CASE WHEN EXISTS (SELECT 1 FROM [dbo].[prices] WHERE CAST([at] AS datetime2(7)) <> " +
				"CAST(CAST([at] AS datetime) AS datetime2(7))) THEN 0 ELSE 1 END"},
		{"ClickHouse a decimal", "clickhouse",
			roundingscan.Change{Table: "prices", Column: "amount", HeldType: "Decimal(8, 3)", DeclaredType: "Decimal(8, 1)"},
			"SELECT CASE WHEN EXISTS (SELECT 1 FROM `prices` WHERE `amount` != CAST(assumeNotNull(`amount`) AS Decimal(8, 1))) " +
				"THEN 0 ELSE 1 END"},
		{"ClickHouse a name the server reads an escape in", "clickhouse",
			roundingscan.Change{Table: "p`q", Column: `a\b -- c`, HeldType: "Decimal(8, 3)", DeclaredType: "Decimal(8, 1)"},
			"SELECT CASE WHEN EXISTS (SELECT 1 FROM `p``q` WHERE `a\\\\b -- c` != CAST(assumeNotNull(`a\\\\b -- c`) AS Decimal(8, 1))) " +
				"THEN 0 ELSE 1 END"},
		{"MySQL a backslash is a character", "mysql",
			roundingscan.Change{Table: "prices", Column: `a\b`, HeldType: "decimal(8,3)", DeclaredType: "INT"},
			"SELECT CASE WHEN EXISTS (SELECT 1 FROM `prices` WHERE `a\\b` <> CAST(`a\\b` AS SIGNED)) THEN 0 ELSE 1 END"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := roundingscan.Assertion(test.dialect, test.change)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestAssertion_FailurePath refuses what the scan refuses: a change [roundingscan.Check]
// refuses writes no SELECT, so a type outside the grammar never reaches a migration file,
// and a table name that does not parse names nothing to read.
func TestAssertion_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		change  roundingscan.Change
		want    string
	}{
		{"SQL in the declared type", "postgres",
			roundingscan.Change{Table: "prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)); DROP TABLE prices; --"},
			`read the values of column "amount" of table "prices": the declared type is not a type spelling: .*`},
		{"a scale raised", "mysql",
			roundingscan.Change{Table: "prices", Column: "amount", HeldType: "decimal(8,1)", DeclaredType: "decimal(8,3)"},
			`read the values of column "amount" of table "prices": decimal\(8,1\) under decimal\(8,3\) is not a change .*`},
		{"an engine that reads no value", "sqlite",
			roundingscan.Change{Table: "prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`read the values of column "amount" of table "prices": dialect "sqlite" reads no value of such a change`},
		{"a table name that does not parse", "postgres",
			roundingscan.Change{Table: "a.b.c", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`read the values of column "amount" of table "a.b.c": the table name "a.b.c" does not parse`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := roundingscan.Assertion(test.dialect, test.change)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestPolicy_ReviewReadsNothingItNeedNot holds the two answers that need no database: a
// plan asked to round reads no row and keeps no change to read again, and a plan with no
// such change reads nothing and refuses nothing. Neither touches the connection, which is
// nil here.
func TestPolicy_ReviewReadsNothingItNeedNot(t *testing.T) {
	change := roundingscan.Change{Table: "prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"}
	tests := []struct {
		name    string
		policy  roundingscan.Policy
		changes []roundingscan.Change
	}{
		{name: "the rounding allowed", policy: roundingscan.Policy{Allowed: true, Request: "--allow-value-rounding"},
			changes: []roundingscan.Change{change}},
		{name: "no change", policy: roundingscan.Policy{Request: "--allow-value-rounding"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			read, err := test.policy.Review(c.Context(), nil, test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(read, qt.HasLen, 0)
		})
	}
}
