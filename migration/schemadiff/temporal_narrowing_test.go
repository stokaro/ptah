package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// heldTemporalColumn is a read of t.c, with the type the engine's reader reports.
func heldTemporalColumn(columnType string) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Type: "BASE TABLE",
		Columns: []catalog.Column{{Name: "c", DataType: columnType, ColumnType: columnType, IsNullable: "YES"}},
	}}}
}

// heldFormattedColumn is a read of t.c on PostgreSQL, whose interval with fields the
// catalog spells only through format_type.
func heldFormattedColumn(formatted string) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Type: "BASE TABLE",
		Columns: []catalog.Column{{Name: "c", DataType: "interval", UDTName: "interval", FormattedType: formatted, IsNullable: "YES"}},
	}}}
}

// temporalDeclaration declares t.c with the given type.
func temporalDeclaration(columnType string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "c", Type: columnType, Nullable: true}},
	}
}

// TestCompareTemporalColumn_NarrowsAsTheEngineReadsIt plans each row of a type the
// catalog holds against the one the declaration names. The answer differs by engine,
// because a type that declares no precision means what the engine says it means, and
// only the comparison knows the engine.
func TestCompareTemporalColumn_NarrowsAsTheEngineReadsIt(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		held        string
		declared    string
		wantNarrows bool
	}{
		{"PostgreSQL six to none", "postgres", "timestamp(6) without time zone", "timestamp(0)", true},
		{"PostgreSQL bare to fewer", "postgres", "timestamp without time zone", "timestamp(3)", true},
		{"PostgreSQL fewer to bare widens", "postgres", "timestamp(3) without time zone", "timestamp", false},
		{"PostgreSQL an interval's digits", "postgres", "interval(6)", "interval(0)", true},
		{"CockroachDB six to none", "cockroachdb", "TIMESTAMP(6)", "timestamp(0)", true},
		{"CockroachDB fewer to bare widens", "cockroachdb", "TIMESTAMP(3)", "timestamp", false},
		{"YugabyteDB bare to fewer", "yugabytedb", "timestamp without time zone", "timestamp(3)", true},
		{"MySQL six to bare", "mysql", "timestamp(6)", "timestamp", true},
		{"MySQL a datetime to bare", "mysql", "datetime(6)", "datetime", true},
		{"MySQL bare to more widens", "mysql", "datetime", "datetime(3)", false},
		{"MariaDB six to bare", "mariadb", "datetime(6)", "datetime", true},
		{"SQL Server bare to fewer", "sqlserver", "DATETIME2", "datetime2(3)", true},
		{"SQL Server fewer to bare widens", "sqlserver", "DATETIME2(3)", "datetime2", false},
		{"SQL Server time to none", "sqlserver", "TIME", "time(0)", true},
		{"SQL Server datetime2 to datetime", "sqlserver", "DATETIME2", "datetime", true},
		{"SQL Server datetime to datetime2", "sqlserver", "DATETIME", "datetime2(7)", false},
		{"ClickHouse down", "clickhouse", "DateTime64(6)", "DateTime64(3)", true},
		{"ClickHouse up", "clickhouse", "DateTime64(3)", "DateTime64(6)", false},
		{"ClickHouse to a date", "clickhouse", "DateTime", "Date", true},
		{"Oracle a timestamp to a date", "oracle", "TIMESTAMP(6)", "DATE", true},
		{"Oracle a date to a timestamp", "oracle", "DATE", "TIMESTAMP", false},
		{"Oracle down", "oracle", "TIMESTAMP(6)", "TIMESTAMP(0)", true},
		{"Oracle bare to fewer", "oracle", "TIMESTAMP(6)", "TIMESTAMP(3)", true},
		{"Oracle up", "oracle", "TIMESTAMP(3)", "TIMESTAMP(6)", false},
		{"Oracle fewer to bare widens", "oracle", "TIMESTAMP(3)", "TIMESTAMP", false},
		{"Oracle with a zone", "oracle", "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(0) WITH TIME ZONE", true},
		{"Oracle a portable zone", "oracle", "TIMESTAMP(6) WITH TIME ZONE", "timestamptz(0)", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Changes["type"], qt.Not(qt.Equals), "")
			c.Assert(column.TypeNarrows, qt.Equals, test.wantNarrows)
		})
	}
}

// TestCompareTemporalColumn_OneTypeInTwoSpellingsIsNoChange holds the other side of the
// rating: a plan that restates a column under another spelling would be rated by it, and
// a false destructive rating stops every plan on a database a person adopted.
func TestCompareTemporalColumn_OneTypeInTwoSpellingsIsNoChange(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		held     string
		declared string
	}{
		{"PostgreSQL six and bare", "postgres", "timestamp(6) without time zone", "timestamp"},
		{"PostgreSQL bare and six", "postgres", "timestamp without time zone", "timestamp(6)"},
		{"PostgreSQL a zone in two spellings", "postgres", "timestamp with time zone", "timestamptz"},
		{"PostgreSQL a precision in two spellings", "postgres", "timestamp(3) with time zone", "timestamptz(3)"},
		{"PostgreSQL interval six and bare", "postgres", "interval(6)", "interval"},
		{"PostgreSQL an interval's fields", "postgres", "interval year to month", "interval year to month"},
		{"PostgreSQL an interval's fields and digits", "postgres", "interval day to second(3)", "interval day to second(3)"},
		{"PostgreSQL an interval's fields, six and bare", "postgres", "interval day to second(6)", "interval day to second"},
		{"CockroachDB six and bare", "cockroachdb", "TIMESTAMP(6)", "timestamp"},
		{"YugabyteDB six and bare", "yugabytedb", "timestamp(6) without time zone", "timestamp"},
		{"MySQL timestamp bare and zero", "mysql", "timestamp", "timestamp(0)"},
		{"MySQL datetime bare and zero", "mysql", "datetime", "datetime(0)"},
		{"MySQL time bare and zero", "mysql", "time", "time(0)"},
		{"MariaDB datetime bare and zero", "mariadb", "datetime", "datetime(0)"},
		{"SQL Server datetime2 bare and seven", "sqlserver", "DATETIME2", "datetime2(7)"},
		{"SQL Server datetime2 three and three", "sqlserver", "DATETIME2(3)", "datetime2(3)"},
		{"SQL Server datetime2 none and none", "sqlserver", "DATETIME2(0)", "datetime2(0)"},
		{"SQL Server time three and three", "sqlserver", "TIME(3)", "time(3)"},
		{"SQL Server offset three and three", "sqlserver", "DATETIMEOFFSET(3)", "datetimeoffset(3)"},
		{"ClickHouse a bare DateTime64", "clickhouse", "DateTime64(3)", "DateTime64"},
		{"Oracle six and bare", "oracle", "TIMESTAMP(6)", "TIMESTAMP"},
		{"Oracle a portable zone", "oracle", "TIMESTAMP(6) WITH TIME ZONE", "timestamptz"},
		{"Oracle a portable zone and digits", "oracle", "TIMESTAMP(3) WITH TIME ZONE", "timestamptz(3)"},
		{"Oracle a portable datetime and digits", "oracle", "TIMESTAMP(3)", "datetime(3)"},
		{"Oracle an interval and its default digits", "oracle", "INTERVAL DAY(2) TO SECOND(6)", "INTERVAL DAY TO SECOND"},
		{"Oracle a year interval and its default digits", "oracle", "INTERVAL YEAR(2) TO MONTH", "INTERVAL YEAR TO MONTH"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareTemporalColumn_AnIntervalTheCatalogSpellsThroughFormatType reads an
// interval's fields where PostgreSQL's reader puts them. A spelling held there also names
// a domain, and an interval is not one, so a declaration that spells the same fields
// otherwise stays one type with the column.
func TestCompareTemporalColumn_AnIntervalTheCatalogSpellsThroughFormatType(t *testing.T) {
	tests := []struct {
		name        string
		held        string
		declared    string
		wantChange  bool
		wantNarrows bool
	}{
		{name: "six and bare", held: "interval(6)", declared: "interval"},
		{name: "its digits", held: "interval(3)", declared: "interval(3)"},
		{name: "its fields", held: "interval year to month", declared: "interval year to month"},
		{name: "its fields and digits", held: "interval day to second(3)", declared: "interval day to second(3)"},
		{name: "fields, six and bare", held: "interval day to second(6)", declared: "interval day to second"},
		{name: "fewer digits", held: "interval(6)", declared: "interval(0)", wantChange: true, wantNarrows: true},
		{name: "fewer digits behind the fields", held: "interval day to second(6)", declared: "interval day to second(0)", wantChange: true, wantNarrows: true},
		{name: "fewer fields", held: "interval day to second", declared: "interval year to month", wantChange: true, wantNarrows: true},
		{name: "more fields", held: "interval hour to minute", declared: "interval", wantChange: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldFormattedColumn(test.held), "postgres", builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			columns := modifiedColumns(diff)
			c.Assert(len(columns) == 1, qt.Equals, test.wantChange)
			c.Assert(narrowedColumns(columns), qt.Equals, test.wantNarrows)
		})
	}
}

// modifiedColumns is every column change of diff.
func modifiedColumns(diff *difftypes.SchemaDiff) []difftypes.ColumnDiff {
	var columns []difftypes.ColumnDiff
	for _, table := range diff.TablesModified {
		columns = append(columns, table.ColumnsModified...)
	}
	return columns
}

// narrowedColumns reports whether a column change among columns narrows its type.
func narrowedColumns(columns []difftypes.ColumnDiff) bool {
	return slices.ContainsFunc(columns, func(column difftypes.ColumnDiff) bool { return column.TypeNarrows })
}

// TestCompareTemporalColumn_AnEngineThatDoesNotCompareAPrecision pins where no plan is
// made, so no rating is owed. SQLite stores a declaration without reading it.
func TestCompareTemporalColumn_AnEngineThatDoesNotCompareAPrecision(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		held     string
		declared string
	}{
		{"SQLite", "sqlite", "TIMESTAMP(6)", "TIMESTAMP(0)"},
		{"SQLite a date", "sqlite", "TIMESTAMP", "DATE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}
