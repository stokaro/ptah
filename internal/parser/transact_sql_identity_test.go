package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// `ptah db read` describes a SQL Server identity column with T-SQL's IDENTITY
// property, `[id] INT IDENTITY(1,1) PRIMARY KEY`. Read as a schema file, the
// description was refused with `unsupported column attribute: IDENTITY`, so a
// description of nearly any SQL Server database could not be read back.

// identityColumn is the part of a parsed column these tests compare.
type identityColumn struct {
	AutoInc    bool
	Generation string
	Start      string
	Increment  string
	Nullable   bool
}

// parseIdentityColumn reads sql as dialect and returns its one table's first
// column.
func parseIdentityColumn(c *qt.C, dialect, sql string) identityColumn {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("parsed %T", statements.Statements[0]))
	column := table.Columns[0]
	return identityColumn{
		AutoInc: column.AutoInc, Generation: column.IdentityGeneration,
		Start: column.IdentityStart, Increment: column.IdentityIncrement, Nullable: column.Nullable,
	}
}

// TestParseIdentityProperty_HappyPath reads the IDENTITY spellings SQL Server
// 2025 accepts, with the seed and increment it records for each: a bare
// IDENTITY leaves both unset, which renders as IDENTITY(1,1), and a plus sign
// is dropped as the server drops it. Every identity column is NOT NULL, as
// sys.columns records it whether or not the definition says so. A document of
// no dialect takes the property too, as it takes any engine's spelling.
func TestParseIdentityProperty_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    identityColumn
	}{
		{
			name:    "the db read form",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE [t] (\n  [id] INT IDENTITY(1,1) PRIMARY KEY,\n  [a] INT\n);",
			want:    identityColumn{AutoInc: true, Start: "1", Increment: "1"},
		},
		{
			name:    "a seed and an increment of their own",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id BIGINT IDENTITY(100,5) NOT NULL, a INT);",
			want:    identityColumn{AutoInc: true, Start: "100", Increment: "5"},
		},
		{
			name:    "bare",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT NOT NULL IDENTITY, a INT);",
			want:    identityColumn{AutoInc: true},
		},
		{
			name:    "signed values and spaces",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT IDENTITY ( -10 , +2 ), a INT);",
			want:    identityColumn{AutoInc: true, Start: "-10", Increment: "2"},
		},
		{
			name:    "NOT NULL from the property alone",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT IDENTITY(1,1), a INT);",
			want:    identityColumn{AutoInc: true, Start: "1", Increment: "1"},
		},
		{
			name:    "lower case",
			dialect: platform.SQLServer,
			sql:     "create table t (id int identity(7, 9) primary key);",
			want:    identityColumn{AutoInc: true, Start: "7", Increment: "9"},
		},
		{
			name: "no dialect",
			sql:  "CREATE TABLE t (id INT IDENTITY(3,4) PRIMARY KEY);",
			want: identityColumn{AutoInc: true, Start: "3", Increment: "4"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := parseIdentityColumn(c, test.dialect, test.sql)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestParseIdentityProperty_FailurePath refuses what SQL Server 2025 refuses,
// and the property on a dialect whose grammar does not have it.
func TestParseIdentityProperty_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a seed alone, Msg 102",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT IDENTITY(1) PRIMARY KEY);",
			wantErr: `(?s)line 1, column 34: .*IDENTITY takes a seed and an increment: expected ',', got '\)'.*`,
		},
		{
			name:    "a decimal seed, Msg 2752",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id DECIMAL(10,0) IDENTITY(1.5, 1) PRIMARY KEY);",
			wantErr: `(?s)line 1, column 43: .*expected the IDENTITY seed as an integer: got "1\.5".*`,
		},
		{
			name:    "a hexadecimal seed, Msg 102",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT IDENTITY(0x10, 1) PRIMARY KEY);",
			wantErr: `(?s)line 1, column 34: .*IDENTITY takes a seed and an increment: expected Operator, got Identifier.*`,
		},
		{
			name:    "an unclosed pair",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT IDENTITY(1, 1 PRIMARY KEY);",
			wantErr: `(?s).*expected '\)' after the IDENTITY increment.*`,
		},
		{
			name:    "NULL after the property, Msg 8147",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT IDENTITY NULL, a INT);",
			wantErr: `(?s)line 1, column 33: .*an IDENTITY column is NOT NULL, and SQL Server 2025 answers Msg 8147 to NULL on one.*`,
		},
		{
			name:    "NULL before the property, Msg 8147",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id INT NULL IDENTITY(1,1), a INT);",
			wantErr: `(?s)line 1, column 29: .*an IDENTITY column is NOT NULL, and SQL Server 2025 answers Msg 8147 to NULL on one.*`,
		},
		{
			name:    "PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY);",
			wantErr: `(?s)line 1, column 24: .*unsupported column attribute: IDENTITY.*`,
		},
		{
			name:    "MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE t (id INT IDENTITY PRIMARY KEY);",
			wantErr: `(?s)line 1, column 24: .*unsupported column attribute: IDENTITY.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
