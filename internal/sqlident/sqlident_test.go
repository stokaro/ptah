package sqlident_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/sqlident"
)

func TestQuote(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		ident   string
		want    string
	}{
		{name: "postgres double quotes", dialect: "postgres", ident: "users", want: `"users"`},
		{name: "sqlite double quotes", dialect: "sqlite", ident: "users", want: `"users"`},
		{name: "cockroachdb falls back to double quotes", dialect: "cockroachdb", ident: "users", want: `"users"`},
		{name: "yugabytedb falls back to double quotes", dialect: "yugabytedb", ident: "users", want: `"users"`},
		{name: "unknown dialect falls back to double quotes", dialect: "duckdb", ident: "users", want: `"users"`},
		{name: "mysql backticks", dialect: "mysql", ident: "users", want: "`users`"},
		{name: "mariadb backticks", dialect: "mariadb", ident: "users", want: "`users`"},
		{name: "clickhouse backticks", dialect: "clickhouse", ident: "users", want: "`users`"},
		{name: "sqlserver brackets", dialect: "sqlserver", ident: "users", want: "[users]"},
		// Every documented spelling of an engine has to pick that engine's quote
		// style. Reverting Quote to matching raw strings makes the four rows
		// below fail with `"users"` (SQL Server) and `"users"` (ClickHouse):
		// the raw switch listed only sqlserver/mssql and clickhouse, so tsql,
		// sql-server, sql_server and ch fell through to the default arm and
		// produced PostgreSQL quoting for a SQL Server / ClickHouse identifier.
		{name: "mssql alias brackets", dialect: "mssql", ident: "users", want: "[users]"},
		{name: "tsql alias brackets", dialect: "tsql", ident: "users", want: "[users]"},
		{name: "sql-server alias brackets", dialect: "sql-server", ident: "users", want: "[users]"},
		{name: "sql_server alias brackets", dialect: "sql_server", ident: "users", want: "[users]"},
		{name: "ch alias backticks", dialect: "ch", ident: "users", want: "`users`"},
		{name: "pgx alias double quotes", dialect: "pgx", ident: "users", want: `"users"`},
		{name: "sqlite3 alias double quotes", dialect: "sqlite3", ident: "users", want: `"users"`},
		{name: "dialect is case and space insensitive", dialect: "  MySQL ", ident: "users", want: "`users`"},
		{name: "postgres escapes embedded double quote", dialect: "postgres", ident: `a"b`, want: `"a""b"`},
		{name: "mysql escapes embedded backtick", dialect: "mysql", ident: "a`b", want: "`a``b`"},
		{name: "sqlserver escapes embedded bracket", dialect: "sqlserver", ident: "a]b", want: "[a]]b]"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(sqlident.Quote(tt.dialect, tt.ident), qt.Equals, tt.want)
		})
	}
}

func TestQualified(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		schema  string
		ident   string
		want    string
	}{
		{name: "postgres schema qualified", dialect: "postgres", schema: "public", ident: "users", want: `"public"."users"`},
		{name: "empty schema yields bare name", dialect: "postgres", schema: "", ident: "users", want: `"users"`},
		{name: "whitespace schema treated as empty", dialect: "postgres", schema: "  ", ident: " users ", want: `" users "`},
		{name: "schema and name bytes are preserved", dialect: "mysql", schema: " app ", ident: " users ", want: "` app `.` users `"},
		{name: "sqlserver schema qualified", dialect: "sqlserver", schema: "dbo", ident: "users", want: "[dbo].[users]"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(sqlident.Qualified(tt.dialect, tt.schema, tt.ident), qt.Equals, tt.want)
		})
	}
}
