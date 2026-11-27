package offlinenames_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/offlinenames"
)

func TestUnanswered_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		semantics identifier.Semantics
		want      bool
	}{
		{name: "the SQL Server offline rules", semantics: identifier.ForDialect(platform.SQLServer), want: true},
		{
			name:      "a SQL Server catalog a connection resolved",
			semantics: identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").WithResolvedNames([]identifier.ResolvedName{{Name: "dbo", Key: "dbo"}}),
			want:      false,
		},
		{name: "the PostgreSQL offline rules", semantics: identifier.ForDialect(platform.Postgres), want: false},
		{name: "the MySQL offline rules", semantics: identifier.ForDialect(platform.MySQL), want: false},
		{name: "the SQLite offline rules", semantics: identifier.ForDialect(platform.SQLite), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(offlinenames.Unanswered(test.semantics), qt.Equals, test.want)
		})
	}
}

func TestIgnoresCatalog_HappyPath(t *testing.T) {
	exact := identifier.Semantics{DefaultSchema: "dbo", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact}
	tests := []struct {
		name      string
		semantics identifier.Semantics
		dialect   string
		want      bool
	}{
		{name: "the exact rule for SQL Server", semantics: exact, dialect: platform.SQLServer, want: true},
		{name: "the PostgreSQL rules for SQL Server", semantics: identifier.ForDialect(platform.Postgres), dialect: platform.SQLServer, want: true},
		{name: "the SQL Server offline rules", semantics: identifier.ForDialect(platform.SQLServer), dialect: platform.SQLServer, want: false},
		{
			name:      "a SQL Server catalog a connection resolved",
			semantics: identifier.ForSQLServerCatalog("Latin1_General_100_CI_AS"),
			dialect:   platform.SQLServer,
			want:      false,
		},
		{name: "the exact rule for PostgreSQL", semantics: exact, dialect: platform.Postgres, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(offlinenames.IgnoresCatalog(test.semantics, test.dialect), qt.Equals, test.want)
		})
	}
}

func TestRolesByCatalog_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    bool
	}{
		{name: "SQL Server", dialect: platform.SQLServer, want: true},
		{name: "PostgreSQL", dialect: platform.Postgres, want: false},
		{name: "MySQL", dialect: platform.MySQL, want: false},
		{name: "Oracle", dialect: platform.Oracle, want: false},
		{name: "an unknown dialect", dialect: "nosuchdb", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(offlinenames.RolesByCatalog(test.dialect), qt.Equals, test.want)
		})
	}
}
