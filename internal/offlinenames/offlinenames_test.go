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
