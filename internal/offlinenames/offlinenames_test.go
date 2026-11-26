package offlinenames_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
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

func TestMark_HappyPath(t *testing.T) {
	c := qt.New(t)
	refusal := fmt.Errorf("%w: two names", ptaherr.ErrInvalidSchemaDiff)

	marked := fmt.Errorf("plan: %w", offlinenames.Mark(refusal))

	c.Assert(marked, qt.ErrorMatches, "plan: invalid schema diff: two names")
	c.Assert(marked, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(offlinenames.Marked(marked), qt.IsTrue)
}

func TestMark_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Assert(offlinenames.Mark(nil), qt.IsNil)
	c.Assert(offlinenames.Marked(errors.New("two names")), qt.IsFalse)
	c.Assert(offlinenames.Marked(nil), qt.IsFalse)
}
