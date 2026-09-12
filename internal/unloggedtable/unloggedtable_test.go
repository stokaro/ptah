package unloggedtable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/unloggedtable"
)

// TestSupported_AnswersPerDialect pins the set, because both ends of the round
// trip read it: the renderer writes the keyword where this is true, and the
// reader asks pg_class for relpersistence where this is true. A dialect that
// moved into the set on one end alone would render a table the next read
// describes as logged.
func TestSupported_AnswersPerDialect(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    bool
	}{
		{name: "postgres", dialect: "postgres", want: true},
		{name: "postgresql spelling", dialect: "postgresql", want: true},
		{name: "yugabytedb", dialect: "yugabytedb", want: true},
		{name: "cockroachdb speaks the wire protocol and has no unlogged table", dialect: "cockroachdb", want: false},
		{name: "spanner speaks the wire protocol and has no unlogged table", dialect: "spanner", want: false},
		{name: "mysql", dialect: "mysql", want: false},
		{name: "mariadb", dialect: "mariadb", want: false},
		{name: "sqlite", dialect: "sqlite", want: false},
		{name: "sqlserver", dialect: "sqlserver", want: false},
		{name: "clickhouse", dialect: "clickhouse", want: false},
		{name: "oracle", dialect: "oracle", want: false},
		{name: "an unrecognized name", dialect: "not-a-database", want: false},
		{name: "the empty dialect", dialect: "", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(unloggedtable.Supported(test.dialect), qt.Equals, test.want)
		})
	}
}
