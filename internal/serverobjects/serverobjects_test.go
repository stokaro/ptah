package serverobjects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/serverobjects"
)

// The extensions YugabyteDB installs in every database are its own, under any
// spelling of the dialect, and no other extension or dialect is.
func TestIsExtension(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		extension string
		want      bool
	}{
		{name: "pg_stat_statements on YugabyteDB", dialect: "yugabytedb", extension: "pg_stat_statements", want: true},
		{name: "postgres_fdw on YugabyteDB", dialect: "yugabytedb", extension: "postgres_fdw", want: true},
		{name: "the dialect's other spelling", dialect: "yugabyte", extension: "pg_stat_statements", want: true},
		{name: "an extension YugabyteDB does not install", dialect: "yugabytedb", extension: "pg_trgm", want: false},
		{name: "plpgsql, which the ignore list answers", dialect: "yugabytedb", extension: "plpgsql", want: false},
		{name: "a name in another case", dialect: "yugabytedb", extension: "PG_STAT_STATEMENTS", want: false},
		{name: "pg_stat_statements on PostgreSQL", dialect: "postgres", extension: "pg_stat_statements", want: false},
		{name: "postgres_fdw on CockroachDB", dialect: "cockroachdb", extension: "postgres_fdw", want: false},
		{name: "no dialect", dialect: "", extension: "pg_stat_statements", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(serverobjects.IsExtension(test.dialect, test.extension), qt.Equals, test.want)
		})
	}
}

// The list comes back sorted, for every spelling of the dialect, and a
// caller that changes its copy changes nothing the next caller reads.
func TestExtensions(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "YugabyteDB", dialect: "yugabytedb", want: []string{"pg_stat_statements", "postgres_fdw"}},
		{name: "the dialect's other spelling", dialect: "ysql", want: []string{"pg_stat_statements", "postgres_fdw"}},
		{name: "PostgreSQL", dialect: "postgres", want: nil},
		{name: "CockroachDB", dialect: "cockroachdb", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(serverobjects.Extensions(test.dialect), qt.DeepEquals, test.want)
		})
	}
}

func TestExtensions_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	first := serverobjects.Extensions("yugabytedb")
	first[0] = "changed"

	c.Assert(serverobjects.Extensions("yugabytedb"), qt.DeepEquals, []string{"pg_stat_statements", "postgres_fdw"})
}
