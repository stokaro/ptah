package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/schemaprep"
)

// TestUniqueIndexMethodRefused answers from each dialect's descriptor which
// access methods build no unique index. PostgreSQL 18.6 refuses a unique hash
// index, YugabyteDB 2026.1.2.0 builds one as lsm, and a method is read the way
// the server reads an unquoted one, whatever its case.
func TestUniqueIndexMethodRefused(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		method  string
		want    bool
	}{
		{name: "postgres hash", dialect: platform.Postgres, method: "hash", want: true},
		{name: "postgres HASH with spaces", dialect: platform.Postgres, method: " HASH ", want: true},
		{name: "postgres gin", dialect: platform.Postgres, method: "gin", want: true},
		{name: "postgres btree", dialect: platform.Postgres, method: "btree"},
		{name: "postgres default", dialect: platform.Postgres},
		{name: "yugabytedb hash", dialect: platform.YugabyteDB, method: "hash"},
		{name: "yugabytedb ybgin", dialect: platform.YugabyteDB, method: "ybgin", want: true},
		{name: "cockroachdb gist", dialect: platform.CockroachDB, method: "gist", want: true},
		{name: "mysql HASH", dialect: platform.MySQL, method: "HASH"},
		{name: "a dialect nothing describes", dialect: "nosuchengine", method: "hash"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemaprep.UniqueIndexMethodRefused(test.dialect, test.method), qt.Equals, test.want)
		})
	}
}
