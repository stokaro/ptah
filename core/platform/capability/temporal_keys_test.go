package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// WITHOUT OVERLAPS and PERIOD arrived in PostgreSQL 18. PostgreSQL 17.x answers
// a syntax error at or near "WITHOUT" to the key, and 18.6 builds it and sets
// pg_constraint.conperiod (stokaro/ptah#6330). The table pins both sides of the
// boundary through the resolver an operator reaches by banner.
func TestPostgresTemporalKeysArriveIn18(t *testing.T) {
	tests := []struct {
		name   string
		banner string
		want   bool
	}{
		{name: "14 is before the keys", banner: "14.13", want: false},
		{name: "16 is before the keys", banner: "16.4", want: false},
		{name: "17 is the line that answers a syntax error", banner: "17.11", want: false},
		{name: "18 is the release that grew them", banner: "18.0", want: true},
		{name: "18.6 is the release the keys were measured on", banner: "18.6", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			resolved := capability.ResolveServerVersion(platform.Postgres, test.banner)

			c.Assert(resolved.Capabilities.Has(capability.TemporalKeys), qt.Equals, test.want)
		})
	}
}

// No other dialect takes the clauses. The PostgreSQL renderer serves
// CockroachDB and YugabyteDB, so the answer cannot be the family's: each is
// refused through the key, not through a dialect name.
func TestTemporalKeysAreAbsentFromEveryOtherDialect(t *testing.T) {
	dialects := []string{
		platform.CockroachDB, platform.YugabyteDB, platform.MySQL, platform.MariaDB,
		platform.SQLite, platform.SQLServer, platform.Oracle, platform.ClickHouse,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(capability.ForDialect(dialect).Has(capability.TemporalKeys), qt.IsFalse)
		})
	}
}
