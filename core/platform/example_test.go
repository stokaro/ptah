package platform_test

import (
	"fmt"

	"ptah.run/core/platform"
)

// ExampleNormalizeDialect folds several spellings of the same targets onto the
// canonical constants the rest of Ptah compares against. The empty answer for
// an unknown name is the part to check: passing it on asks every layer below
// to render for a target nobody implemented.
func ExampleNormalizeDialect() {
	for _, spelling := range []string{"PostgreSQL", "pgx", "crdb", "libsql", "duckdb"} {
		fmt.Printf("%q -> %q\n", spelling, platform.NormalizeDialect(spelling))
	}

	// Output:
	// "PostgreSQL" -> "postgres"
	// "pgx" -> "postgres"
	// "crdb" -> "cockroachdb"
	// "libsql" -> "sqlite"
	// "duckdb" -> ""
}

// ExampleIsPostgresFamily shows which dialects share the PostgreSQL wire
// protocol and catalog -- the question this predicate answers. CockroachDB,
// YugabyteDB, and Spanner are in the family even though each refuses
// constructs PostgreSQL accepts, so a caller deciding whether a feature
// exists reads a capability, not this. Aliases work because the predicate
// normalizes its argument first.
func ExampleIsPostgresFamily() {
	for _, dialect := range []string{"postgres", "crdb", "yugabyte", "spanner", "mysql"} {
		fmt.Printf("%s: %t\n", dialect, platform.IsPostgresFamily(dialect))
	}

	// Output:
	// postgres: true
	// crdb: true
	// yugabyte: true
	// spanner: true
	// mysql: false
}

// ExampleLookup reads what a dialect states about itself instead of
// comparing its name: the family it belongs to and how its statements mark a
// bound parameter. Any accepted spelling selects the descriptor.
func ExampleLookup() {
	for _, spelling := range []string{"crdb", "mssql", "ydbs"} {
		described, found := platform.Lookup(spelling)
		fmt.Printf("%s: found=%t name=%s family=%q placeholder=%s\n", spelling, found, described.Name, described.Family, described.Placeholder)
	}

	// Output:
	// crdb: found=true name=cockroachdb family="postgres" placeholder=dollar
	// mssql: found=true name=sqlserver family="" placeholder=at-p
	// ydbs: found=true name=ydb family="" placeholder=dollar-named
}
