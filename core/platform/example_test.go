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
