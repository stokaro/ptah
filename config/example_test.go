package config_test

import (
	"fmt"

	"ptah.run/config"
)

// ExampleDefaultCompareOptions shows what the defaults actually contain: the
// ignore list holds exactly plpgsql, and every other field stays at its zero
// value. This is the starting point for an embedder who wants Ptah's stock
// comparison behavior against a PostgreSQL database.
func ExampleDefaultCompareOptions() {
	opts := config.DefaultCompareOptions()

	fmt.Println(opts.IgnoredExtensions)

	// Output:
	// [plpgsql]
}

// ExampleWithIgnoredExtensions replaces the ignore list outright: the
// arguments are the whole list, and the default plpgsql entry is gone unless
// it is spelled again. Reach for this constructor to take full control of
// which extensions the comparison skips.
func ExampleWithIgnoredExtensions() {
	opts := config.WithIgnoredExtensions("adminpack", "pg_stat_statements")

	fmt.Println(opts.IgnoredExtensions)
	fmt.Println(opts.IsExtensionIgnored("plpgsql"))

	// Output:
	// [adminpack pg_stat_statements]
	// false
}

// ExampleWithAdditionalIgnoredExtensions extends the default ignore list
// instead of replacing it: plpgsql stays first and the arguments follow. This
// is the usual constructor for a database with more pre-installed extensions
// than the default accounts for.
func ExampleWithAdditionalIgnoredExtensions() {
	opts := config.WithAdditionalIgnoredExtensions("adminpack", "pg_stat_statements")

	fmt.Println(opts.IgnoredExtensions)

	// Output:
	// [plpgsql adminpack pg_stat_statements]
}

// ExampleCompareOptions_IsExtensionIgnored checks membership in the configured
// ignore list. The match is exact and case-sensitive, so a name spelled in a
// different case is not ignored.
func ExampleCompareOptions_IsExtensionIgnored() {
	opts := config.WithAdditionalIgnoredExtensions("adminpack")

	fmt.Println(opts.IsExtensionIgnored("plpgsql"))
	fmt.Println(opts.IsExtensionIgnored("PLPGSQL"))
	fmt.Println(opts.IsExtensionIgnored("pg_trgm"))

	// Output:
	// true
	// false
	// false
}
