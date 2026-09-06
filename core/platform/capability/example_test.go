package capability_test

import (
	"fmt"
	"strings"

	"ptah.run/core/platform/capability"
)

// ExampleForDialect resolves the default capability preset for a dialect name
// and gates an emission with Has. Any spelling platform.NormalizeDialect
// accepts works ("postgresql" here). An unknown dialect gets nil — the
// conservative empty set — and reading it is safe: every Has answer on it is
// false, so a target Ptah does not model is treated as having nothing rather
// than everything.
func ExampleForDialect() {
	postgres := capability.ForDialect("postgresql")
	mysql := capability.ForDialect("mysql")

	fmt.Println("postgres concurrent index builds:", postgres.Has(capability.CreateIndexConcurrently))
	fmt.Println("mysql concurrent index builds:", mysql.Has(capability.CreateIndexConcurrently))

	unknown := capability.ForDialect("db2")
	fmt.Println("unknown dialect preset is nil:", unknown == nil)
	fmt.Println("unknown dialect concurrent index builds:", unknown.Has(capability.CreateIndexConcurrently))

	// Output:
	// postgres concurrent index builds: true
	// mysql concurrent index builds: false
	// unknown dialect preset is nil: true
	// unknown dialect concurrent index builds: false
}

// ExampleCapabilities_With composes a target description from a preset: start
// from the closest shipped preset, override the keys that differ, and Validate
// the result. With never mutates its receiver — it returns a copy — so the
// preset the set was derived from keeps answering for the unmodified target.
func ExampleCapabilities_With() {
	base := capability.Postgres17()
	restricted := base.With(capability.CreateIndexConcurrently, false)

	if err := restricted.Validate(); err != nil {
		fmt.Println("invalid set:", err)
		return
	}

	fmt.Println("restricted:", restricted.Has(capability.CreateIndexConcurrently))
	fmt.Println("base preset unchanged:", base.Has(capability.CreateIndexConcurrently))

	// Output:
	// restricted: false
	// base preset unchanged: true
}

// ExampleCapabilities_Validate shows the rule kinds Validate enforces on a
// hand-built set: every key must come from the curated registry (a typo fails
// fast instead of reading as "capability absent"), an enabled capability's
// requirements must be enabled too, a mutual-exclusion group admits at most
// one member, and enabling ForeignKeys demands exactly one foreign-key
// reference policy beside it. A refusal names the key or the pair at fault:
// report that text, branch on whether there is a refusal at all. Presets
// satisfy every rule, which is the last line here — hand-built sets are the
// ones to check.
func ExampleCapabilities_Validate() {
	typo := capability.Capabilities{"drop_index_if_exist": true}
	fmt.Println("unregistered key refused:", typo.Validate() != nil)

	orphan := capability.Capabilities{capability.DropConstraintIfExists: true}
	fmt.Println("unmet requirement refused:", orphan.Validate() != nil)

	bothEnumModes := capability.Capabilities{
		capability.EnumInlineColumn: true,
		capability.EnumCustomType:   true,
	}
	fmt.Println("mutually exclusive pair refused:", bothEnumModes.Validate() != nil)

	fmt.Println("shipped preset accepted:", capability.Postgres17().Validate() == nil)

	// Output:
	// unregistered key refused: true
	// unmet requirement refused: true
	// mutually exclusive pair refused: true
	// shipped preset accepted: true
}

// ExampleCapabilities_Established separates "decided false" from "never
// answered". A preset fills every registry key, so a false read out of one was
// decided and Established still reports true; the nil set an unknown dialect
// receives holds no answer at all, and reading that silence through Has alone
// would turn "nobody established this" into "the target does not have it".
func ExampleCapabilities_Established() {
	spanner := capability.ForDialect("spanner")
	unknown := capability.ForDialect("db2")

	key := capability.CreateIndexConcurrently
	fmt.Printf("spanner: has=%t established=%t\n", spanner.Has(key), spanner.Established(key))
	fmt.Printf("unknown: has=%t established=%t\n", unknown.Has(key), unknown.Established(key))

	// Output:
	// spanner: has=false established=true
	// unknown: has=false established=false
}

// ExampleResolveServerVersion maps a live server version string — typically
// the result of SELECT version() — onto a capability preset and reports how
// the mapping went. The MariaDB banner arrives on the mysql dialect the way a
// live connection delivers it: the banner outranks the declared dialect, and
// ResolvedDialect names the ladder the capabilities actually came from. The
// last resolution is the operator-input contract: a string nothing could be
// read from reports Recognized false, and a caller holding a version a person
// typed refuses it instead of accepting the silent dialect-default fallback.
func ExampleResolveServerVersion() {
	res := capability.ResolveServerVersion("postgres", "PostgreSQL 16.3 (Debian 16.3-1.pgdg120+1) on x86_64-pc-linux-gnu")
	fmt.Println("dialect:", res.ResolvedDialect)
	fmt.Println("version-specific:", res.VersionSpecific)
	fmt.Println("set expression on generated columns:", res.Capabilities.Has(capability.AlterGeneratedColumnExpression))

	res = capability.ResolveServerVersion("mysql", "5.5.5-10.11.6-MariaDB-1:10.11.6+maria~ubu2204")
	fmt.Println("dialect:", res.ResolvedDialect)
	fmt.Println("drop index if exists:", res.Capabilities.Has(capability.DropIndexIfExists))

	res = capability.ResolveServerVersion("postgres", "latest")
	fmt.Println("recognized:", res.Recognized)

	// Output:
	// dialect: postgres
	// version-specific: true
	// set expression on generated columns: false
	// dialect: mariadb
	// drop index if exists: true
	// recognized: false
}

// ExampleIdentifiers reads the identifier limit Ptah models for a dialect and
// applies it with Exceeds. The unit is the point: PostgreSQL truncates at 63
// bytes while MySQL counts characters, so one 32-character multibyte name
// overflows the first limit and fits the second. Exceeds applies the
// byte-versus-rune rule itself, so callers never compare Max to a length they
// measured on their own.
func ExampleIdentifiers() {
	fmt.Println("postgres:", capability.Identifiers("postgres"))
	fmt.Println("sqlserver:", capability.Identifiers("sqlserver"))
	fmt.Println("sqlite:", capability.Identifiers("sqlite"))

	name := strings.Repeat("é", 32) // 32 characters, 64 UTF-8 bytes
	fmt.Println("postgres exceeds:", capability.Identifiers("postgres").Exceeds(name))
	fmt.Println("mysql exceeds:", capability.Identifiers("mysql").Exceeds(name))

	// Output:
	// postgres: 63 bytes
	// sqlserver: 128 characters
	// sqlite: unlimited
	// postgres exceeds: true
	// mysql exceeds: false
}
