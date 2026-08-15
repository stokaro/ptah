package lintdialect_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lintdialect"
	"go.5x5.cz/ptah/migration/lint"
)

// normalizeDialectSource is the file that decides which dialect spellings ptah
// accepts anywhere. The spelling list below is read out of it rather than
// copied here, so a spelling added to the switch is covered by these tests
// without anyone editing this file.
//
// That property is the whole point for this package: lintdialect accepted a
// hand-maintained list of nine canonical names while the rest of the tree took
// twenty-four spellings, and a copied list is exactly what let the two drift
// apart unnoticed (stokaro/ptah#270).
//
// internal/convert/fromschema/dialect_spelling_test.go reads the same switch the
// same way. The extraction is duplicated rather than shared because the only
// place to share it from would be a non-test package exported solely to be read
// by tests.
const normalizeDialectSource = "../../core/platform/constants.go"

// quotedLiteral deliberately requires a non-empty literal: the switch's default
// arm returns "", which is the one string in the body that is not a spelling.
var quotedLiteral = regexp.MustCompile(`"([^"]+)"`)

// acceptedSpellings returns every dialect spelling that appears as a case in
// platform.NormalizeDialect's switch, read from the switch body itself.
func acceptedSpellings(tb testing.TB) []string {
	c := qt.New(tb)
	source, err := os.ReadFile(normalizeDialectSource)
	c.Assert(err, qt.IsNil)

	_, afterSignature, foundSignature := strings.Cut(string(source), "func NormalizeDialect(dialect string) string {")
	c.Assert(foundSignature, qt.IsTrue, qt.Commentf("NormalizeDialect signature moved in %s", normalizeDialectSource))

	body, _, foundEnd := strings.Cut(afterSignature, "\n}")
	c.Assert(foundEnd, qt.IsTrue, qt.Commentf("NormalizeDialect body is unterminated in %s", normalizeDialectSource))

	matches := quotedLiteral.FindAllStringSubmatch(body, -1)
	spellings := make([]string, 0, len(matches))
	for _, match := range matches {
		spellings = append(spellings, match[1])
	}
	slices.Sort(spellings)
	return slices.Compact(spellings)
}

// canonicalDialects returns the distinct canonical names the accepted spellings
// resolve to -- one per supported engine.
func canonicalDialects(tb testing.TB) []string {
	c := qt.New(tb)
	spellings := acceptedSpellings(c.TB)
	canonicals := make([]string, 0, len(spellings))
	for _, spelling := range spellings {
		canonicals = append(canonicals, platform.NormalizeDialect(spelling))
	}
	slices.Sort(canonicals)
	return slices.Compact(canonicals)
}

// familyMembers groups the canonical dialects by the family lintdialect.Family
// assigns them to.
func familyMembers(tb testing.TB) map[string][]string {
	c := qt.New(tb)
	members := make(map[string][]string)
	for _, canonical := range canonicalDialects(c.TB) {
		family := lintdialect.Family(canonical)
		members[family] = append(members[family], canonical)
	}
	return members
}

func assertCompatible(c *qt.C, compatible bool) {
	c.Assert(compatible, qt.IsTrue)
}

func assertIncompatible(c *qt.C, compatible bool) {
	c.Assert(compatible, qt.IsFalse)
}

// TestAcceptedSpellings_ExtractionControls proves the spelling list the sweeps
// below iterate is really the switch's own list.
//
// Reverting the extraction -- a renamed signature, a regexp that stops matching
// -- leaves acceptedSpellings empty, and an empty list makes every exhaustive
// test below pass while comparing nothing.
func TestAcceptedSpellings_ExtractionControls(t *testing.T) {
	c := qt.New(t)

	spellings := acceptedSpellings(c.TB)

	// Positive control: aliases that exist only inside the switch, one per
	// engine family that has one.
	for _, alias := range []string{"pgx", "postgresql", "ch", "sqlite3", "mssql", "tsql", "sql-server", "crdb", "cockroach", "ysql", "yugabyte", "cloudspanner", "google_spanner", "google-spanner", "sql_server"} {
		c.Assert(spellings, qt.Contains, alias)
	}
	// Positive control: every canonical name is a case of its own switch.
	for _, canonical := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	} {
		c.Assert(spellings, qt.Contains, canonical)
	}
	// Negative control: the extractor must not reach past the switch body.
	for _, spelling := range spellings {
		c.Assert(platform.NormalizeDialect(spelling), qt.Not(qt.Equals), "", qt.Commentf("collected %q, which is not an accepted spelling", spelling))
	}
	// The engine count is what the exhaustive sweeps below depend on.
	c.Assert(canonicalDialects(c.TB), qt.HasLen, 9)
}

// TestValid_HappyPath_AcceptsEveryAcceptedSpelling is the exhaustive alias
// coverage: one subtest per spelling platform.NormalizeDialect knows.
//
// Before stokaro/ptah#270 this package accepted only the nine canonical names,
// so every one of the fifteen aliases below was refused as an "unsupported lint
// dialect" while `ptah sql lint`, `--dev-url` inference and the renderer all
// took it.
func TestValid_HappyPath_AcceptsEveryAcceptedSpelling(t *testing.T) {
	c := qt.New(t)

	for _, spelling := range acceptedSpellings(c.TB) {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintdialect.Valid(spelling), qt.IsTrue)
		})
	}
}

// TestCanonical_HappyPath_ResolvesEverySpellingToItsEngine is the half of the
// fix that accepting aliases would be unsafe without.
//
// migration/lint matches Rule.Dialects and picks its lexer mode by exact string
// comparison and validates neither, so a spelling that reached the engine
// unresolved would not fail -- it would silently select no dialect-specific
// rule. Each spelling must therefore come back as a canonical name, and
// resolving an already-canonical name must be a no-op.
func TestCanonical_HappyPath_ResolvesEverySpellingToItsEngine(t *testing.T) {
	c := qt.New(t)

	canonicals := canonicalDialects(c.TB)

	for _, spelling := range acceptedSpellings(c.TB) {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			canonical, ok := lintdialect.Canonical(spelling)

			c.Assert(ok, qt.IsTrue)
			c.Assert(canonicals, qt.Contains, canonical)

			idempotent, ok := lintdialect.Canonical(canonical)
			c.Assert(ok, qt.IsTrue)
			c.Assert(idempotent, qt.Equals, canonical)
		})
	}
}

// TestCanonical_HappyPath_EmptyDialectResolvesToItself pins the one supported
// value that is not an engine: the empty dialect means "run every
// dialect-independent rule".
func TestCanonical_HappyPath_EmptyDialectResolvesToItself(t *testing.T) {
	c := qt.New(t)

	canonical, ok := lintdialect.Canonical("")

	c.Assert(ok, qt.IsTrue)
	c.Assert(canonical, qt.Equals, "")
	c.Assert(lintdialect.Valid(""), qt.IsTrue)
}

func TestValid_FailurePath(t *testing.T) {
	for _, dialect := range []string{"oracle", "db2", "postgres!", "post gres", " ", "sqlserver2022"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintdialect.Valid(dialect), qt.IsFalse)
			canonical, ok := lintdialect.Canonical(dialect)
			c.Assert(ok, qt.IsFalse)
			c.Assert(canonical, qt.Equals, "")
		})
	}
}

// TestCompatible_HappyPath_EverySpellingMatchesItsOwnEngine is the exhaustive
// coverage for the comparison itself: one row per spelling.
//
// This is the sweep a raw `!=` comparison fails. It reddens for all fifteen
// aliases under the pre-fix gate, which compared the policy's spelling against
// the canonical name the wire reports.
func TestCompatible_HappyPath_EverySpellingMatchesItsOwnEngine(t *testing.T) {
	c := qt.New(t)

	for _, spelling := range acceptedSpellings(c.TB) {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintdialect.Compatible(spelling, platform.NormalizeDialect(spelling)), qt.IsTrue)
		})
	}
}

func TestCompatible(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		policy   string
		database string
		assert   func(c *qt.C, compatible bool)
	}{
		{
			name:     "an alias matches its own canonical name",
			policy:   "pgx",
			database: platform.Postgres,
			assert:   assertCompatible,
		},
		{
			name:     "two aliases of one engine match each other",
			policy:   "mssql",
			database: "tsql",
			assert:   assertCompatible,
		},
		{
			name:     "MySQL and MariaDB share a family",
			policy:   platform.MySQL,
			database: platform.MariaDB,
			assert:   assertCompatible,
		},
		{
			name:     "MariaDB and MySQL share a family in the other direction",
			policy:   platform.MariaDB,
			database: platform.MySQL,
			assert:   assertCompatible,
		},
		{
			name:     "CockroachDB rides the PostgreSQL family",
			policy:   platform.Postgres,
			database: platform.CockroachDB,
			assert:   assertCompatible,
		},
		{
			name:     "YugabyteDB rides the PostgreSQL family",
			policy:   platform.Postgres,
			database: platform.YugabyteDB,
			assert:   assertCompatible,
		},
		{
			name:     "Spanner rides the PostgreSQL family",
			policy:   platform.Spanner,
			database: platform.Postgres,
			assert:   assertCompatible,
		},
		{
			name:     "an empty policy asserts nothing",
			policy:   "",
			database: platform.MariaDB,
			assert:   assertCompatible,
		},
		{
			name:     "an empty policy asserts nothing even about an unresolvable database",
			policy:   "",
			database: "oracle",
			assert:   assertCompatible,
		},
		{
			name:     "an unknown database dialect constrains nothing",
			policy:   platform.Postgres,
			database: "",
			assert:   assertCompatible,
		},
		{
			name:     "MySQL and PostgreSQL are different families",
			policy:   platform.MySQL,
			database: platform.Postgres,
			assert:   assertIncompatible,
		},
		{
			name:     "MariaDB and PostgreSQL are different families",
			policy:   platform.MariaDB,
			database: platform.Postgres,
			assert:   assertIncompatible,
		},
		{
			name:     "SQLite stands alone",
			policy:   platform.SQLite,
			database: platform.MySQL,
			assert:   assertIncompatible,
		},
		{
			name:     "SQL Server stands alone",
			policy:   platform.SQLServer,
			database: platform.Postgres,
			assert:   assertIncompatible,
		},
		{
			name:     "ClickHouse stands alone",
			policy:   platform.ClickHouse,
			database: platform.MySQL,
			assert:   assertIncompatible,
		},
		{
			name:     "an unsupported policy dialect is never compatible",
			policy:   "oracle",
			database: platform.Postgres,
			assert:   assertIncompatible,
		},
		{
			name:     "a nonempty policy is not compatible with an unresolvable database",
			policy:   platform.Postgres,
			database: "oracle",
			assert:   assertIncompatible,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			test.assert(c, lintdialect.Compatible(test.policy, test.database))
		})
	}
}

// ruleDialectCoverage is how much of one family a single rule names.
type ruleDialectCoverage struct {
	rule   string
	family string
	named  []string
	want   []string
}

// ruleFamilyCoverage measures every built-in rule against every lint family.
func ruleFamilyCoverage(tb testing.TB) []ruleDialectCoverage {
	c := qt.New(tb)
	members := familyMembers(c.TB)
	rules := lint.Rules()
	c.Assert(len(rules) > 0, qt.IsTrue, qt.Commentf("no built-in rules to measure"))

	coverages := make([]ruleDialectCoverage, 0, len(rules)*len(members))
	for _, rule := range rules {
		for family, want := range members {
			named := slices.DeleteFunc(slices.Clone(want), func(member string) bool {
				return !slices.Contains(rule.Dialects, member)
			})
			coverages = append(coverages, ruleDialectCoverage{rule: rule.Code, family: family, named: named, want: want})
		}
	}
	return coverages
}

// TestBuiltInRules_NoRuleSplitsTheMySQLFamily is the measurement that justifies
// treating "mysql" and "mariadb" as interchangeable in a lint policy.
//
// Every built-in MySQL-family rule names both, and lint's scanner mode treats
// the two identically, so the names select an identical analysis and refusing a
// policy that declares one while the wire reports the other diagnoses nothing.
// If a rule ever names one and not the other that premise is gone, and this
// test says which rule rather than letting Compatible widen in silence.
func TestBuiltInRules_NoRuleSplitsTheMySQLFamily(t *testing.T) {
	c := qt.New(t)

	mysqlFamily := slices.DeleteFunc(ruleFamilyCoverage(c.TB), func(cov ruleDialectCoverage) bool {
		return cov.family != platform.MySQL
	})

	// Non-vacuity control: at least one rule has to name the whole family, or
	// the comparison below would hold no matter how Family grouped things.
	namesTheWholeFamily := slices.ContainsFunc(mysqlFamily, func(cov ruleDialectCoverage) bool {
		return len(cov.want) > 1 && len(cov.named) == len(cov.want)
	})
	c.Assert(namesTheWholeFamily, qt.IsTrue, qt.Commentf("no rule names the whole MySQL family, so this guard compares nothing"))

	split := slices.DeleteFunc(mysqlFamily, func(cov ruleDialectCoverage) bool {
		return len(cov.named) == 0 || len(cov.named) == len(cov.want)
	})

	c.Assert(split, qt.HasLen, 0, qt.Commentf("these rules name part of the MySQL family, so mysql and mariadb are no longer interchangeable"))
}

// TestBuiltInRules_PostgresFamilyRulesNameOnlyPostgres pins the asymmetry that
// makes grouping the PostgreSQL family a policy decision rather than a
// consequence of the rule table.
//
// Unlike the MySQL family, the PostgreSQL family's members do NOT select the
// same rule set: every PG and TX rule names the literal "postgres", and no rule
// names cockroachdb, yugabytedb or spanner at all. So a CockroachDB database
// runs the dialect-independent families only -- which is true whether or not a
// policy file exists, because what gets linted is the dialect the wire reports.
//
// lintdialect.Compatible still accepts `dialect: postgres` against those
// databases, on the separate ground that they share PostgreSQL's wire protocol
// and planner family and that the declaration is an honest description of the
// target. This test exists so that ground stays visible: if a rule is ever
// added for cockroachdb, yugabytedb or spanner specifically, the members stop
// being describable by one name and the grouping has to be revisited.
func TestBuiltInRules_PostgresFamilyRulesNameOnlyPostgres(t *testing.T) {
	c := qt.New(t)

	postgresFamily := slices.DeleteFunc(ruleFamilyCoverage(c.TB), func(cov ruleDialectCoverage) bool {
		return cov.family != platform.Postgres
	})

	// Non-vacuity control: some rule must name postgres, or the assertion
	// below would hold on an empty rule table.
	namesPostgres := slices.ContainsFunc(postgresFamily, func(cov ruleDialectCoverage) bool {
		return slices.Contains(cov.named, platform.Postgres)
	})
	c.Assert(namesPostgres, qt.IsTrue, qt.Commentf("no rule names postgres, so this guard compares nothing"))

	beyondPostgres := slices.DeleteFunc(postgresFamily, func(cov ruleDialectCoverage) bool {
		named := slices.DeleteFunc(slices.Clone(cov.named), func(member string) bool {
			return member == platform.Postgres
		})
		return len(named) == 0
	})

	c.Assert(beyondPostgres, qt.HasLen, 0, qt.Commentf("a rule now names a PostgreSQL-family member other than postgres; revisit lintdialect.Family"))
}
