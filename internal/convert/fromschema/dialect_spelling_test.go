package fromschema_test

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// normalizeDialectSource is the file that decides which dialect spellings ptah
// accepts. The spelling list below is read out of it rather than copied here, so
// a spelling added to the switch is covered by this test without anyone editing
// this file.
const normalizeDialectSource = "../../../core/platform/constants.go"

// quotedLiteral deliberately requires a non-empty literal: the switch's default
// arm returns "", which is the one string in the body that is not a spelling.
var quotedLiteral = regexp.MustCompile(`"([^"]+)"`)

// acceptedSpellings returns every dialect spelling that appears as a case in
// platform.NormalizeDialect's switch, read from the switch body itself.
//
// The body holds no string literal other than the case spellings: the argument
// is lowercased through strings helpers and every return is a named constant.
func acceptedSpellings(c *qt.C) []string {
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

// convertedStatements is the conversion this test compares: the AST that
// fromschema builds for a dialect spelling, rendered to SQL. A render error is
// folded into the compared string instead of failing the test, so a dialect that
// refuses part of the fixture still contributes a value both spellings of that
// engine must agree on.
func convertedStatements(database goschema.Database, dialect string) []string {
	nodes := fromschema.FromDatabase(database, dialect)
	rendered := make([]string, 0, len(nodes.Statements))
	for _, node := range nodes.Statements {
		sql, err := renderer.RenderSQL(dialect, node)
		rendered = append(rendered, fmt.Sprintf("%s | err=%v", sql, err))
	}
	return rendered
}

func spellingFixture(c *qt.C) goschema.Database {
	database, err := goschema.ParseDir("testdata/dialectspellings")
	c.Assert(err, qt.IsNil)
	c.Assert(database, qt.IsNotNil)
	return *database
}

// TestAcceptedSpellings_ExtractionControls proves the spelling list the parity
// test iterates is really the switch's own list.
//
// Reverting the extraction (a renamed signature, a regexp that stops matching)
// leaves acceptedSpellings empty, and an empty list makes the parity test below
// pass while comparing nothing. This test prints the missing spelling name.
func TestAcceptedSpellings_ExtractionControls(t *testing.T) {
	c := qt.New(t)

	spellings := acceptedSpellings(c)

	// Positive control: aliases that exist only inside the switch, one per
	// engine family that has one.
	for _, alias := range []string{"pgx", "ch", "sqlite3", "tsql", "sql-server", "crdb", "ysql", "google_spanner"} {
		c.Assert(spellings, qt.Contains, alias)
	}
	// Positive control: every canonical name is a case of its own switch.
	for _, canonical := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	} {
		c.Assert(spellings, qt.Contains, canonical)
	}
	// Negative control: the extractor must not reach past the switch body. Every
	// literal it collected has to be a spelling NormalizeDialect actually
	// accepts, so a comment word or a neighboring function's literal fails here.
	for _, spelling := range spellings {
		c.Assert(platform.NormalizeDialect(spelling), qt.Not(qt.Equals), "", qt.Commentf("collected %q, which is not an accepted spelling", spelling))
	}
}

// TestFromDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName is the
// completion criterion for stokaro/ptah#929 workstream A: one dialect spelling,
// one answer.
//
// If the normalization is reverted — isPostgreSQLPlatform back to its two-string
// form, handleEnumTypes back to comparing the raw name, applyPlatformOverrides
// back to indexing Overrides by the raw name — this fails with the list of
// spellings that disagree with their own canonical name, so the failure is a
// count of engines-times-predicates rather than a single boolean. On the
// measured baseline that list held all 15 non-canonical spellings.
func TestFromDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c)
	spellings := acceptedSpellings(c)

	divergent := slices.DeleteFunc(slices.Clone(spellings), func(spelling string) bool {
		return slices.Equal(
			convertedStatements(database, spelling),
			convertedStatements(database, platform.NormalizeDialect(spelling)),
		)
	})

	c.Assert(divergent, qt.HasLen, 0, qt.Commentf("these spellings convert differently from their own canonical name"))
}

// TestFromDatabase_FixtureDiscriminatesEngines is the negative control for the
// parity test above.
//
// The parity comparison is only meaningful if the fixture can tell engines
// apart at all. A fixture that rendered to the same statements everywhere would
// make TestFromDatabase_EveryAcceptedSpellingConvertsLikeItsCanonicalName pass
// no matter what the predicates did. If the fixture is emptied or its
// per-engine overrides are deleted, this fails naming the two engines that
// collided.
func TestFromDatabase_FixtureDiscriminatesEngines(t *testing.T) {
	c := qt.New(t)

	database := spellingFixture(c)
	canonicals := []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	}

	fingerprints := make(map[string]string, len(canonicals))
	for _, canonical := range canonicals {
		fingerprints[strings.Join(convertedStatements(database, canonical), "\n")] = canonical
	}

	c.Assert(fingerprints, qt.HasLen, len(canonicals))
}
