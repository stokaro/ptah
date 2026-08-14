package lint_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/migration/lint"
)

// This file measures the half of stokaro/ptah#270's fix that accepting dialect
// aliases would be unsafe without.
//
// Accepting `dialect: pgx` is not enough, because nothing downstream resolves
// it: lint matches Rule.Dialects and picks its lexer mode by exact string
// comparison and ValidateOptions never looks at Dialect at all. A policy whose
// spelling survived parsing would run clean, report no PostgreSQL finding, and
// exit 0 -- a silently narrower analysis rather than an error. So the reader
// has to rewrite what it keeps, and that is what these tests pin.

// normalizeDialectSource is read rather than copied so a spelling added to
// platform.NormalizeDialect is covered here without anyone editing this file.
// internal/lintdialect/dialect_test.go and
// internal/convert/fromschema/dialect_spelling_test.go read the same switch for
// the same reason.
const normalizeDialectSource = "../../core/platform/constants.go"

var quotedLiteral = regexp.MustCompile(`"([^"]+)"`)

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
	spellings = slices.Compact(spellings)
	c.Assert(len(spellings) > 9, qt.IsTrue, qt.Commentf("extracted %d spellings, so the sweeps below cover no aliases", len(spellings)))
	return spellings
}

// dialectFixture is a directory whose up migration adds an index to a table it
// did not create. That is PG101, a PostgreSQL-only rule, so the findings this
// directory produces depend on the dialect the analysis runs under -- which is
// what makes the parity comparison below meaningful.
func dialectFixture(policyDialect string) fstest.MapFS {
	return fstest.MapFS{
		lint.ConfigFileName: {Data: []byte("dialect: " + policyDialect + "\n")},
		"0000000001_add_index.up.sql": {
			Data: []byte("CREATE INDEX idx_users_name ON users (name);\n"),
		},
		"0000000001_add_index.down.sql": {
			Data: []byte("DROP INDEX idx_users_name;\n"),
		},
	}
}

// findingCodes lints the fixture under the dialect its committed policy
// resolves to, and returns the sorted rule codes. Going through LoadConfigFS is
// the point: the canonicalization under test lives in the reader, not in the
// engine.
func findingCodes(c *qt.C, policyDialect string) []string {
	fsys := dialectFixture(policyDialect)

	cfg, err := lint.LoadConfigFS(fsys, lint.ConfigFileName)
	c.Assert(err, qt.IsNil)

	findings, err := lint.LintFS(fsys, lint.Options{Dialect: cfg.Dialect})
	c.Assert(err, qt.IsNil)

	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

// TestLoadConfigFS_HappyPath_CanonicalizesEveryAcceptedSpelling is the
// exhaustive coverage: one row per spelling platform.NormalizeDialect knows.
func TestLoadConfigFS_HappyPath_CanonicalizesEveryAcceptedSpelling(t *testing.T) {
	c := qt.New(t)

	for _, spelling := range acceptedSpellings(c) {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := lint.LoadConfigFS(dialectFixture(spelling), lint.ConfigFileName)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Dialect, qt.Equals, platform.NormalizeDialect(spelling))
		})
	}
}

// TestLintFS_HappyPath_FixtureDiscriminatesDialects is the control the parity
// test below is worthless without.
//
// If the fixture produced the same findings under every dialect, comparing an
// alias against its canonical name would hold no matter what the reader did
// with the spelling. Measured: postgres raises PG101 here and mysql does not.
func TestLintFS_HappyPath_FixtureDiscriminatesDialects(t *testing.T) {
	c := qt.New(t)

	c.Assert(findingCodes(c, platform.Postgres), qt.Contains, "PG101")
	c.Assert(findingCodes(c, platform.MySQL), qt.Not(qt.Contains), "PG101")
}

// TestLintFS_HappyPath_EverySpellingLintsLikeItsCanonicalName is the parity
// comparison: a policy naming an engine by any accepted spelling must produce
// exactly the findings the canonical name produces.
//
// Under a reader that validates the spelling without resolving it, every alias
// of postgres loses PG101 and this test names them.
func TestLintFS_HappyPath_EverySpellingLintsLikeItsCanonicalName(t *testing.T) {
	c := qt.New(t)

	for _, spelling := range acceptedSpellings(c) {
		c.Run(spelling, func(c *qt.C) {
			c.Assert(findingCodes(c, spelling), qt.DeepEquals, findingCodes(c, platform.NormalizeDialect(spelling)))
		})
	}
}
