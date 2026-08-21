package atlasurl_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlasurl"
)

// normalizeDialectSource is the file that decides which dialect spellings Ptah
// accepts anywhere. The list below is read out of it rather than copied here,
// which is the whole point: this test exists because a copy of that list lived
// in DialectFromURL and drifted by fifteen spellings.
//
// internal/lintdialect and internal/convert/fromschema read the same switch the
// same way, for the same reason.
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
	c.Assert(len(spellings) > 9, qt.IsTrue,
		qt.Commentf("extracted %d spellings, so the sweep below compares nothing", len(spellings)))
	return spellings
}

// TestDialectFromURL_AcceptsEveryAcceptedSpelling is the sweep the drift
// survived.
//
// A hand-written scheme list here omitted fifteen spellings Ptah accepts
// everywhere else, two of them canonical dialect names: `oracle://` and
// `spanner://` were refused as a dev URL while `--dialect oracle` and
// `--dialect spanner` both rendered. The other thirteen were documented aliases
// -- `crdb`, `ch`, `pgx`, `tsql`, `ysql`, `sql-server`, `cloudspanner` and the
// rest -- accepted by every other boundary and refused by this one
// (stokaro/ptah#1875).
func TestDialectFromURL_AcceptsEveryAcceptedSpelling(t *testing.T) {
	c := qt.New(t)

	usable, unusable := partitionBySchemeLegality(acceptedSpellings(c))
	c.Assert(len(usable) > 20, qt.IsTrue, qt.Commentf("only %d spellings swept", len(usable)))

	for _, spelling := range usable {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			dialect, err := atlasurl.DialectFromURL(spelling + "://localhost/dev")

			c.Assert(err, qt.IsNil)
			c.Assert(dialect, qt.Equals, platform.NormalizeDialect(spelling))
			c.Assert(dialect, qt.Not(qt.Equals), "")
		})
	}

	// The other side of the partition, asserted rather than skipped.
	c.Assert(unusable, qt.DeepEquals, []string{"google_spanner", "sql_server"})
}

// TestDialectFromURL_RefusesASpellingNoURLCanCarry holds the two spellings that
// are legal --dialect values and cannot be URL schemes.
//
// A URL scheme may carry letters, digits, `+`, `-` and `.` and nothing else, so
// Go's parser never reaches a scheme in `sql_server://…` and reads the whole
// string as a path. The refusal is about the URL grammar rather than about the
// dialect, and pinning the parser's own message is what says which.
func TestDialectFromURL_RefusesASpellingNoURLCanCarry(t *testing.T) {
	c := qt.New(t)

	_, unusable := partitionBySchemeLegality(acceptedSpellings(c))

	for _, spelling := range unusable {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			dialect, err := atlasurl.DialectFromURL(spelling + "://localhost/dev")

			c.Assert(err, qt.ErrorMatches, `parse --dev-url: .*first path segment in URL cannot contain colon`)
			c.Assert(dialect, qt.Equals, "")
			// The control: the spelling itself is a dialect Ptah accepts, so
			// the refusal above is not about the name.
			c.Assert(platform.NormalizeDialect(spelling), qt.Not(qt.Equals), "")
		})
	}
}

// partitionBySchemeLegality splits accepted spellings into those a URL scheme
// can carry and those it cannot.
func partitionBySchemeLegality(spellings []string) (usable, unusable []string) {
	for _, spelling := range spellings {
		if strings.Contains(spelling, "_") {
			unusable = append(unusable, spelling)
			continue
		}
		usable = append(usable, spelling)
	}
	return usable, unusable
}

// TestDialectFromURL_RefusesASchemeNamingNoDialect is the negative control: the
// sweep above would pass against a function that accepted everything.
func TestDialectFromURL_RefusesASchemeNamingNoDialect(t *testing.T) {
	tests := []string{"db2", "informix", "firebird", "notadriver"}

	for _, scheme := range tests {
		t.Run(scheme, func(t *testing.T) {
			c := qt.New(t)

			dialect, err := atlasurl.DialectFromURL(scheme + "://localhost/dev")

			c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect ".*"`)
			c.Assert(dialect, qt.Equals, "")
		})
	}
}
