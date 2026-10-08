package atlasurl_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/atlasurl"
)

// acceptedSpellings reads the declaration used by built-in normalization and
// registration, so adding an alias automatically extends these sweeps.
func acceptedSpellings(c *qt.C) []string {
	spellings := platform.DialectSpellings()
	slices.Sort(spellings)
	c.Assert(len(spellings) > 9, qt.IsTrue,
		qt.Commentf("only %d spellings, so the sweep is incomplete", len(spellings)))
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

// TestDialectFromURL_AnswersTheDriverOfAnImageURL reads the dialect of a
// `docker+<driver>://` dev URL off its driver, before anything is started, as
// it reads a `docker://` URL off its engine (stokaro/ptah#4040).
func TestDialectFromURL_AnswersTheDriverOfAnImageURL(t *testing.T) {
	tests := []struct {
		rawURL string
		want   string
	}{
		{rawURL: "docker+postgres://_/postgres:17/dev", want: platform.Postgres},
		{rawURL: "docker+mysql://_/mysql:8.4/dev", want: platform.MySQL},
		{rawURL: "docker+maria://_/mariadb:11/dev", want: platform.MariaDB},
		{rawURL: "docker+mariadb://_/mariadb:11/dev", want: platform.MariaDB},
		{rawURL: "docker+sqlserver://_/mssql:2022/dev", want: platform.SQLServer},
		{rawURL: "docker+clickhouse://_/clickhouse:24/dev", want: platform.ClickHouse},
	}

	for _, test := range tests {
		t.Run(test.rawURL, func(t *testing.T) {
			c := qt.New(t)
			dialect, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(dialect, qt.Equals, test.want)
		})
	}
}

// TestDialectFromURL_RefusesAnImageDriverTheCommunityBinaryDoesNotRegister is
// the control: `docker+` alone does not make a URL a docker URL.
func TestDialectFromURL_RefusesAnImageDriverTheCommunityBinaryDoesNotRegister(t *testing.T) {
	c := qt.New(t)
	dialect, err := atlasurl.DialectFromURL("docker+sqlite://_/x:1/dev")
	c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "docker\+sqlite://_/x:1/dev"`)
	c.Assert(dialect, qt.Equals, "")
}

// TestDialectFromURL_ReadsADockerURLAsWritten pins that a docker URL with a
// leading space is not a docker URL. Measured on the pinned community binary
// v1.3.0, ` docker://sqlite/dev` answers `parse open url: first path segment in
// URL cannot contain colon`, where `docker://sqlite/dev` answers `unsupported
// docker image "sqlite"`: the space makes the value a relative path.
func TestDialectFromURL_ReadsADockerURLAsWritten(t *testing.T) {
	tests := []string{" docker://sqlite/dev", " docker://postgres/16/dev", " docker+postgres://_/postgres:17/dev"}

	for _, rawURL := range tests {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			dialect, err := atlasurl.DialectFromURL(rawURL)
			c.Assert(err, qt.ErrorMatches, `parse --dev-url: .*first path segment in URL cannot contain colon`)
			c.Assert(dialect, qt.Equals, "")
		})
	}
}

// TestDialectFromURL_TrimsAnyOtherURL is the control: a database URL keeps the
// surrounding space this function has always trimmed, and so does a docker URL
// with a trailing space, which is part of its database name.
func TestDialectFromURL_TrimsAnyOtherURL(t *testing.T) {
	tests := []struct {
		rawURL string
		want   string
	}{
		{rawURL: " postgres://localhost/dev ", want: "postgres"},
		{rawURL: "docker://postgres/16/dev ", want: "postgres"},
	}

	for _, test := range tests {
		t.Run(test.rawURL, func(t *testing.T) {
			c := qt.New(t)
			dialect, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(dialect, qt.Equals, test.want)
		})
	}
}
