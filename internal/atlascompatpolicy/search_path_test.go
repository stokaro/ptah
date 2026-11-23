package atlascompatpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlascompatpolicy"
)

// The strict profile reads a search_path as one literal schema name, as the
// community binary does, so the value reaches the server as one quoted
// identifier. Measured on v1.4.0 against PostgreSQL 18, the binary selects
// " a" for the first row, "a " for the third and "A" for the fourth, and
// refuses the list as `schema "a,b" was not found`.
func TestStrictCESearchPathURL_QuotesTheValue(t *testing.T) {
	for _, test := range []struct {
		name, rawURL, want string
	}{
		{name: "a leading space", rawURL: "postgres://u@h/db?search_path=%20a", want: "postgres://u@h/db?search_path=%22%20a%22"},
		{name: "a leading space spelled as a plus", rawURL: "postgres://u@h/db?search_path=+a", want: "postgres://u@h/db?search_path=%22%20a%22"},
		{name: "a trailing space", rawURL: "postgres://u@h/db?search_path=a%20", want: "postgres://u@h/db?search_path=%22a%20%22"},
		{name: "a capital letter", rawURL: "postgres://u@h/db?search_path=A", want: "postgres://u@h/db?search_path=%22A%22"},
		{name: "a list is one name", rawURL: "postgres://u@h/db?search_path=a,b", want: "postgres://u@h/db?search_path=%22a%2Cb%22"},
		{name: "a quote is part of the name", rawURL: "postgres://u@h/db?search_path=%22a%22", want: "postgres://u@h/db?search_path=%22%22%22a%22%22%22"},
		{name: "the other parameters keep their bytes", rawURL: "postgres://u@h/db?sslmode=disable&search_path=a&x=%2F", want: "postgres://u@h/db?sslmode=disable&search_path=%22a%22&x=%2F"},
		{name: "the postgresql scheme", rawURL: "postgresql://u@h/db?search_path=a%20", want: "postgresql://u@h/db?search_path=%22a%20%22"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlascompatpolicy.StrictCE().ValidateURL(test.rawURL), qt.IsNil)

			got := atlascompatpolicy.StrictCE().SearchPathURL(test.rawURL)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A URL that selects no schema through a search_path, and every URL on the
// full profile, which reads the value with the server's own grammar, are
// returned as written.
func TestSearchPathURL_KeepsTheURL(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
		rawURL string
	}{
		{name: "no search_path", policy: atlascompatpolicy.StrictCE(), rawURL: "postgres://u@h/db?sslmode=disable"},
		{name: "an empty search_path", policy: atlascompatpolicy.StrictCE(), rawURL: "postgres://u@h/db?search_path="},
		{name: "a MySQL URL, which selects no schema with it", policy: atlascompatpolicy.StrictCE(), rawURL: "mysql://u@h/db?search_path=%20a"},
		{name: "a docker URL", policy: atlascompatpolicy.StrictCE(), rawURL: "docker://postgres/16/dev?search_path=%20a"},
		{name: "a file URL", policy: atlascompatpolicy.StrictCE(), rawURL: "file://schema.sql"},
		{name: "the full profile", policy: atlascompatpolicy.Full(), rawURL: "postgres://u@h/db?search_path=%20a,B"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.policy.SearchPathURL(test.rawURL)

			c.Assert(got, qt.Equals, test.rawURL)
		})
	}
}

// An env's database, dev and desired-state URLs are each rewritten once, and a
// source list is copied rather than written through.
func TestStrictCESearchPathProjectConfig_QuotesEveryURL(t *testing.T) {
	c := qt.New(t)
	sources := []string{"postgres://u@h/src?search_path=b%20", "file://schema.hcl"}
	config := projectconfig.Config{
		DatabaseURL:   "postgres://u@h/db?search_path=%20a",
		DevURL:        "postgres://u@h/dev?search_path=A",
		SchemaSources: sources,
	}

	got := atlascompatpolicy.StrictCE().SearchPathProjectConfig(config)

	c.Assert(got.DatabaseURL, qt.Equals, "postgres://u@h/db?search_path=%22%20a%22")
	c.Assert(got.DevURL, qt.Equals, "postgres://u@h/dev?search_path=%22A%22")
	c.Assert(got.SchemaSources, qt.DeepEquals, []string{"postgres://u@h/src?search_path=%22b%20%22", "file://schema.hcl"})
	c.Assert(sources, qt.DeepEquals, []string{"postgres://u@h/src?search_path=b%20", "file://schema.hcl"})
}

// The full profile keeps an env's URLs, and the strict one keeps a missing
// source list missing.
func TestSearchPathProjectConfig_KeepsTheConfig(t *testing.T) {
	c := qt.New(t)
	config := projectconfig.Config{DatabaseURL: "postgres://u@h/db?search_path=%20a", SchemaSources: []string{"postgres://u@h/src?search_path=b%20"}}

	c.Assert(atlascompatpolicy.Full().SearchPathProjectConfig(config).DatabaseURL, qt.Equals, config.DatabaseURL)
	c.Assert(atlascompatpolicy.Full().SearchPathProjectConfig(config).SchemaSources, qt.DeepEquals, config.SchemaSources)
	c.Assert(atlascompatpolicy.StrictCE().SearchPathProjectConfig(projectconfig.Config{}).SchemaSources, qt.IsNil)
}
