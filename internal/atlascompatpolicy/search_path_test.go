package atlascompatpolicy_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlascompatpolicy"
)

// strictSearchPathRefusal is the refusal for value read from source, as a
// pattern that matches it literally.
func strictSearchPathRefusal(value, source string) string {
	return regexp.QuoteMeta(`Atlas Community Edition strict compatibility refuses search_path=` + value + ` in ` + source + `: ` +
		`the community binary reads the value as one schema name, byte for byte, and the server's ` +
		`search_path grammar reads another; name a schema other than $user, in lower case, at most 63 ` +
		`bytes, without white space, quotes, commas or a plus sign, or unset PTAH_ATLAS_STRICT_COMPAT ` +
		`to read the value as the server does`)
}

// Strict mode refuses a search_path the pinned community binary reads as one
// schema name and the server's grammar reads as another. Measured on v1.4.0
// against PostgreSQL 18, the binary selects " a" for `%20a`, "a " for `a%20`
// and "A" for `A`, which the server reads as `a`, and reads `a,b` as the one
// schema a,b.
func TestStrictCEValidateSearchPath_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, rawURL, value string
	}{
		{name: "a leading space", rawURL: "postgres://u@h/db?search_path=%20a", value: "%20a"},
		{name: "a trailing space", rawURL: "postgres://u@h/db?search_path=a%20", value: "a%20"},
		{name: "a space inside", rawURL: "postgres://u@h/db?search_path=a%20b", value: "a%20b"},
		{name: "a plus sign", rawURL: "postgres://u@h/db?search_path=a+b", value: "a+b"},
		{name: "a capital letter", rawURL: "postgres://u@h/db?search_path=A", value: "A"},
		{name: "a list", rawURL: "postgres://u@h/db?search_path=a,b", value: "a,b"},
		{name: "the session user's schema", rawURL: "postgres://u@h/db?search_path=$user", value: "$user"},
		{name: "a quoted name", rawURL: "postgres://u@h/db?search_path=%22%20a%22", value: "%22%20a%22"},
		{name: "the postgresql scheme", rawURL: "postgresql://u@h/db?search_path=A", value: "A"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlascompatpolicy.StrictCE().ValidateSearchPath("--url", test.rawURL)

			c.Assert(err, qt.ErrorMatches, strictSearchPathRefusal(test.value, "--url"))
		})
	}
}

// A plain name passes under strict mode, and the full surface reads every
// spelling the server's way.
func TestValidateSearchPath_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
		rawURL string
	}{
		{name: "a plain name", policy: atlascompatpolicy.StrictCE(), rawURL: "postgres://u@h/db?search_path=app_2"},
		{name: "no search_path", policy: atlascompatpolicy.StrictCE(), rawURL: "postgres://u@h/db?sslmode=disable"},
		{name: "a MySQL URL, which selects no schema with it", policy: atlascompatpolicy.StrictCE(), rawURL: "mysql://u@h/db?search_path=%20a"},
		{name: "the full surface, a leading space", policy: atlascompatpolicy.Full(), rawURL: "postgres://u@h/db?search_path=%20a"},
		{name: "the full surface, a quoted name", policy: atlascompatpolicy.Full(), rawURL: "postgres://u@h/db?search_path=%22%20a%22"},
		{name: "the full surface, a list", policy: atlascompatpolicy.Full(), rawURL: "postgres://u@h/db?search_path=a,b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.policy.ValidateSearchPath("--url", test.rawURL), qt.IsNil)
		})
	}
}

// A project file's URLs are refused with the attribute and the env they came
// from, so the message says which value to change.
func TestStrictCEValidateProjectConfig_SearchPath_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		config projectconfig.Config
		want   string
	}{
		{
			name:   "the url of an env",
			config: projectconfig.Config{EnvName: "local", DatabaseURL: "postgres://u@h/db?search_path=A"},
			want:   strictSearchPathRefusal("A", `the url of env "local"`),
		},
		{
			name:   "the dev url of an env",
			config: projectconfig.Config{EnvName: "local", DevURL: "postgres://u@h/dev?search_path=a%20"},
			want:   strictSearchPathRefusal("a%20", `the dev url of env "local"`),
		},
		{
			name:   "a schema src of an env",
			config: projectconfig.Config{EnvName: "local", SchemaSources: []string{"postgres://u@h/db?search_path=%20a"}},
			want:   strictSearchPathRefusal("%20a", `a schema src of env "local"`),
		},
		{
			name:   "a project file without an env",
			config: projectconfig.Config{DatabaseURL: "postgres://u@h/db?search_path=A"},
			want:   strictSearchPathRefusal("A", "the url of the project file"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlascompatpolicy.StrictCE().ValidateProjectConfig(test.config)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// The control: the same project file passes on the full surface, and under
// strict mode with a plain name.
func TestValidateProjectConfig_SearchPath_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
		rawURL string
	}{
		{name: "strict mode, a plain name", policy: atlascompatpolicy.StrictCE(), rawURL: "postgres://u@h/db?search_path=app"},
		{name: "the full surface", policy: atlascompatpolicy.Full(), rawURL: "postgres://u@h/db?search_path=A"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config := projectconfig.Config{EnvName: "local", DatabaseURL: test.rawURL, DevURL: test.rawURL}

			c.Assert(test.policy.ValidateProjectConfig(config), qt.IsNil)
		})
	}
}
