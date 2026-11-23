package atlascompatpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
)

// The strict profile refuses a search_path that is not a bare name, where the
// community binary reads one schema name byte for byte and Ptah reads the
// schema the server's grammar selects. Measured on v1.4.0 against PostgreSQL
// 18, the binary refuses every row here but the first, which it reads as ` a`
// and Ptah would read as `a`.
func TestStrictCEValidateURL_SearchPath_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, rawURL, wantErr string
	}{
		{name: "a leading space", rawURL: "postgres://u@h/db?search_path=%20a", wantErr: `" a"`},
		{name: "a leading space spelled as a plus", rawURL: "postgres://u@h/db?search_path=+a", wantErr: `" a"`},
		{name: "a trailing space", rawURL: "postgres://u@h/db?search_path=a%20", wantErr: `"a "`},
		{name: "a quoted name", rawURL: "postgres://u@h/db?search_path=%22a%22", wantErr: `"\\"a\\""`},
		{name: "a quoted name with a space", rawURL: "postgres://u@h/db?search_path=%22%20a%22", wantErr: `"\\" a\\""`},
		{name: "cockroachdb", rawURL: "cockroachdb://u@h/db?search_path=a%20", wantErr: `"a "`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlascompatpolicy.StrictCE().ValidateURL(test.rawURL)

			c.Assert(err, qt.ErrorMatches, `Atlas Community Edition strict compatibility does not support a search_path `+
				`that begins or ends with white space or holds a double quote: `+test.wantErr)
		})
	}
}

// A bare name and a list pass as they did, and the full profile reads each
// spelling the server's way, as it did.
func TestStrictCEValidateURL_SearchPath_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
		rawURL string
	}{
		{name: "a bare name", policy: atlascompatpolicy.StrictCE(), rawURL: "postgres://u@h/db?search_path=a"},
		{name: "a list", policy: atlascompatpolicy.StrictCE(), rawURL: "postgres://u@h/db?search_path=a,b"},
		{name: "a MySQL URL, which selects no schema with it", policy: atlascompatpolicy.StrictCE(), rawURL: "mysql://u@h/db?search_path=%20a"},
		{name: "the full profile", policy: atlascompatpolicy.Full(), rawURL: "postgres://u@h/db?search_path=%22%20a%22"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.policy.ValidateURL(test.rawURL), qt.IsNil)
		})
	}
}
