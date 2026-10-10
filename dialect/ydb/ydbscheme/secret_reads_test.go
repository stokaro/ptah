package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/plangraph"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
)

// TestSecretPathReads_HappyPath reads each secret the paths name once. A path
// written absolute names the same secret relative to the database root, and
// an empty path reads nothing.
func TestSecretPathReads_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		root  string
		paths []string
		want  []plangraph.Effect
	}{
		{name: "an absolute path", root: "/local", paths: []string{"/local/token"},
			want: []plangraph.Effect{{Subject: ydbsecret.Ref("", "token"), Action: plangraph.Read}}},
		{name: "one secret named twice", paths: []string{"secrets/token", "", "secrets/token"},
			want: []plangraph.Effect{{Subject: ydbsecret.Ref("secrets", "token"), Action: plangraph.Read}}},
		{name: "an absolute path where the root is not known", paths: []string{"/local/token"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effects, err := ydbscheme.SecretPathReads(test.root, test.paths...)
			c.Assert(err, qt.IsNil)
			c.Assert(effects, qt.DeepEquals, test.want)
		})
	}
}

// TestSecretPathReads_FailurePath refuses a secret path outside the database
// the plan runs in, including one that leaves it through a parent segment.
func TestSecretPathReads_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{name: "another database", path: "/other/pw", // #nosec G101 -- a secret path, not a credential
			wantErr: `.*secret path "/other/pw" is outside the database /local, so no statement of it can read the secret.*`},
		{name: "a parent segment", path: "/local/../other/pw", // #nosec G101 -- a secret path, not a credential
			wantErr: `.*secret path "/local/../other/pw" is outside the database /local.*`},
		{name: "the database itself", path: "/local", wantErr: `.*secret path "/local" is outside the database /local.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effects, err := ydbscheme.SecretPathReads("/local", test.path)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(effects, qt.IsNil)
		})
	}
}
