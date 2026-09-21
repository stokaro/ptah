package projectconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// writeOnlineAlterConfig writes a ptah.yaml and returns its path.
func writeOnlineAlterConfig(c *qt.C, t *testing.T, contents string) string {
	c.Helper()
	path := filepath.Join(t.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
	return path
}

// An environment says what it wants, in both directions. A field left out of
// the merge is one an env block can neither set nor unset, so a deployment
// reads the opposite policy from the environment it selected.
func TestOnlineAlter_EnvironmentDecidesInBothDirections(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		env      string
		want     bool
	}{
		{
			name:     "the environment turns it on",
			contents: "env:\n  prod:\n    diff:\n      online_alter: true\n",
			env:      "prod",
			want:     true,
		},
		{
			name: "the environment turns off what the top level asked for",
			contents: "diff:\n  online_alter: true\n" +
				"env:\n  local:\n    diff:\n      online_alter: false\n",
			env:  "local",
			want: false,
		},
		{
			name: "an environment that says nothing inherits",
			contents: "diff:\n  online_alter: true\n" +
				"env:\n  local:\n    diff:\n      concurrent_index: true\n",
			env:  "local",
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := writeOnlineAlterConfig(c, t, test.contents)

			cfg, err := projectconfig.LoadPtahFile(path, test.env)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.OnlineAlterRequested(), qt.Equals, test.want)
		})
	}
}
