package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// The schema apply log is on unless a project turns it off, at the top level
// or in the env it selects, and the env's word wins over the top level's.
func TestParsePtahSchemaApplyLog_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		envName string
		want    bool
	}{
		{name: "nothing set", raw: "url: sqlite://app.db\n", want: true},
		{name: "turned off", raw: "schema_apply:\n  log: false\n", want: false},
		{name: "turned on", raw: "schema_apply:\n  log: true\n", want: true},
		{
			name:    "turned off in the selected env",
			raw:     "env:\n  ci:\n    schema_apply:\n      log: false\n",
			envName: "ci",
			want:    false,
		},
		{
			name:    "turned back on in the selected env",
			raw:     "schema_apply:\n  log: false\nenv:\n  dev:\n    schema_apply:\n      log: true\n",
			envName: "dev",
			want:    true,
		},
		{
			name:    "an env that is not selected",
			raw:     "env:\n  ci:\n    schema_apply:\n      log: false\n  prod:\n    url: sqlite://prod.db\n",
			envName: "prod",
			want:    true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParsePtah([]byte(test.raw), "ptah.yaml", test.envName)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.SchemaApply.LogEnabled(), qt.Equals, test.want)
		})
	}
}

func TestParsePtahSchemaApplyLog_FailurePath(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte("schema_apply:\n  logs: false\n"), "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: .*unknown .*"logs".*`)
	c.Assert(cfg.SchemaApply, qt.DeepEquals, projectconfig.SchemaApplyConfig{})
}
