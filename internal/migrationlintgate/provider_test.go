package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/internal/migrationlintgate"
)

func TestPolicyDefaultsDoNotRequireUnselectedEngineRules(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New()
	c.Assert(err, qt.IsNil)
	policy, err := migrationlintgate.LoadPolicy(runtime, fstest.MapFS{}, "fixture")
	c.Assert(err, qt.IsNil)
	c.Assert(policy.BlockingFamilies(), qt.Contains, "DS")
}

func TestPolicyStillRefusesUserSelectorsForUnselectedEngines(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New()
	c.Assert(err, qt.IsNil)
	_, err = migrationlintgate.LoadPolicy(runtime, fstest.MapFS{
		".ptah-lint.yaml": {Data: []byte("disabled-rules: [PG]\n")},
	}, "fixture")
	c.Assert(err, qt.ErrorMatches, `rule selector "PG" does not match any registered rule`)
}
