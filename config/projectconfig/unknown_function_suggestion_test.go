package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// TestParseAtlas_SuggestsTheSameFunctionOnEveryRun reads an atlas.hcl that
// calls an unknown function many times and gets one message, as the schema
// reader does; the HCL library's own suggestion changed between runs.
func TestParseAtlas_SuggestsTheSameFunctionOnEveryRun(t *testing.T) {
	c := qt.New(t)
	document := []byte("env \"dev\" {\n  url = now()\n}\n")
	for range 100 {
		_, err := projectconfig.ParseAtlas(document, "atlas.hcl", "dev")

		c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" at atlas\.hcl:2: atlas\.hcl:2,9-12: `+
			`Call to unknown function; There is no function named "now"\. Did you mean "pow"\?`)
	}
}
