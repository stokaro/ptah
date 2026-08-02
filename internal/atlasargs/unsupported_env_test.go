package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasargs"
)

// Map fills unset flags from their PTAH_<FLAG> environment twins, but an
// unsupported flag is explicit-only: its only behavior is a loud refusal, and a
// refusal must mean the operator asked for it.
//
// The concrete collision this rule prevents: PTAH_SKIP_CHECKS is the sanctioned
// pre-migration check bypass on `ptah-compat migrate apply`, and `migrate down`
// carries an unsupported --skip-checks. Synthesizing the down flag from that
// variable made exporting it for an apply break every down in the same shell.

func TestMap_UnsupportedFlagsAreNotSynthesizedFromTheEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag atlasargs.Flag
		// env is the PTAH_<FLAG> twin of flag.
		env string
		// value is what that variable is set to.
		value string
	}{
		{
			name:  "bool with a rationale",
			flag:  atlasargs.UnsupportedBoolReason("skip-checks", "", "Skip safety checks", "no generated checks to skip"),
			env:   "PTAH_SKIP_CHECKS",
			value: "1",
		},
		{
			name:  "bool without a rationale",
			flag:  atlasargs.UnsupportedBool("plan", "", "Force dynamic down planning"),
			env:   "PTAH_PLAN",
			value: "true",
		},
		{
			name:  "string with a rationale",
			flag:  atlasargs.UnsupportedStringReason("to-tag", "", "Target tag", "tags exist only in Atlas Registry"),
			env:   "PTAH_TO_TAG",
			value: "release-v1",
		},
		{
			name:  "string without a rationale",
			flag:  atlasargs.UnsupportedString("format", "", "Go template output format"),
			env:   "PTAH_FORMAT",
			value: "{{ json . }}",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(tc.env, tc.value)

			out, err := atlasargs.Map("migrate", "down", []atlasargs.Flag{tc.flag}, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.HasLen, 0)
		})
	}
}

// The same variables still fill SUPPORTED flags, so the exclusion is keyed on
// the flag being unsupported and not on the environment fallback being off.
// Without this case, deleting appendEnvArgs entirely would pass the test above.
func TestMap_SupportedFlagsAreStillFilledFromTheEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TO_VERSION", "20260801000001")

	out, err := atlasargs.Map("migrate", "down", []atlasargs.Flag{
		atlasargs.NativeString("to-version", "", "Target version", "target"),
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.DeepEquals, []string{"--target=20260801000001"})
}

// An unsupported flag passed explicitly still fails loudly while its twin is
// set, so the rule narrows what the environment may do without weakening the
// waiver itself.
func TestMap_ExplicitUnsupportedFlagStillFailsWithTheEnvironmentSet(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SKIP_CHECKS", "1")

	_, err := atlasargs.Map("migrate", "down", []atlasargs.Flag{
		atlasargs.UnsupportedBoolReason("skip-checks", "", "Skip safety checks", "no generated checks to skip"),
	}, []string{"--skip-checks"})

	c.Assert(err, qt.ErrorMatches,
		"atlas migrate down accepts --skip-checks, but Ptah does not implement its behavior: no generated checks to skip")
}
