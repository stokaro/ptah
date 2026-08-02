package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasargs"
)

// Map fills unset flags from their PTAH_<FLAG> environment twins, and that
// includes unsupported ones on purpose: setting PTAH_TO_TAG is a request for
// --to-tag, and the loud refusal is the correct answer to a request Ptah cannot
// honor. Silently dropping it would be worse than refusing — on `migrate down`
// a discarded --to-tag leaves an empty target that parses as version 0, turning
// a bounded rollback into a full one.
//
// Exactly one flag opts out, via EnvDisabled, and only because another verb
// repurposed its name: `migrate apply` reads PTAH_SKIP_CHECKS as its
// pre-migration check bypass, so on `migrate down` that variable is not an ask.

func TestMap_UnsupportedFlagsAreStillSynthesizedFromTheEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag atlasargs.Flag
		// env is the PTAH_<FLAG> twin of flag.
		env string
		// value is what that variable is set to.
		value string
		// want is the refusal the mapper must produce.
		want string
	}{
		{
			name:  "bool with a rationale",
			flag:  atlasargs.UnsupportedBoolReason("plan", "", "Force dynamic down planning", "requires a hosted planner"),
			env:   "PTAH_PLAN",
			value: "1",
			want:  "atlas migrate down accepts --plan, but Ptah does not implement its behavior: requires a hosted planner",
		},
		{
			name:  "bool without a rationale",
			flag:  atlasargs.UnsupportedBool("plan", "", "Force dynamic down planning"),
			env:   "PTAH_PLAN",
			value: "true",
			want:  "atlas migrate down accepts --plan, but Ptah does not implement its behavior yet",
		},
		{
			name:  "string with a rationale",
			flag:  atlasargs.UnsupportedStringReason("to-tag", "", "Target tag", "tags require a hosted registry"),
			env:   "PTAH_TO_TAG",
			value: "release-v1",
			want:  "atlas migrate down accepts --to-tag, but Ptah does not implement its behavior: tags require a hosted registry",
		},
		{
			name:  "string without a rationale",
			flag:  atlasargs.UnsupportedString("format", "", "Go template output format"),
			env:   "PTAH_FORMAT",
			value: "{{ json . }}",
			want:  "atlas migrate down accepts --format, but Ptah does not implement its behavior yet",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(tc.env, tc.value)

			_, err := atlasargs.Map("migrate", "down", []atlasargs.Flag{tc.flag}, nil)

			c.Assert(err, qt.ErrorMatches, tc.want)
		})
	}
}

// The EnvDisabled marker is what narrows the exclusion to a single flag. The
// cases use true values because the mapper separately ignores a bool-false
// environment value, which would mask the marker.
func TestMap_EnvDisabledFlagsAreNotSynthesizedFromTheEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag atlasargs.Flag
		env  string
	}{
		{
			name: "unsupported waiver",
			flag: atlasargs.ExplicitUnsupportedBoolReason("skip-checks", "", "Skip down checks", "no generated checks to skip"),
			env:  "PTAH_SKIP_CHECKS",
		},
		{
			name: "native mapping",
			flag: atlasargs.ExplicitNativeBool("auto-approve", "", "Skip interactive approval", "auto-approve"),
			env:  "PTAH_AUTO_APPROVE",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(tc.env, "1")

			out, err := atlasargs.Map("migrate", "down", []atlasargs.Flag{tc.flag}, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.HasLen, 0)
		})
	}
}

// Supported flags are still filled from the environment, so the exclusion is
// keyed on the marker and not on the fallback being off. Without this case,
// deleting appendEnvArgs entirely would pass everything above.
func TestMap_SupportedFlagsAreStillFilledFromTheEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TO_VERSION", "20260801000001")

	out, err := atlasargs.Map("migrate", "down", []atlasargs.Flag{
		atlasargs.NativeString("to-version", "", "Target version", "target"),
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.DeepEquals, []string{"--target=20260801000001"})
}

// An EnvDisabled waiver passed explicitly still fails loudly while its twin is
// set, so the marker narrows what the environment may do without weakening the
// waiver itself.
func TestMap_ExplicitEnvDisabledFlagStillFailsWithTheEnvironmentSet(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SKIP_CHECKS", "1")

	_, err := atlasargs.Map("migrate", "down", []atlasargs.Flag{
		atlasargs.ExplicitUnsupportedBoolReason("skip-checks", "", "Skip down checks", "no generated checks to skip"),
	}, []string{"--skip-checks"})

	c.Assert(err, qt.ErrorMatches,
		"atlas migrate down accepts --skip-checks, but Ptah does not implement its behavior: no generated checks to skip")
}
