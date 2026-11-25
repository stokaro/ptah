package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasargs"
)

// skipLockReason is the waiver rationale of the flags below.
const skipLockReason = "the report reads the revision table under the migration lock"

// skipLockRefusal is what a value of --skip-lock that reads as true answers.
const skipLockRefusal = "atlas migrate drift accepts --skip-lock, but Ptah does not implement its behavior: " +
	skipLockReason

func skipLockFlag() atlasargs.Flag {
	return atlasargs.UnsupportedTrueBoolReason("skip-lock", "", "Skip taking the migration lock", skipLockReason)
}

// A value that reads as false asks for what Ptah does without the flag, so it
// is accepted and forwards nothing, whether the command line or the variable
// carries it. The command line takes precedence over the variable, as it does
// for every flag.
func TestMap_HappyPathAcceptsAnUnsupportedTrueFlagThatReadsAsFalse(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{name: "false", args: []string{"--skip-lock=false"}},
		{name: "zero", args: []string{"--skip-lock=0"}},
		{name: "upper case", args: []string{"--skip-lock=FALSE"}},
		{name: "the variable", env: map[string]string{"PTAH_SKIP_LOCK": "false"}},
		{
			name: "the command line over the variable",
			args: []string{"--skip-lock=false"},
			env:  map[string]string{"PTAH_SKIP_LOCK": "true"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			out, err := atlasargs.Map("migrate", "drift", []atlasargs.Flag{skipLockFlag()}, test.args)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.HasLen, 0)
		})
	}
}

// The bare flag and a value that reads as true ask for what Ptah does not do,
// and are refused with the reason, from the command line and the variable.
func TestMap_FailurePathRefusesAnUnsupportedTrueFlagThatReadsAsTrue(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{name: "bare", args: []string{"--skip-lock"}},
		{name: "true", args: []string{"--skip-lock=true"}},
		{name: "one", args: []string{"--skip-lock=1"}},
		{name: "the variable", env: map[string]string{"PTAH_SKIP_LOCK": "true"}},
		{name: "true after false", args: []string{"--skip-lock=false", "--skip-lock=true"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			out, err := atlasargs.Map("migrate", "drift", []atlasargs.Flag{skipLockFlag()}, test.args)

			c.Assert(err, qt.ErrorMatches, skipLockRefusal)
			c.Assert(out, qt.IsNil)
		})
	}
}

// A value that reads as no boolean is refused as pflag refuses it, not taken
// as false.
func TestMap_FailurePathRefusesAnUnsupportedTrueFlagThatReadsAsNoBoolean(t *testing.T) {
	c := qt.New(t)

	out, err := atlasargs.Map("migrate", "drift", []atlasargs.Flag{skipLockFlag()}, []string{"--skip-lock=maybe"})

	c.Assert(err, qt.ErrorMatches, `invalid argument "maybe" for "--skip-lock" flag: strconv.ParseBool: parsing "maybe": invalid syntax`)
	c.Assert(out, qt.IsNil)
}

// The control: an unsupported boolean without the mark refuses false too, so
// the acceptance above comes from the mark and not from every waiver.
func TestMap_FailurePathRefusesFalseOnAnUnsupportedBoolean(t *testing.T) {
	c := qt.New(t)
	flag := atlasargs.UnsupportedBoolReason("skip-lock", "", "Skip taking the migration lock", skipLockReason)

	out, err := atlasargs.Map("migrate", "drift", []atlasargs.Flag{flag}, []string{"--skip-lock=false"})

	c.Assert(err, qt.ErrorMatches, skipLockRefusal)
	c.Assert(out, qt.IsNil)
}
