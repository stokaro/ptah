package fileopen_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/fileopen"
)

// The pair the issue asks for, written as one table because the two rows differ
// only in the suppression and must not differ in anything else.
//
// Why it has to be a pair: a suppression asserted on its own passes on a
// machine that could not have opened a browser anyway, and detection asserted
// on its own passes on a build where the suppression is what refused. Both rows
// run with CI set, so the environment is identical and the only variable is the
// one under test -- which also pins the ORDER, since a build that consulted the
// environment first would answer "CI is set" to both.
func TestOpen_SaysWhichRefusalAnswered(t *testing.T) {
	tests := []struct {
		name string
		skip bool
		want string
	}{
		{
			name: "the operator said not to, and that answer comes first",
			skip: true,
			want: fileopen.SkipEnvVar + " is set",
		},
		{
			name: "nothing said not to, and the environment cannot show a window",
			skip: false,
			want: "CI is set",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("CI", "true")

			result := fileopen.Open(context.Background(),
				filepath.Join(t.TempDir(), "erd.html"), fileopen.Options{Skip: test.skip})

			c.Assert(result.Opened, qt.IsFalse)
			c.Assert(result.Reason, qt.Equals, test.want)
		})
	}
}

// A refusal always says why. A caller prints the reason after the path, and a
// run that opened nothing and said nothing is indistinguishable from one that
// opened a window on a machine nobody is watching.
func TestOpen_NeverRefusesWithoutAReason(t *testing.T) {
	c := qt.New(t)
	t.Setenv("CI", "true")

	result := fileopen.Open(context.Background(),
		filepath.Join(t.TempDir(), "erd.html"), fileopen.Options{})

	c.Assert(result.Opened, qt.IsFalse)
	c.Assert(result.Reason, qt.Not(qt.Equals), "")
}

// The suppressing variable is declared, so the strict-compatibility policy and
// the guard over `PTAH_*` names both see it. A constant spelled at the call
// site would be invisible to each.
func TestSkipRequested_ReadsADeclaredVariable(t *testing.T) {
	c := qt.New(t)
	t.Setenv(fileopen.SkipEnvVar, "true")

	skip, err := fileopen.SkipRequested()

	c.Assert(err, qt.IsNil)
	c.Assert(skip, qt.IsTrue)
}

// A malformed value is a configuration error, not the default. An operator who
// wrote `yes` believes they suppressed the browser, and nothing else would tell
// them otherwise.
func TestSkipRequested_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(fileopen.SkipEnvVar, "yes")

	skip, err := fileopen.SkipRequested()

	c.Assert(err, qt.ErrorMatches, `.*`+fileopen.SkipEnvVar+`.*`)
	c.Assert(skip, qt.IsFalse)
}
