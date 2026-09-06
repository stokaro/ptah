package atlas_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/atlas"
	"ptah.run/internal/atlascompatpolicy"
)

// wordmark is one line of the shared banner, enough to find it in a stream.
//
// Written out rather than imported from cmd/internal/banner, so that this
// asserts what a consumer of ptah-compat's output sees rather than agreeing
// with whatever the banner package emits.
const wordmark = `|  ___/| |_ | (_| | | | |`

// TestCompatCommand_KeepsTheBannerOutOfCapturedOutput is the compatibility
// guarantee this surface exists to keep.
//
// The pinned community binary writes no banner, and the conformance CLI-surface
// tier compares captured output. An in-process buffer is the same kind of
// writer a conformance run and a shell redirect give the process, so what this
// asserts is that every measured invocation is byte-for-byte what it was.
//
// `Usage:` is the control: a "does not contain" assertion passes for a command
// that wrote nothing at all.
func TestCompatCommand_KeepsTheBannerOutOfCapturedOutput(t *testing.T) {
	// An empty non-nil slice, never nil: cobra reads os.Args[1:] when SetArgs
	// is given nil, so a nil row would run this test binary's own flags
	// through the command.
	tests := []struct {
		name string
		args []string
	}{
		{name: "the entry screen", args: make([]string, 0)},
		{name: "an explicit help flag", args: []string{"--help"}},
		{name: "a group's own entry screen", args: []string{"migrate"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommand("ptah-compat")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.args)

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, "Usage:")
			c.Assert(out.String(), qt.Not(qt.Contains), wordmark)
		})
	}
}

// TestCompatCommand_TheStrictProfileCarriesNoBannerAtAll is the second guard,
// and it is not the same assertion as the one above.
//
// Above, the writer decides: a terminal would get the banner and a buffer does
// not. Under PTAH_ATLAS_STRICT_COMPAT the banner is not wired at all, whatever
// the writer is, because strict CE is the profile the conformance measurement
// runs under and anything Ptah adds there reads as a divergence Ptah
// introduced rather than one it found. That is the same reasoning the `script`
// command is registered outside this profile under.
//
// Asserting it needs the entry screen to be reachable and unchanged, which the
// Usage control gives, plus the two profiles agreeing on it -- so a strict
// profile that had quietly stopped producing an entry screen could not pass.
func TestCompatCommand_TheStrictProfileCarriesNoBannerAtAll(t *testing.T) {
	c := qt.New(t)

	strict := atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.StrictCE())
	var strictOut bytes.Buffer
	strict.SetOut(&strictOut)
	strict.SetErr(&strictOut)
	strict.SetArgs(make([]string, 0))
	c.Assert(strict.Execute(), qt.IsNil)

	full := atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.Full())
	var fullOut bytes.Buffer
	full.SetOut(&fullOut)
	full.SetErr(&fullOut)
	full.SetArgs(make([]string, 0))
	c.Assert(full.Execute(), qt.IsNil)

	c.Assert(strictOut.String(), qt.Contains, "Usage:")
	c.Assert(strictOut.String(), qt.Not(qt.Contains), wordmark)
	c.Assert(fullOut.String(), qt.Not(qt.Contains), wordmark)
}
