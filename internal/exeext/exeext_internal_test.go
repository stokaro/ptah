package exeext

// White-box testing required: the trimming has to be exercised with a Windows
// suffix from every operating system, and the exported entry point reads the
// build-tagged constant, which is empty on all but one of them.

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestTrimBase_TakesOffTheExtensionWindowsAdds pins the Windows answer on every
// platform, by handing the suffix in rather than reading the constant.
//
// The operand is measured, not assumed: a copy of the pinned community binary
// renamed to atlas-renamed.exe still prints `Usage:\n  atlas migrate [command]`,
// so that binary's displayed name does not follow its filename. A drop-in that
// echoed argv[0] would diverge from it on Windows, where the extension is the
// only way to install an executable at all.
func TestTrimBase_TakesOffTheExtensionWindowsAdds(t *testing.T) {
	tests := []struct {
		name   string
		argv0  string
		suffix string
		want   string
	}{
		{name: "the extension comes off", argv0: "atlas.exe", suffix: ".exe", want: "atlas"},
		{name: "case does not matter", argv0: "ATLAS.EXE", suffix: ".exe", want: "ATLAS"},
		{name: "a path keeps only its last element", argv0: filepath.Join("bin", "atlas.exe"), suffix: ".exe", want: "atlas"},
		{name: "a name without it is unchanged", argv0: "atlas", suffix: ".exe", want: "atlas"},
		{name: "another dot is not the extension", argv0: "atlas.v2", suffix: ".exe", want: "atlas.v2"},
		{name: "no suffix declared leaves the name alone", argv0: "atlas.exe", suffix: "", want: "atlas.exe"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(trimBase(test.argv0, test.suffix), qt.Equals, test.want)
		})
	}
}
