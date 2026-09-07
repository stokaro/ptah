package fileopen

// White-box testing required: headlessReason and command take the platform and
// the environment as parameters precisely so every row runs on every platform,
// and that is only reachable from inside the package. Exporting them would put
// two functions on this package's surface that nothing outside it calls, which
// is the test-only accessor AGENTS.md refuses; driving them through Open
// instead would read runtime.GOOS and os.Getenv, and every assertion about a
// platform other than the runner's would become a tautology.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// Headless detection, over every platform on every platform. The environment is
// a parameter so none of these rows becomes a tautology on the runner that
// happens to execute them.
func TestHeadlessReason_DetectsWhatCannotShowAWindow(t *testing.T) {
	tests := []struct {
		name     string
		goos     string
		env      map[string]string
		want     string
		headless bool
	}{
		{
			name: "continuous integration, on every platform",
			goos: "darwin", env: map[string]string{"CI": "true"},
			want: "CI is set", headless: true,
		},
		{
			name: "continuous integration on windows too",
			goos: "windows", env: map[string]string{"CI": "1"},
			want: "CI is set", headless: true,
		},
		{
			name: "a unix session with no display",
			goos: "linux", env: nil,
			want: "no DISPLAY or WAYLAND_DISPLAY", headless: true,
		},
		{
			name: "an X session",
			goos: "linux", env: map[string]string{"DISPLAY": ":0"},
			want: "", headless: false,
		},
		{
			name: "a Wayland session",
			goos: "linux", env: map[string]string{"WAYLAND_DISPLAY": "wayland-0"},
			want: "", headless: false,
		},
		{
			name: "macOS needs no display variable",
			goos: "darwin", env: nil,
			want: "", headless: false,
		},
		{
			name: "Windows needs no display variable",
			goos: "windows", env: nil,
			want: "", headless: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reason, headless := headlessReason(test.goos, func(name string) string {
				return test.env[name]
			})

			c.Assert(headless, qt.Equals, test.headless)
			c.Assert(reason, qt.Equals, test.want)
		})
	}
}

// The opener per platform, asserted on every platform for the same reason.
func TestCommand_NamesThePlatformOpener(t *testing.T) {
	tests := []struct {
		name  string
		goos  string
		want  string
		args  []string
		known bool
	}{
		{name: "macOS", goos: "darwin", want: "open", args: []string{"/tmp/erd.html"}, known: true},
		{
			name: "Windows", goos: "windows", want: "rundll32",
			args: []string{"url.dll,FileProtocolHandler", "/tmp/erd.html"}, known: true,
		},
		{name: "Linux", goos: "linux", want: "xdg-open", args: []string{"/tmp/erd.html"}, known: true},
		{name: "FreeBSD", goos: "freebsd", want: "xdg-open", args: []string{"/tmp/erd.html"}, known: true},
		{name: "a platform with no opener", goos: "plan9", want: "", args: nil, known: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			name, args, known := command(test.goos, "/tmp/erd.html")

			c.Assert(known, qt.Equals, test.known)
			c.Assert(name, qt.Equals, test.want)
			c.Assert(args, qt.DeepEquals, test.args)
		})
	}
}

// The path travels as an argument, never through a shell. A path with a space
// is the case that breaks a `cmd /c start` spelling, and it is the ordinary
// shape of a temporary directory on macOS and Windows.
func TestCommand_CarriesAPathWithSpacesAsOneArgument(t *testing.T) {
	tests := []struct {
		name string
		goos string
		at   int
	}{
		{name: "macOS", goos: "darwin", at: 0},
		{name: "Windows", goos: "windows", at: 1},
		{name: "Linux", goos: "linux", at: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, args, known := command(test.goos, `/tmp/a folder/erd file.html`)

			c.Assert(known, qt.IsTrue)
			c.Assert(args[test.at], qt.Equals, `/tmp/a folder/erd file.html`)
		})
	}
}
