// Package fileopen hands a local file to the desktop's own handler.
//
// It exists for the commands that write an artifact a person is meant to look
// at -- an ERD document today -- and it is deliberately the smallest thing that
// can do that. Nothing here knows what the file contains, and nothing here
// reaches the network: the argument is a path on this machine, and the program
// it runs is the operating system's own opener.
//
// # Opening never fails a run
//
// [Open] returns no error. That is the contract, not an omission: the artifact
// is the deliverable and it was already written before anything here was
// called, so a machine with no desktop, no opener or a refusing one must still
// exit 0. Making that structural rather than a rule each caller remembers is
// the point -- a caller cannot accidentally propagate a failure it never
// receives.
//
// What a caller does get is why nothing opened, so it can print the path
// instead of saying nothing. A run that silently did not open is
// indistinguishable from one that opened a window on a machine nobody is
// looking at.
package fileopen

import (
	"context"
	"os"
	"os/exec"
	"runtime"

	"ptah.run/internal/envbool"
)

// SkipEnvVar suppresses opening without suppressing the artifact.
//
// It is an environment variable and not a flag because the surface that needs
// it most is `ptah-compat`, whose conformance cli-surface tier asserts that it
// registers exactly the flags the pinned community binary registers. A flag
// that binary does not have would break the promise that surface exists to
// keep; the precedent and the spelling are
// [ptah.run/internal/atlasfilter.AllowUnmatchedExcludeEnvVar].
const SkipEnvVar = envbool.Prefix + "SKIP_BROWSER_OPEN"

// skipOpen is the declared variable. Its default is false, so the default is to
// open, which is what a flag named for the browser promises.
//
// This is a restrictive toggle, which AGENTS.md asks be justified rather than
// added by habit. The justification is what a typo costs: `PTAH_SKIP_BROWSER_OPEN=yes`
// is a configuration error and refused, and a value this package never reads
// leaves the default, which opens a window on a machine that could open one.
// That is visible the moment it happens and changes nothing on disk -- the
// artifact is written either way. It cannot be a capability gate because the
// capability is not in question: the run can open a browser, and the operator
// is saying they would rather it did not.
var skipOpen = envbool.New(SkipEnvVar, false, envbool.Gated)

// SkipRequested resolves [SkipEnvVar].
//
// Call it before the work that produces the artifact, so a malformed value is
// refused on every invocation of the command that reads it rather than on the
// ones that happen to reach the opening step.
func SkipRequested() (bool, error) { return skipOpen.Resolve() }

// Result is what happened, and why when nothing did.
type Result struct {
	// Opened reports that the opener was started. It says nothing about
	// whether a window appeared: the opener detaches, and a caller that waited
	// to find out would hang on the browser rather than on the schema.
	Opened bool
	// Reason names why nothing opened, in words a caller can print after the
	// path. It is empty exactly when Opened is true.
	Reason string
}

// Options are the decisions a caller has already made.
type Options struct {
	// Skip suppresses opening. The caller resolves [SkipEnvVar] itself, with
	// [SkipRequested], so a malformed value is a configuration error at the
	// start of the command rather than a silent default here.
	Skip bool
}

// Open asks the desktop to open path, and says what happened.
//
// The order of the refusals is the order of certainty. An explicit request not
// to open comes first, because an operator who said so should not have their
// environment second-guessed. Then the environments that cannot: a continuous
// integration run, a Unix session with no display, a platform this package has
// no opener for, and an opener that is not installed. Only then is a program
// started.
func Open(ctx context.Context, path string, opts Options) Result {
	if opts.Skip {
		return Result{Reason: SkipEnvVar + " is set"}
	}
	if reason, headless := headlessReason(runtime.GOOS, os.Getenv); headless {
		return Result{Reason: reason}
	}
	name, args, known := command(runtime.GOOS, path)
	if !known {
		return Result{Reason: "no opener is known for " + runtime.GOOS}
	}
	binary, err := exec.LookPath(name)
	if err != nil {
		return Result{Reason: name + " is not installed"}
	}
	// #nosec G204 -- name comes from the table in command() and the only
	// caller-supplied value is the path, passed as an argument rather than
	// through a shell.
	cmd := exec.CommandContext(ctx, binary, args...)
	if err := cmd.Start(); err != nil {
		return Result{Reason: name + " did not start: " + err.Error()}
	}
	// The opener is not waited on. It detaches into a browser that outlives
	// this process, and reaping it would mean holding the command open for as
	// long as someone reads the page.
	go func() { _ = cmd.Wait() }()
	return Result{Opened: true}
}

// headlessReason answers whether this environment can be expected to show a
// window, and says why not.
//
// goos and lookup are parameters rather than reads of the running process so
// the table that covers every platform runs on every platform. A check written
// against runtime.GOOS directly would reduce to a tautology on two of the three
// (AGENTS.md, "a platform-conditional assertion tends to pass on the platform
// it cannot test").
func headlessReason(goos string, lookup func(string) string) (string, bool) {
	// CI first and on every platform: a Windows or macOS runner has no one
	// watching either, and the variable is the one thing every provider sets.
	if lookup("CI") != "" {
		return "CI is set", true
	}
	// A display is a Unix idea. Windows and macOS have a session or they have
	// no process to run this in at all, so asking there would refuse every
	// ordinary desktop run.
	switch goos {
	case "windows", "darwin":
		return "", false
	}
	if lookup("DISPLAY") == "" && lookup("WAYLAND_DISPLAY") == "" {
		return "no DISPLAY or WAYLAND_DISPLAY", true
	}
	return "", false
}

// command is the program that asks a desktop to open a path, per platform.
//
// Windows goes through rundll32 rather than `cmd /c start`, because start is a
// shell builtin whose first quoted argument is the window title -- a path in
// quotes becomes a title and nothing opens, and a path without them breaks on
// the first space.
func command(goos, path string) (name string, args []string, known bool) {
	switch goos {
	case "darwin":
		return "open", []string{path}, true
	case "windows":
		return "rundll32", []string{"url.dll,FileProtocolHandler", path}, true
	case "linux", "freebsd", "openbsd", "netbsd":
		return "xdg-open", []string{path}, true
	}
	return "", nil, false
}
