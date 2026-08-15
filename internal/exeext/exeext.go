// Package exeext carries the one fact that differs between operating systems
// about what an executable file is called.
//
// Two things need it and must not disagree. A test that builds a helper binary
// has to write the name the platform will run -- `go build -o dir/tool` on
// Windows produces a file exec refuses, reported as `executable file not found
// in %PATH%` for a program given by absolute path. And a command that derives
// its displayed name from os.Args[0] has to take the suffix back off, or it
// reports a name nobody typed.
//
// Those are the two ends of one fact, so they read one constant. See AGENTS.md,
// "Recognition that spans two functions belongs to one of them".
package exeext

import (
	"path/filepath"
	"strings"
)

// TrimmedBase returns the command name behind an argv[0], with the platform's
// executable extension removed.
//
// On Windows `atlas` and `atlas.exe` are the same command -- the extension is
// how the loader finds the file, not part of what the operator typed -- so a
// drop-in installed as atlas.exe must still call itself atlas.
func TrimmedBase(argv0 string) string {
	return trimBase(argv0, Suffix)
}

// trimBase takes the suffix as an argument rather than reading the constant, so
// the Windows answer is reachable from a test on any operating system.
//
// Reading Suffix directly would make every assertion about the trimming pass
// on Unix by doing nothing -- green, and about nothing. This is the same trap
// as a file-mode assertion that reduces to 0o200 == 0o200 on Windows.
func trimBase(argv0, suffix string) string {
	base := filepath.Base(argv0)
	if suffix == "" {
		return base
	}
	// Case-insensitively, because Windows does not distinguish ATLAS.EXE from
	// atlas.exe and an operator may have installed either.
	if trimmed, ok := strings.CutSuffix(strings.ToLower(base), strings.ToLower(suffix)); ok {
		return base[:len(trimmed)]
	}
	return base
}
