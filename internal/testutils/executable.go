package testutils

import "go.5x5.cz/ptah/internal/exeext"

// ExecutableSuffix is the extension a file needs before Windows will run it.
//
// A test that builds a helper binary has to add it: `go build -o dir/tool`
// writes a file with no extension, and exec then answers `executable file not
// found in %PATH%` -- a sentence about the search path, for a program given by
// absolute path.
//
// It is [exeext.Suffix] rather than a second copy, because the production side
// takes the same suffix back off when it derives a displayed command name, and
// two constants for one fact is how the two ends of a rule stop agreeing.
const ExecutableSuffix = exeext.Suffix
