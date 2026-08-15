//go:build windows

package testutils

// ExecutableSuffix is the extension a file needs before Windows will run it.
//
// A test that builds a helper binary has to add it: `go build -o dir/tool`
// writes a file called `tool` with no extension, and exec then answers
// `executable file not found in %PATH%` -- a sentence about the search path
// for a program given by absolute path, which is the diagnostic this constant
// exists to prevent someone chasing.
const ExecutableSuffix = ".exe"
