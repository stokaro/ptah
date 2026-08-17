package atlashcl

// White-box testing required: the destination the `print` function writes to is
// the process's own standard output, and a test that captured that would also
// capture whatever else the run wrote. Redirecting it needs the unexported
// variable, and keeping the seam in a test file is what stops it becoming part
// of the package's API.

import (
	"io"

	qt "github.com/frankban/quicktest"
)

// SetPrintDestinationForTest redirects the `print` function's output for the
// duration of one test and restores it afterwards.
//
// It exists because the destination is the process's own standard output, and a
// test that captured that would also capture whatever else the run wrote. This
// is in an _test.go file so the seam is not part of the package's API.
func SetPrintDestinationForTest(c *qt.C, destination io.Writer) {
	c.Helper()
	previous := printDestination
	printDestination = destination
	c.Cleanup(func() { printDestination = previous })
}
