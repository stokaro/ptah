// Package clirunguard hosts the test that keeps every package building with
// ptah.run/internal/clirun from leaving its programs in the temp directory.
//
// clirun.Build compiles a program once per test binary, about 120 MB of it, and
// no test can remove it, because later tests in the same process still run it.
// The test binary removes it, through clirun.Main in its TestMain. A package
// that imports clirun without that call leaves a directory behind on every run,
// and 200 of them are 20 GB of a development disk (stokaro/ptah#3869).
//
// clirun.Build refuses to compile when clirun.Main is not running, which
// catches the omission when a test builds. This test catches it without
// running anything, including in the integration trees, whose files carry a
// build tag the unit contour does not compile: it parses every file whatever
// its build constraints, and asks two questions of the tree.
//
//   - Only test files import clirun. A library package importing it would hand
//     the build to test binaries this test cannot see from the import.
//   - Every directory with a test file that imports clirun has a TestMain that
//     calls clirun.Main, under whatever name the file imports clirun as.
//
// A third test runs a package whose one test fails through clirun.Main, and
// expects the failure to reach go test. clirun's own tests cannot see a Main
// that loses the exit code, because they run through it: the loss would hide
// their own failure too.
//
// The package carries no runtime code of its own.
package clirunguard
