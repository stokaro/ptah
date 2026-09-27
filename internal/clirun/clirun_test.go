package clirun_test

import (
	"bufio"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// childMode is the variable a test that runs this test binary again sets to
// pick what TestMain does in the child.
const childMode = "CLIRUN_TEST_CHILD"

// TestMain runs the tests through clirun.Main, as every package that builds
// must. A child started with childMode set to without-main runs them the old
// way, which is how TestBuild_RefusesWithoutMain reaches the refusal.
func TestMain(m *testing.M) {
	if os.Getenv(childMode) == "without-main" {
		os.Exit(m.Run())
	}
	clirun.Main(m)
}

// smallTarget is the cheapest program Build can compile: gofmt, from the
// toolchain every test already has. The tests that run a child test binary
// build it there instead of Ptah, which is a hundred megabytes each time.
const smallTarget = clirun.Target("cmd/gofmt")

// TestRun_HappyPath drives the real binary and reads back each stream on its
// own.
//
// `version` is the cheapest command that proves the whole path: the binary
// built, it ran, it wrote where it says it writes, and it exited 0.
func TestRun_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{}, "version")

	c.Assert(got.ExitCode, qt.Equals, 0)
	c.Assert(got.Stdout, qt.Contains, "Version:")
	c.Assert(got.Stdout, qt.Contains, "Platform:")
	c.Assert(got.Stderr, qt.Equals, "")
}

// TestRun_FailurePath is the half most end-to-end assertions need.
//
// A refusal is behavior Ptah promises, so a harness that failed the test on a
// non-zero exit would make the promise unmeasurable. The diagnostic belongs on
// stderr, and keeping the streams apart is what lets a test say so.
func TestRun_FailurePath(t *testing.T) {
	c := qt.New(t)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{}, "no-such-command")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Not(qt.Equals), "")
	c.Assert(got.Stdout, qt.Equals, "")
}

// TestRun_RunsInTheDirectoryItIsGiven pins the option an end-to-end test needs
// most.
//
// These commands read and write files relative to where they run, so a harness
// that ignored Dir would measure the test process's directory and quietly agree
// with whatever was already there.
func TestRun_RunsInTheDirectoryItIsGiven(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "ls", "--dir", ".")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), -1)
	c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), filepath.Dir(dir)+"/..")
}

// TestBuild_CompilesOncePerTarget is the property that makes a new end-to-end
// test cheap.
//
// Five copies of the old helper each rebuilt per test. Asserting on the path is
// how the memo is observable at all: a second compilation would land in a
// second temp directory.
func TestBuild_CompilesOncePerTarget(t *testing.T) {
	c := qt.New(t)

	first := clirun.Build(c, clirun.Ptah)
	second := clirun.Build(c, clirun.Ptah)

	c.Assert(second, qt.Equals, first)
	c.Assert(first, qt.Not(qt.Equals), "")
}

// TestBuild_KeepsTargetsApart is the control for the row above.
//
// A memo keyed on nothing would hand every caller the first binary it built,
// and a test asking for the compatibility surface would measure the native one
// while reading as if it had not.
func TestBuild_KeepsTargetsApart(t *testing.T) {
	c := qt.New(t)

	native := clirun.Build(c, clirun.Ptah)
	compat := clirun.Build(c, clirun.Compat)

	c.Assert(compat, qt.Not(qt.Equals), native)
}

// TestBuild_BuildsAMainPackageByImportPath is the test the child runs start.
//
// It logs the path so the parent can find the directory the child built in,
// and assert on it after the child has exited.
func TestBuild_BuildsAMainPackageByImportPath(t *testing.T) {
	c := qt.New(t)

	path := clirun.Build(c, smallTarget)

	_, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Logf("built %s", path)
}

// TestMain_RemovesWhatBuildCompiled runs a test binary that builds, and looks
// in its temp directory after it has exited: the directory Build made is gone
// (stokaro/ptah#3869). The logged path is what keeps this from passing on a
// child that built somewhere else.
func TestMain_RemovesWhatBuildCompiled(t *testing.T) {
	c := qt.New(t)
	temp := c.TempDir()

	output, err := runChild(c, temp, "")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	built := builtPath(c, output)
	c.Assert(filepath.Dir(filepath.Dir(built)), qt.Equals, temp)
	_, err = os.Stat(filepath.Dir(built))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(compilationDirs(c, temp), qt.HasLen, 0)
}

// TestBuild_RefusesWithoutMain runs a test binary whose TestMain does not call
// clirun.Main. Nothing would remove what Build compiled there, so Build fails
// the test before it compiles anything.
func TestBuild_RefusesWithoutMain(t *testing.T) {
	c := qt.New(t)
	temp := c.TempDir()

	output, err := runChild(c, temp, "without-main")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "clirun.Build needs clirun.Main")
	c.Assert(compilationDirs(c, temp), qt.HasLen, 0)
}

// TestBuild_SweepsWhatNoRunningProcessOwns seeds a temp directory with what
// earlier test binaries leave, and runs a child that builds in it.
//
// A directory whose owner file nobody holds a lock on belonged to a process
// that died before clirun.Main returned, and goes. A directory without an owner
// file was made before the owner file existed, and goes once it is older than
// any test could still be using it; a fresh one stays, because a directory has
// no owner file for a moment after it is made. Anything else stays.
func TestBuild_SweepsWhatNoRunningProcessOwns(t *testing.T) {
	c := qt.New(t)
	temp := c.TempDir()
	abandoned := seedDir(c, temp, "ptah-clirun-abandoned", "owner.lock", time.Now())
	oldUnowned := seedDir(c, temp, "ptah-clirun-old", "ptah", time.Now().Add(-4*time.Hour))
	newUnowned := seedDir(c, temp, "ptah-clirun-new", "ptah", time.Now())
	other := seedDir(c, temp, "other-abandoned", "owner.lock", time.Now().Add(-4*time.Hour))

	output, err := runChild(c, temp, "")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(compilationDirs(c, temp), qt.DeepEquals, []string{filepath.Base(newUnowned)})
	for _, gone := range []string{abandoned, oldUnowned} {
		_, err := os.Stat(gone)
		c.Assert(err, qt.ErrorIs, fs.ErrNotExist, qt.Commentf("%s", gone))
	}
	_, err = os.Stat(other)
	c.Assert(err, qt.IsNil)
}

// TestBuild_KeepsTheDirectoryOfARunningProcess is the control for the sweep:
// this process builds and holds its directory, and a child that sweeps the same
// temp directory leaves it alone. The directory is dated past the age that
// lets a sweep take a directory without an owner, so the lock this process
// holds is the only reason left to keep it.
func TestBuild_KeepsTheDirectoryOfARunningProcess(t *testing.T) {
	c := qt.New(t)
	own := clirun.Build(c, smallTarget)
	temp := filepath.Dir(filepath.Dir(own))
	old := time.Now().Add(-4 * time.Hour)
	c.Assert(os.Chtimes(filepath.Dir(own), old, old), qt.IsNil)

	output, err := runChild(c, temp, "")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(filepath.Dir(filepath.Dir(builtPath(c, output))), qt.Equals, temp)
	_, err = os.Stat(own)
	c.Assert(err, qt.IsNil)
}

// runChild runs this test binary again, on the one test that builds, with its
// temp directory set to temp and TestMain in the given mode. It returns the
// child's combined output and the error exec reports for it.
func runChild(c *qt.C, temp, mode string) (string, error) {
	c.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestBuild_BuildsAMainPackageByImportPath$", "-test.v") // #nosec G204 G702 -- this test binary, run again on a fixed test
	// TMPDIR is where Unix looks for the temp directory, and TMP and TEMP are
	// where Windows does.
	cmd.Env = append(os.Environ(), "TMPDIR="+temp, "TMP="+temp, "TEMP="+temp, childMode+"="+mode)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// builtPath reads the path the child logged.
func builtPath(c *qt.C, output string) string {
	c.Helper()
	var paths []string
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		if _, path, found := strings.Cut(scanner.Text(), ": built "); found {
			paths = append(paths, strings.TrimSpace(path))
		}
	}
	c.Assert(paths, qt.HasLen, 1, qt.Commentf("output:\n%s", output))
	return paths[0]
}

// compilationDirs lists the directories in temp a compilation could have made.
func compilationDirs(c *qt.C, temp string) []string {
	c.Helper()
	entries, err := os.ReadDir(temp)
	c.Assert(err, qt.IsNil)
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "ptah-clirun-") {
			dirs = append(dirs, entry.Name())
		}
	}
	return dirs
}

// seedDir makes temp/name holding one empty file, and dates the directory.
func seedDir(c *qt.C, temp, name, file string, modified time.Time) string {
	c.Helper()
	dir := filepath.Join(temp, name)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, file), nil, 0o600), qt.IsNil)
	c.Assert(os.Chtimes(dir, modified, modified), qt.IsNil)
	return dir
}
