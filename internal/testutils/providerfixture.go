package testutils

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	qt "github.com/frankban/quicktest"
)

// fixtureProviderDirectory is where the independent provider sources live,
// relative to the repository root.
const fixtureProviderDirectory = "provider-sdk/testdata/fixture/"

// BuildFixtureProvider builds one command of the fixture provider module and
// returns the path of the executable.
//
// The build runs outside the checkout, against the host's published SDK
// dependency: the module is copied from `git ls-files`, its local replacement
// is dropped and the SDK version the host requires is pinned, so no workspace
// or replacement can satisfy it. The dependency list of the command must name
// no `ptah.run/` package, which is what makes the provider independent of the
// host. command names a directory below cmd/ of the fixture module, such as
// "provider". The build is reproducible: -trimpath keeps the path of the
// temporary copy out of the executable, so two builds of one command with the
// same settings have one digest.
//
// The caller's working directory must be inside the repository.
func BuildFixtureProvider(c *qt.C, command string) string {
	c.Helper()
	return BuildFixtureProviderRelease(c, command, FixtureRelease{})
}

// FixtureRelease selects how [BuildFixtureProviderRelease] links one fixture
// provider. The zero value builds the fixture as [BuildFixtureProvider] does.
type FixtureRelease struct {
	// Version replaces the release version the fixture declares. Empty keeps
	// the declared one.
	Version string
	// Stripped links without the symbol table and debug information. The
	// executable then has another digest than an unstripped build of the same
	// source, as another platform build of one release does, while the
	// provider declares exactly the same description. It is the only setting
	// that changes the bytes without changing what the provider declares.
	Stripped bool
}

// BuildFixtureProviderRelease is [BuildFixtureProvider] with the link-time
// settings release selects, for a test that needs two builds of one release
// or two releases of one provider.
func BuildFixtureProviderRelease(c *qt.C, command string, release FixtureRelease) string {
	c.Helper()
	repo := strings.TrimSpace(string(runBuildCommand(c, ".", "go", "list", "-m", "-f", "{{.Dir}}", "ptah.run")))
	dir := c.TempDir()
	copyRoot, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(copyRoot.Close(), qt.IsNil) })
	paths := strings.Split(strings.TrimSpace(string(runBuildCommand(c, repo, "git", "ls-files", fixtureProviderDirectory))), "\n")
	c.Assert(len(paths) >= 5, qt.IsTrue)
	for _, path := range paths {
		relative, ok := strings.CutPrefix(path, fixtureProviderDirectory)
		c.Assert(ok, qt.IsTrue)
		data, err := os.ReadFile(filepath.Join(repo, path))
		c.Assert(err, qt.IsNil)
		c.Assert(copyRoot.MkdirAll(filepath.Dir(relative), 0o700), qt.IsNil)
		c.Assert(copyRoot.WriteFile(relative, data, 0o600), qt.IsNil)
	}
	const sdk = "github.com/stokaro/ptah/provider-sdk"
	version := strings.TrimSpace(string(runBuildCommand(c, repo, "go", "list", "-m", "-f", "{{.Version}}", sdk)))
	c.Assert(version, qt.Not(qt.Equals), "")
	runBuildCommand(c, dir, "go", "mod", "edit", "-dropreplace="+sdk, "-require="+sdk+"@"+version)
	runBuildCommand(c, dir, "go", "mod", "tidy")
	dependencies := string(runBuildCommand(c, dir, "go", "list", "-deps", "-f", "{{.ImportPath}}", "./cmd/"+command))
	c.Assert(dependencies, qt.Not(qt.Contains), "ptah.run/")
	var linkFlags []string
	if release.Stripped {
		linkFlags = append(linkFlags, "-s", "-w")
	}
	if release.Version != "" {
		linkFlags = append(linkFlags, "-X", fixtureVersionSymbol+"="+release.Version)
	}
	binary := filepath.Join(c.TempDir(), "provider"+ExecutableSuffix)
	build := []string{"build", "-trimpath", "-o", binary}
	if len(linkFlags) > 0 {
		build = append(build, "-ldflags="+strings.Join(linkFlags, " "))
	}
	runBuildCommand(c, dir, "go", append(build, "./cmd/"+command)...)
	return binary
}

// fixtureVersionSymbol is the fixture variable that holds its release version.
const fixtureVersionSymbol = "example.com/ptah-fixture-provider/backend.version"

func runBuildCommand(c *qt.C, dir, program string, args ...string) []byte {
	c.Helper()
	command := exec.CommandContext(c.Context(), program, args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s %v: %s", program, args, output))
	return output
}
