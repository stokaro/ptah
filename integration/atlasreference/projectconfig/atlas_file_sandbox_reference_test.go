// Unix-only because two escape fixtures use symbolic links, whose creation on
// Windows needs privileges an ordinary CI account does not have.
//go:build integration && !windows

package projectconfig_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/integration/atlasreference"
)

// The community half of the stokaro/ptah#1042 divergence, measured rather than
// remembered.
//
// The decision recorded in that issue is to keep Ptah's file() sandbox on both
// surfaces, which leaves `ptah-compat` stricter than the community binary in
// exactly one respect. A divergence nobody re-measures rots: if a later
// community build starts refusing these reads, the divergence is gone and the
// sandbox is plain parity -- and nothing here would have said so. So this run
// pins the community behavior itself, and asserts Ptah's refusal of the same
// fixture beside it.
//
// It runs only with PTAH_ATLAS_REFERENCE set, and the Atlas CE Oracle workflow
// fails if it skips.

const fileReferenceEnv = atlasreference.EnvVar

// fileReferenceVersion is the only build these verdicts describe. Comparing
// against another build would report version drift as divergence.
const fileReferenceVersion = "atlas community version v1.3.0"

// fileOracleProbeURL is planted outside the config directory. It is a URL scheme
// so the value can be traced into the binary's own error text: the leak this
// sandbox exists to prevent is the content landing somewhere observable, not
// merely being opened.
const fileOracleProbeURL = "ptahsecret1042://x"

func TestOracleReadsFilesOutsideTheAtlasHCLDirectory(t *testing.T) {
	oracle := requireFileOracle(t)

	tests := []struct {
		name string
		// argument plants whatever the escape needs inside the config directory
		// dir and returns the file() argument aimed at the file outside. The
		// error is its own arrangement's, returned rather than asserted so that
		// a failure to plant is reported at the row's call site, alongside the
		// verdicts the measurement depends on.
		argument func(dir, outside string) (string, error)
		// ptahErr is what Ptah says about the identical config.
		ptahErr string
	}{
		{
			name:     "absolute path",
			argument: escapeAbsolutePath,
			ptahErr:  `.*absolute paths are not supported.*`,
		},
		{
			name:     "parent traversal",
			argument: escapeParentTraversal,
			ptahErr:  `.*path escapes atlas\.hcl directory.*`,
		},
		{
			name:     "symbolic link out of the directory",
			argument: escapeSymlinkedFile,
			ptahErr:  `.*path escapes atlas\.hcl directory.*symbolic link.*`,
		},
		{
			name:     "symbolic link to a directory outside",
			argument: escapeSymlinkedDirectory,
			ptahErr:  `.*path escapes atlas\.hcl directory.*symbolic link.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			base := t.TempDir()
			dir := filepath.Join(base, "project")
			c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
			outside := plantOracleSecret(c, base)
			argument, plantErr := tt.argument(dir, outside)
			c.Assert(plantErr, qt.IsNil)
			path := writeOracleFileConfig(c, dir, argument)

			out, code := runFileOracle(c, oracle, dir)

			// The community binary reads it. Exit 0 on a config whose only
			// unusual content is the file() call is the whole claim.
			c.Assert(code, qt.Equals, 0, qt.Commentf("oracle output: %s", out))

			_, err := projectconfig.LoadAtlasFile(path, "local")

			c.Assert(err, qt.ErrorMatches, tt.ptahErr)
		})
	}
}

// The control the exit codes above depend on. A missing file makes the same
// position fail, so `exit 0` up there means the community binary read the file
// -- not that it never evaluated the expression.
func TestOracleEvaluatesTheFileCallItIsGiven(t *testing.T) {
	oracle := requireFileOracle(t)
	c := qt.New(t)

	base := t.TempDir()
	dir := filepath.Join(base, "project")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	path := writeOracleFileConfig(c, dir, "no-such-file.txt")

	out, code := runFileOracle(c, oracle, dir)

	c.Assert(code, qt.Equals, 1, qt.Commentf("oracle output: %s", out))
	c.Assert(out, qt.Contains, "no-such-file.txt")

	_, err := projectconfig.LoadAtlasFile(path, "local")

	// Ptah fails the same config for the same reason, so the missing-file
	// control is not itself a divergence.
	c.Assert(err, qt.ErrorMatches, `.*no-such-file\.txt.*`)
}

// The consequence, stated as a measurement: the contents of a file outside the
// config directory reach a place the caller can see. This is what the sandbox
// costs a user and what removing it would buy an attacker who can commit an
// atlas.hcl.
func TestOraclePlacesOutsideFileContentsWhereTheCallerCanSeeThem(t *testing.T) {
	oracle := requireFileOracle(t)
	c := qt.New(t)

	base := t.TempDir()
	dir := filepath.Join(base, "project")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	outside := plantOracleSecret(c, base)
	path := writeOracleURLConfig(c, dir, outside)

	out, code := runFileOracle(c, oracle, dir)

	c.Assert(code, qt.Equals, 1, qt.Commentf("oracle output: %s", out))
	// The binary lowercases the scheme before reporting it, so the comparison
	// is on the lowercased output rather than on a rewritten expectation.
	c.Assert(strings.ToLower(out), qt.Contains, "ptahsecret1042")

	_, err := projectconfig.LoadAtlasFile(path, "local")

	c.Assert(err, qt.ErrorMatches, `.*absolute paths are not supported.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "ptahsecret1042")
}

func plantOracleSecret(c *qt.C, base string) string {
	c.Helper()

	path := filepath.Join(base, "secret.txt")
	c.Assert(os.WriteFile(path, []byte(fileOracleProbeURL), 0o600), qt.IsNil)
	return path
}

func quoteHCL(value string) string {
	return `"` + value + `"`
}

// The escapes below plant nothing, or one symbolic link, and report only
// whether that arrangement succeeded. None of them judges the binaries under
// measurement; the verdicts all live in the test bodies.

func escapeAbsolutePath(_, outside string) (string, error) {
	return outside, nil
}

func escapeParentTraversal(_, outside string) (string, error) {
	return "../" + filepath.Base(outside), nil
}

func escapeSymlinkedFile(dir, outside string) (string, error) {
	return "link.txt", os.Symlink(outside, filepath.Join(dir, "link.txt"))
}

func escapeSymlinkedDirectory(dir, outside string) (string, error) {
	return "outdir/" + filepath.Base(outside), os.Symlink(filepath.Dir(outside), filepath.Join(dir, "outdir"))
}

// writeOracleFileConfig puts the file() call in a position whose value is
// discarded: an unknown top-level attribute, which both the community binary
// and Ptah evaluate and then ignore. That isolates the read from everything a
// URL would then do with the value.
func writeOracleFileConfig(c *qt.C, dir, argument string) string {
	c.Helper()

	path := filepath.Join(dir, "atlas.hcl")
	body := "probe_value = file(" + quoteHCL(argument) + ")\n\n" +
		"env \"local\" {\n  url = \"sqlite://file?mode=memory\"\n  migration {\n    dir = \"file://migrations\"\n  }\n}\n"
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// writeOracleURLConfig puts the same read in the env URL, where the value is
// used and therefore visible.
func writeOracleURLConfig(c *qt.C, dir, argument string) string {
	c.Helper()

	path := filepath.Join(dir, "atlas.hcl")
	body := "env \"local\" {\n  url = file(" + quoteHCL(argument) + ")\n" +
		"  migration {\n    dir = \"file://migrations\"\n  }\n}\n"
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// runFileOracle runs `migrate status --env local` in dir and returns its
// combined output and exit code. The verb needs no database of its own beyond
// the in-memory SQLite the config names, so the whole measurement is local.
func runFileOracle(c *qt.C, oracle, dir string) (string, int) {
	c.Helper()

	warmUpFileOracle(c, oracle)

	cmd := exec.Command(oracle, "migrate", "status", "--env", "local")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	c.Assert(err == nil || asExitError(err, &exitErr), qt.IsTrue, qt.Commentf("running the oracle failed: %v", err))
	return string(out), cmd.ProcessState.ExitCode()
}

func asExitError(err error, target **exec.ExitError) bool {
	exitErr, ok := err.(*exec.ExitError) //nolint:errorlint // the run either exited or never started; nothing wraps it here
	if ok {
		*target = exitErr
	}
	return ok
}

// fileOracleWarmup absorbs the once-per-environment edition notice, which
// otherwise attaches itself to whichever measurement happens to run first.
var fileOracleWarmup sync.Once

func warmUpFileOracle(c *qt.C, oracle string) {
	c.Helper()

	fileOracleWarmup.Do(func() {
		dir := c.TempDir()
		//nolint:errcheck // best effort: a failed write still runs the oracle, which is all the warm-up needs
		_ = os.WriteFile(filepath.Join(dir, "atlas.hcl"),
			[]byte("env \"local\" {\n  url = \"sqlite://file?mode=memory\"\n}\n"), 0o600)
		cmd := exec.Command(oracle, "migrate", "status", "--env", "local")
		cmd.Dir = dir
		//nolint:errcheck // output and status are both discarded; this run exists only to spend the notice
		_, _ = cmd.CombinedOutput()
	})
}

func requireFileOracle(t *testing.T) string {
	t.Helper()

	oracle := os.Getenv(fileReferenceEnv)
	if oracle == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run the atlas.hcl file() conformance run",
			fileReferenceEnv, fileReferenceVersion)
	}

	out, err := exec.Command(oracle, "version").Output() // #nosec G204 G702 -- the oracle path is operator-provided via PTAH_ATLAS_REFERENCE
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", fileReferenceEnv, oracle, err)
	}
	got, _, _ := strings.Cut(string(out), "\n")
	if strings.TrimSpace(got) != fileReferenceVersion {
		t.Fatalf("%s=%s reports %q, want %q; a different build may have changed the rules under test",
			fileReferenceEnv, oracle, strings.TrimSpace(got), fileReferenceVersion)
	}
	return oracle
}
