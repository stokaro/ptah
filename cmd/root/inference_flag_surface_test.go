package root_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/root"
)

// runNativeRoot drives the real binary's command tree, which is the only place
// the environment binding is installed. cmd/inference declares the flag groups
// this file is about, but a test constructing inference.NewCommand() alone
// would never apply a PTAH_* value and so could not observe the defect.
func runNativeRoot(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := root.NewRootCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return output.String(), err
}

// writeInferenceSpecFile puts a document somewhere PTAH_SPEC can name it. The
// contents never have to be valid: every assertion below is about which source
// the run chose, and it is decided before anything reads the file.
func writeInferenceSpecFile(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "spec.yaml")
	c.Assert(os.WriteFile(path, []byte("version: 1\n"), 0o600), qt.IsNil)
	return path
}

// TestInferenceReleaseIsNotRefusedByAnExportedSpec covers stokaro/ptah#2648
// finding 11. The inference quick start instructs `export PTAH_SPEC=...`, and
// --release is the promotion path every other page points at; with both, the
// group refused a command line carrying one flag and named a second the
// operator never typed.
//
// The assertion is that the run reached the registry, because that is what
// separates the fix from the two ways of getting it wrong: refusing the run,
// and answering it from the exported file without contacting anything.
func TestInferenceReleaseIsNotRefusedByAnExportedSpec(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_SPEC", writeInferenceSpecFile(c))

	output, err := runNativeRoot(c, "inference", "describe", "--release", "oci://127.0.0.1:9/x:y")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "fetch the release oci://127.0.0.1:9/x:y")
	c.Assert(output, qt.Not(qt.Contains), "none of the others can be")
}

// TestInferenceReleaseReachesTheRegistryWithoutTheVariable is the control that
// makes the test above about the variable rather than about the reference. The
// two runs have to fail the same way, or the exported specification is still
// deciding something.
func TestInferenceReleaseReachesTheRegistryWithoutTheVariable(t *testing.T) {
	c := qt.New(t)

	_, err := runNativeRoot(c, "inference", "describe", "--release", "oci://127.0.0.1:9/x:y")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "fetch the release oci://127.0.0.1:9/x:y")
}

// TestInferenceSpecAndReleaseTogetherAreStillRefused proves the run above was
// not bought by giving up the group. Two flags typed is the case the group
// exists for, and cobra's own sentence is what the operator has always seen.
func TestInferenceSpecAndReleaseTogetherAreStillRefused(t *testing.T) {
	c := qt.New(t)

	_, err := runNativeRoot(c,
		"inference", "describe",
		"--spec", writeInferenceSpecFile(c),
		"--release", "oci://127.0.0.1:9/x:y",
	)

	c.Assert(err, qt.ErrorMatches,
		`if any flags in the group \[spec release\] are set none of the others can be; `+
			`\[release spec\] were all set`)
}

// TestInferenceEveryGroupOnAMultiGroupCommandIsChecked is the test the first
// version of this file did not have, and its absence was a real hole.
//
// `cutover` declares three exclusive groups. They compose onto one PreRunE
// chain, and a chain that dropped what came before it would leave only the
// last-registered group checked -- measured: `cutover --spec X --release Y`
// stopped being refused and went on to load the specification. Every other
// assertion here drives `describe`, which has one group, so none of them can
// see it.
func TestInferenceEveryGroupOnAMultiGroupCommandIsChecked(t *testing.T) {
	c := qt.New(t)

	_, err := runNativeRoot(c,
		"inference", "cutover",
		"--spec", writeInferenceSpecFile(c),
		"--release", "oci://127.0.0.1:9/x:y",
		"--db-url", "postgres://user@127.0.0.1:1/db",
		"--run-id", "a-run",
	)

	c.Assert(err, qt.ErrorMatches,
		`if any flags in the group \[spec release\] are set none of the others can be; `+
			`\[release spec\] were all set`)
}

// TestInferenceApprovalIsNotRefusedByAnExportedApprove covers the second group
// finding 11 names. It is a separate command from the one in the report, which
// is the point: the defect belonged to every group whose members are
// environment-bound, not to `describe`.
func TestInferenceApprovalIsNotRefusedByAnExportedApprove(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_APPROVE", "someone@example.com")

	_, err := runNativeRoot(c,
		"inference", "cutover",
		"--spec", writeInferenceSpecFile(c),
		"--db-url", "postgres://user@127.0.0.1:1/db",
		"--run-id", "a-run",
		"--approval", filepath.Join(c.TempDir(), "plan.txt"),
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "none of the others can be")
}

// TestInferenceBackfillDoesNotAdvertiseMaintainFor covers finding 10. The flag
// was registered on both running verbs and read only by catchup, so a backfill
// carrying it exited 0, wrote no window, and left an operator believing they
// had a way back. Refusing it is the honest answer: a backfill is not what
// keeps a previous generation current.
func TestInferenceBackfillDoesNotAdvertiseMaintainFor(t *testing.T) {
	c := qt.New(t)

	help, err := runNativeRoot(c, "inference", "backfill", "--help")

	c.Assert(err, qt.IsNil)
	c.Assert(help, qt.Not(qt.Contains), "--maintain-for")

	_, err = runNativeRoot(c,
		"inference", "backfill",
		"--spec", writeInferenceSpecFile(c),
		"--maintain-for", "2h",
	)
	c.Assert(err, qt.ErrorMatches, `unknown flag: --maintain-for`)
}

// TestInferenceCatchUpStillTakesMaintainFor is the control for the removal.
// Without it, deleting the capability outright would read as the fix.
func TestInferenceCatchUpStillTakesMaintainFor(t *testing.T) {
	c := qt.New(t)

	help, err := runNativeRoot(c, "inference", "catchup", "--help")

	c.Assert(err, qt.IsNil)
	c.Assert(help, qt.Contains, "--maintain-for")
	c.Assert(help, qt.Contains, "PTAH_MAINTAIN_FOR")
}

// TestInferenceDescribeChecksFormatBeforeTheNetwork covers finding 12. A run
// refused for a value the operator typed should not first resolve a mutable
// reference over the network; probe and status already check first, and
// describe was the verb that did not.
func TestInferenceDescribeChecksFormatBeforeTheNetwork(t *testing.T) {
	c := qt.New(t)

	_, err := runNativeRoot(c,
		"inference", "describe", "--release", "oci://127.0.0.1:9/x:y", "--format", "bogus")

	c.Assert(err, qt.ErrorMatches, `invalid --format value "bogus": text or json`)
}

// TestInferenceDescribeStillResolvesForAGoodFormat is the control: a format the
// verb accepts must not short-circuit the resolution the run asked for.
func TestInferenceDescribeStillResolvesForAGoodFormat(t *testing.T) {
	c := qt.New(t)

	_, err := runNativeRoot(c,
		"inference", "describe", "--release", "oci://127.0.0.1:9/x:y", "--format", "json")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "fetch the release")
}
