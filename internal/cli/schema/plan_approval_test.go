package schema_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/cli/schema"
	"ptah.run/migration/safety"
)

// `--require-approval` is the gate: a plan nobody reviewed must not execute.
// Approval itself is a detached SSH signature over the plan, so Ptah needs no
// identity provider and no service (stokaro/ptah#1857).

// approvedPlanFixture writes a plan, a keypair, and an allowed_signers file,
// and returns the directory plus the plan path.
func approvedPlanFixture(c *qt.C) (dir, planPath, keyPath string) {
	c.Helper()
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		c.Skip("ssh-keygen is not installed")
	}
	dir = c.TempDir()
	planPath = filepath.Join(dir, "plan.json")
	c.Assert(os.WriteFile(planPath, []byte(
		`{"format_version":1,"name":"p","dialect":"sqlite","from_fingerprint":"a","to_fingerprint":"b","statements":[{"sql":"CREATE TABLE t (id int)"}]}`,
	), 0o600), qt.IsNil)

	keyPath = filepath.Join(dir, "key")
	out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-C", "alice@example.com",
		"-f", keyPath).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	pub, err := os.ReadFile(keyPath + ".pub")
	c.Assert(err, qt.IsNil)
	signers := filepath.Join(dir, "allowed_signers")
	// #nosec G703 -- signers is built from c.TempDir(); no external input reaches the path
	c.Assert(os.WriteFile(signers, []byte("alice@example.com "+string(pub)), 0o600), qt.IsNil)
	return dir, planPath, keyPath
}

func signersPath(dir string) string { return filepath.Join(dir, "allowed_signers") }

func TestSchemaApproveThenVerifyNamesTheSigner(t *testing.T) {
	c := qt.New(t)
	dir, planPath, keyPath := approvedPlanFixture(c)

	out, err := runSchema("", "approve", "--plan", planPath, "--key", keyPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runSchema("", "verify-approval", "--plan", planPath,
		"--allowed-signers", signersPath(dir))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Approved by alice@example.com")
}

// TestSchemaApplyRequireApprovalRefusesAnUnapprovedPlan is the gate.
func TestSchemaApplyRequireApprovalRefusesAnUnapprovedPlan(t *testing.T) {
	c := qt.New(t)
	dir, planPath, _ := approvedPlanFixture(c)

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+filepath.Join(dir, "never-reached.db"),
		"--plan", planPath, "--require-approval",
		"--allowed-signers", signersPath(dir), "--auto-approve")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "carries no approval")
}

// TestSchemaApplyRequireApprovalRefusesAPlanEditedAfterApproval is the case the
// whole feature exists for: approval that stops meaning anything the moment the
// reviewed content changes.
func TestSchemaApplyRequireApprovalRefusesAPlanEditedAfterApproval(t *testing.T) {
	c := qt.New(t)
	dir, planPath, keyPath := approvedPlanFixture(c)
	out, err := runSchema("", "approve", "--plan", planPath, "--key", keyPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// A destructive statement appended after the reviewer signed.
	c.Assert(os.WriteFile(planPath, []byte(
		`{"format_version":1,"name":"p","dialect":"sqlite","from_fingerprint":"a","to_fingerprint":"b","statements":[{"sql":"CREATE TABLE t (id int)"},{"sql":"DROP TABLE users"}]}`,
	), 0o600), qt.IsNil)

	out, err = runSchema("", "apply",
		"--db-url", "sqlite://"+filepath.Join(dir, "never-reached.db"),
		"--plan", planPath, "--require-approval",
		"--allowed-signers", signersPath(dir), "--auto-approve")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "either the plan changed after it was approved")
}

// TestSchemaApplyWithoutRequireApprovalDoesNotGate is the control: the gate is
// opt-in, and without it the plan is not checked for an approval at all.
//
// Failing on plan validity rather than on approval is the observation that
// separates them: execution got past where the gate would have stopped it.
func TestSchemaApplyWithoutRequireApprovalDoesNotGate(t *testing.T) {
	c := qt.New(t)
	dir, planPath, _ := approvedPlanFixture(c)
	c.Assert(os.WriteFile(planPath, []byte(`{"format_version":1,"name":"p"}`), 0o600), qt.IsNil)

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+filepath.Join(dir, "never-reached.db"),
		"--plan", planPath, "--auto-approve")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Not(qt.Contains), "approval")
}

// TestSchemaVerifyApprovalRequiresAPlan keeps a bare invocation from reading as a
// passing verification.
func TestSchemaVerifyApprovalRequiresAPlan(t *testing.T) {
	c := qt.New(t)

	out, err := runSchema("", "verify-approval")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "--plan is required")
}

// TestSchemaApplyReportsMissingApprovalBeforeAnyOtherPlanComplaint pins the
// ordering the gate needs.
//
// The plan here is both unapproved AND malformed. Whichever check runs first
// decides what the operator is told, and "this plan is invalid" would hide that
// nobody reviewed it either -- they would fix the syntax, re-run, and only then
// discover the gate. The approval answer has to come first.
//
// This exists because a mutation that moved the gate after the plan was parsed
// survived every other test here: with a well-formed plan the two orderings are
// indistinguishable, so only a plan that fails both checks separates them.
func TestSchemaApplyReportsMissingApprovalBeforeAnyOtherPlanComplaint(t *testing.T) {
	c := qt.New(t)
	dir, planPath, _ := approvedPlanFixture(c)
	// Well-formed JSON, but missing the fingerprints ReadPlanFile requires.
	c.Assert(os.WriteFile(planPath, []byte(`{"format_version":1,"name":"p"}`), 0o600), qt.IsNil)

	out, err := runSchema("", "apply",
		"--db-url", "sqlite://"+filepath.Join(dir, "never-reached.db"),
		"--plan", planPath, "--require-approval",
		"--allowed-signers", signersPath(dir), "--auto-approve")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "carries no approval")
	c.Assert(out+err.Error(), qt.Not(qt.Contains), "from_fingerprint is required")
}

// swapOnApproval is standard error for a run whose plan file is replaced the
// moment the command reports the approval verified. That line is written after
// the signature check and before the plan is decoded, which is the window in
// which a second read of the file would find the replacement.
type swapOnApproval struct {
	bytes.Buffer
	planPath    string
	replacement []byte
	swapped     bool
	swapErr     error
}

func (w *swapOnApproval) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if !w.swapped && strings.Contains(w.String(), "Plan approved by ") {
		w.swapped = true
		w.swapErr = os.WriteFile(w.planPath, w.replacement, 0o600)
	}
	return n, err
}

// TestSchemaApplyRequireApprovalExecutesTheBytesItVerified is the approval
// gate against a file replaced after the check. The replacement is a plan for
// the same database state whose one statement nobody reviewed, and it lands on
// disk between the signature check and the decode. What runs has to be what
// was verified: the reviewed statement, under the reviewed digest.
func TestSchemaApplyRequireApprovalExecutesTheBytesItVerified(t *testing.T) {
	c := qt.New(t)
	approverDir, _, keyPath := approvedPlanFixture(c)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	out, err := runSchema("", "approve", "--plan", planPath, "--key", keyPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	approved, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	unreviewed := plan
	unreviewed.Statements = []atlasschema.PlanStatement{
		{SQL: `CREATE TABLE "unreviewed" ("id" INTEGER PRIMARY KEY)`, Severity: safety.Safe},
	}
	replacement, err := atlasschema.MarshalPlanFile(unreviewed)
	c.Assert(err, qt.IsNil)
	stderr := &swapOnApproval{planPath: planPath, replacement: replacement}
	var stdout bytes.Buffer
	cmd := schema.NewSchemaCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(stderr)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs([]string{"apply",
		"--db-url", f.dbURL, "--plan", planPath, "--require-approval",
		"--allowed-signers", signersPath(approverDir), "--auto-approve", "--json"})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stderr.swapped, qt.IsTrue, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stderr.swapErr, qt.IsNil)
	c.Assert(fileDigest(c, planPath), qt.Equals, digest.FromBytes(replacement).String())
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout.String()), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeApplied,
		PlanName:        plan.Name,
		PlanDigest:      digest.FromBytes(approved).String(),
		Statements:      plan.StatementSQL(),
	})
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"orders", "users"})
}

// TestSchemaApplyJSONReportsAnUnapprovedPlanAsFailed is the gate's refusal in
// the document. It has no refusal code, and it comes before the plan is
// decoded, so the document names no plan.
func TestSchemaApplyJSONReportsAnUnapprovedPlanAsFailed(t *testing.T) {
	c := qt.New(t)
	approverDir, _, _ := approvedPlanFixture(c)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", planPath, "--require-approval",
		"--allowed-signers", signersPath(approverDir), "--auto-approve", "--json")

	c.Assert(err, qt.ErrorMatches, `--require-approval: .*add-orders\.plan\.json carries no approval; sign it with `+"`ptah schema approve`")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeFailed,
		Error:           err.Error(),
	})
	c.Assert(stderr, qt.Equals, "error: "+err.Error()+"\n")
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
}

// TestSchemaApplyJSONReportsAPlanEditedAfterApprovalAsFailed is the refusal a
// review gate exists for, in the document.
func TestSchemaApplyJSONReportsAPlanEditedAfterApprovalAsFailed(t *testing.T) {
	c := qt.New(t)
	approverDir, _, keyPath := approvedPlanFixture(c)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	out, err := runSchema("", "approve", "--plan", planPath, "--key", keyPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	plan.Statements = append(plan.Statements, atlasschema.PlanStatement{SQL: `DROP TABLE "users"`, Severity: safety.Destructive})
	edited, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(planPath, edited, 0o600), qt.IsNil)

	stdout, _, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", planPath, "--require-approval",
		"--allowed-signers", signersPath(approverDir), "--auto-approve", "--json")

	c.Assert(err, qt.ErrorMatches, `--require-approval: approval does not verify against .*: either the plan changed after it was approved.*`)
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeFailed,
		Error:           err.Error(),
	})
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
}
