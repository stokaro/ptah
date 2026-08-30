package inference

// White-box testing required: the two halves under test -- writing a plan an
// approver can read, and turning a verified signature into an approval bound to
// that plan -- are reachable through the exported surface only from a cutover,
// which needs a live PostgreSQL, a prepared run and a verified generation. A
// black-box test would either need all of that to assert one file's contents,
// or would exercise the typed-digest path and assert nothing about the signed
// one.

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/planapproval"
)

// aPlan is the identity a cutover would produce.
func aPlan() planIdentity {
	return planIdentity{
		operation: "cutover",
		digest:    strings.Repeat("a", 64),
		lines: []string{
			"generation: " + strings.Repeat("b", 64),
			"replaces: " + strings.Repeat("c", 64),
			"target: public.articles.embedding_v2",
		},
	}
}

// TestWritePlanFile_SaysWhatIsBeingApproved is why a plan file is more than the
// digest.
//
// A signature over sixty-four hex characters attests to a number nobody could
// have checked. The file names the operation and what it would do, and the
// signature covers all of it.
func TestWritePlanFile_SaysWhatIsBeingApproved(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "cutover.plan")
	var out bytes.Buffer

	c.Assert(writePlanFile(&out, path, aPlan()), qt.IsNil)

	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Contains, "ptah inference cutover plan, format 1")
	c.Assert(string(body), qt.Contains, "target: public.articles.embedding_v2")
	c.Assert(string(body), qt.Contains, "plan: "+aPlan().digest)
	// And it says how to sign it, because the next thing whoever ran this does
	// is hand the file to somebody.
	c.Assert(out.String(), qt.Contains, "ptah schema approve --plan "+path)
	// Written for the approver rather than offered to the filesystem.
	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o600))
}

// TestApprovalFor_ASignatureNamesWhoApproved is the capability this closes.
//
// `--approve <digest> --approver "a name"` records what the operator typed. A
// verified signature records whose key covered these exact bytes, which is the
// question an audit is asking and the one a shell history cannot answer.
func TestApprovalFor_ASignatureNamesWhoApproved(t *testing.T) {
	c := qt.New(t)
	plan := aPlan()
	path, allowedSigners := signedPlanFor(c, plan, "alice@example.com")

	approval, err := approvalFor(context.Background(), approvalOptions{
		approvalFile: path, allowedSigners: allowedSigners,
	}, plan)

	c.Assert(err, qt.IsNil)
	c.Assert(approval, qt.IsNotNil)
	c.Assert(approval.Approver, qt.Equals, "alice@example.com")
	c.Assert(approval.PlanDigest, qt.Equals, plan.digest)
	// Signed, which is what a policy requiring one reads.
	c.Assert(approval.Signed, qt.IsTrue)
}

// TestApprovalFor_ASignatureOverAnotherPlanIsRefused is the half that makes the
// signature about THIS plan.
//
// The signature establishes that an allowed key covered these bytes. Without
// checking the digest inside them, a signature over any file an approver ever
// signed would authorize any cutover.
func TestApprovalFor_ASignatureOverAnotherPlanIsRefused(t *testing.T) {
	c := qt.New(t)
	signedPlan := aPlan()
	path, allowedSigners := signedPlanFor(c, signedPlan, "alice@example.com")
	// The evidence moved: same operator, same key, a different plan.
	current := aPlan()
	current.digest = strings.Repeat("d", 64)

	approval, err := approvalFor(context.Background(), approvalOptions{
		approvalFile: path, allowedSigners: allowedSigners,
	}, current)

	c.Assert(approval, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `.*was signed for plan [0-9a-f]+ and the plan built now is [0-9a-f]+.*`)
}

// TestApprovalFor_AnEditedPlanFileIsRefused is the case a signature exists for.
//
// The file is what was signed, so changing a line the approver read invalidates
// it -- including a line that changes what would be destroyed.
func TestApprovalFor_AnEditedPlanFileIsRefused(t *testing.T) {
	c := qt.New(t)
	plan := aPlan()
	path, allowedSigners := signedPlanFor(c, plan, "alice@example.com")
	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	edited := strings.Replace(string(body),
		"target: public.articles.embedding_v2", "target: public.articles.embedding_v9", 1)
	// #nosec G703 -- path comes from c.TempDir(); no external input reaches it
	c.Assert(os.WriteFile(path, []byte(edited), 0o600), qt.IsNil)

	approval, err := approvalFor(context.Background(), approvalOptions{
		approvalFile: path, allowedSigners: allowedSigners,
	}, plan)

	c.Assert(approval, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
}

// TestApprovalFor_AnUnsignedPlanFileSaysSoRatherThanReportingTampering keeps
// the two problems apart.
//
// A plan nobody reviewed and a plan whose approval does not check out call for
// different actions, and telling an operator their plan was tampered with when
// it was merely unreviewed sends them looking for an attacker.
func TestApprovalFor_AnUnsignedPlanFileSaysSoRatherThanReportingTampering(t *testing.T) {
	c := qt.New(t)
	plan := aPlan()
	path := filepath.Join(c.TempDir(), "cutover.plan")
	c.Assert(writePlanFile(&bytes.Buffer{}, path, plan), qt.IsNil)
	allowedSigners := writeAllowedSigners(c, "alice@example.com")

	_, err := approvalFor(context.Background(), approvalOptions{
		approvalFile: path, allowedSigners: allowedSigners,
	}, plan)

	c.Assert(err, qt.ErrorMatches, `.*carries no approval; sign it with .*`)
}

// TestApprovalFor_ATypedDigestIsNotASignature is the control.
//
// Without it, an implementation that marked every approval signed would satisfy
// the test above and make a policy requiring a signature accept a name.
func TestApprovalFor_ATypedDigestIsNotASignature(t *testing.T) {
	c := qt.New(t)
	plan := aPlan()

	approval, err := approvalFor(context.Background(), approvalOptions{
		digest: plan.digest, approver: "somebody",
	}, plan)

	c.Assert(err, qt.IsNil)
	c.Assert(approval, qt.IsNotNil)
	c.Assert(approval.Approver, qt.Equals, "somebody")
	c.Assert(approval.Signed, qt.IsFalse)
}

// TestApprovalFor_NoApprovalAtAllIsNotAnError is the plan waiting for a
// decision, which every verb prints the digest of.
func TestApprovalFor_NoApprovalAtAllIsNotAnError(t *testing.T) {
	c := qt.New(t)

	approval, err := approvalFor(context.Background(), approvalOptions{}, aPlan())

	c.Assert(err, qt.IsNil)
	c.Assert(approval, qt.IsNil)
}

// signedPlanFor writes a plan file and signs it with a fresh key.
func signedPlanFor(c *qt.C, plan planIdentity, principal string) (path, allowedSigners string) {
	c.Helper()
	requireSSHKeygen(c)
	dir := c.TempDir()
	path = filepath.Join(dir, "cutover.plan")
	c.Assert(writePlanFile(&bytes.Buffer{}, path, plan), qt.IsNil)

	keyPath, allowedSigners := aKeyFor(c, dir, principal)
	c.Assert(planapproval.Sign(context.Background(), path, keyPath), qt.IsNil)
	return path, allowedSigners
}

// writeAllowedSigners produces the approver list without signing anything.
func writeAllowedSigners(c *qt.C, principal string) string {
	c.Helper()
	requireSSHKeygen(c)
	_, allowedSigners := aKeyFor(c, c.TempDir(), principal)
	return allowedSigners
}

// aKeyFor makes a keypair and the allowed-signers file naming it.
func aKeyFor(c *qt.C, dir, principal string) (keyPath, allowedSigners string) {
	c.Helper()
	keyPath = filepath.Join(dir, "id_ed25519")
	out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-C", principal,
		"-f", keyPath).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	pub, err := os.ReadFile(keyPath + ".pub")
	c.Assert(err, qt.IsNil)
	allowedSigners = filepath.Join(dir, "allowed_signers")
	// #nosec G703 -- allowedSigners is built from c.TempDir(); no external input reaches the path
	c.Assert(os.WriteFile(allowedSigners, []byte(principal+" "+string(pub)), 0o600), qt.IsNil)
	return keyPath, allowedSigners
}

// requireSSHKeygen skips where OpenSSH is not installed.
//
// A skip rather than a failure: the signature is produced by ssh-keygen itself
// rather than by a reimplementation, which is the whole point of reusing the
// mechanism, and a machine without it cannot exercise the path at all.
func requireSSHKeygen(c *qt.C) {
	c.Helper()
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		c.Skip("ssh-keygen is not installed")
	}
}
