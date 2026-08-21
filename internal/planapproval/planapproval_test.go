package planapproval_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/planapproval"
)

// An approval is a detached SSH signature over a plan's exact bytes, verified
// against an allowed-signers list the repository carries -- the mechanism git
// uses for signed commits, answering the same question: which known key vouched
// for this exact content (stokaro/ptah#1857).

// approvalFixture writes a plan, an SSH keypair, and an allowed_signers file
// naming that key, and returns their paths.
func approvalFixture(c *qt.C, principal string) (planPath, keyPath, allowedSigners string) {
	c.Helper()
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		c.Skip("ssh-keygen is not installed")
	}
	dir := c.TempDir()
	planPath = filepath.Join(dir, "plan.json")
	c.Assert(os.WriteFile(planPath,
		[]byte(`{"format_version":1,"name":"p","statements":[{"sql":"CREATE TABLE t (id int)"}]}`),
		0o600), qt.IsNil)

	keyPath = filepath.Join(dir, "id_ed25519")
	out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-C", principal,
		"-f", keyPath).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	pub, err := os.ReadFile(keyPath + ".pub")
	c.Assert(err, qt.IsNil)
	allowedSigners = filepath.Join(dir, "allowed_signers")
	// #nosec G703 -- allowedSigners is built from c.TempDir(); no external input reaches the path
	c.Assert(os.WriteFile(allowedSigners, []byte(principal+" "+string(pub)), 0o600), qt.IsNil)
	return planPath, keyPath, allowedSigners
}

func TestSignThenVerifyReportsTheSigner(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	planPath, keyPath, allowedSigners := approvalFixture(c, "alice@example.com")

	c.Assert(planapproval.Sign(ctx, planPath, keyPath), qt.IsNil)
	approval, err := planapproval.Verify(ctx, planapproval.VerifyOptions{
		PlanPath: planPath, AllowedSigners: allowedSigners,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(approval.Signer, qt.Equals, "alice@example.com")
	c.Assert(approval.Digest, qt.HasLen, 64)
}

// TestVerifyRefusesAPlanEditedAfterApproval is the property the whole feature
// exists for.
//
// The signature covers the plan's bytes, so a statement added after approval is
// a plan nobody approved -- which is exactly the case a review gate has to
// catch, and the case a signature over "the fields that matter" would miss the
// moment a new field appeared.
func TestVerifyRefusesAPlanEditedAfterApproval(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	planPath, keyPath, allowedSigners := approvalFixture(c, "alice@example.com")
	c.Assert(planapproval.Sign(ctx, planPath, keyPath), qt.IsNil)

	// One extra destructive statement, appended after the approval.
	c.Assert(os.WriteFile(planPath,
		[]byte(`{"format_version":1,"name":"p","statements":[{"sql":"CREATE TABLE t (id int)"},{"sql":"DROP TABLE users"}]}`),
		0o600), qt.IsNil)

	_, err := planapproval.Verify(ctx, planapproval.VerifyOptions{
		PlanPath: planPath, AllowedSigners: allowedSigners,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "either the plan changed after it was approved")
}

// TestVerifyRefusesAKeyTheRepositoryDoesNotList covers the other half: a real
// signature from a key nobody authorized.
func TestVerifyRefusesAKeyTheRepositoryDoesNotList(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	planPath, _, allowedSigners := approvalFixture(c, "alice@example.com")

	// A second, unlisted key signs the same plan.
	strangerDir := c.TempDir()
	strangerKey := filepath.Join(strangerDir, "id_ed25519")
	out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-C", "mallory@example.com",
		"-f", strangerKey).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(planapproval.Sign(ctx, planPath, strangerKey), qt.IsNil)

	_, err = planapproval.Verify(ctx, planapproval.VerifyOptions{
		PlanPath: planPath, AllowedSigners: allowedSigners,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "either the plan changed after it was approved")
}

// TestVerifyDistinguishesUnapprovedFromTampered keeps the two apart.
//
// "Nobody approved this" and "someone approved it and it does not check out"
// call for different actions, and a caller that could not tell them apart would
// report tampering where there was only an unreviewed plan.
func TestVerifyDistinguishesUnapprovedFromTampered(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	planPath, _, allowedSigners := approvalFixture(c, "alice@example.com")

	_, err := planapproval.Verify(ctx, planapproval.VerifyOptions{
		PlanPath: planPath, AllowedSigners: allowedSigners,
	})

	c.Assert(err, qt.ErrorIs, planapproval.ErrNoApproval)
}

// TestVerifyRefusesASignatureMadeForAnotherPurpose covers the namespace.
//
// Without it, any SSH signature over the same bytes -- made to sign a commit,
// or anything else -- would replay as a plan approval.
func TestVerifyRefusesASignatureMadeForAnotherPurpose(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	planPath, keyPath, allowedSigners := approvalFixture(c, "alice@example.com")

	out, err := exec.Command("ssh-keygen", "-Y", "sign", "-f", keyPath,
		"-n", "git", planPath).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	_, err = planapproval.Verify(ctx, planapproval.VerifyOptions{
		PlanPath: planPath, AllowedSigners: allowedSigners,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "either the plan changed after it was approved")
}

// TestDigestIsTheFileBytes pins what an approval attests to.
func TestDigestIsTheFileBytes(t *testing.T) {
	c := qt.New(t)

	first := planapproval.Digest([]byte(`{"a":1}`))
	same := planapproval.Digest([]byte(`{"a":1}`))
	other := planapproval.Digest([]byte(`{"a":2}`))

	c.Assert(first, qt.Equals, same)
	c.Assert(first, qt.Not(qt.Equals), other)
	c.Assert(first, qt.HasLen, 64)
}
