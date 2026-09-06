package inference

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/internal/embedcutover"
	"ptah.run/internal/planapproval"
)

// approvalOptions are the flags a verb that needs authorization takes.
type approvalOptions struct {
	// digest is the plan digest an operator typed, and approver the name they
	// typed beside it.
	digest   string
	approver string
	// planFile is where a refused plan is written so that it can be signed.
	planFile string
	// approvalFile is a plan file whose signature is checked, and
	// allowedSigners the OpenSSH file listing whose signature counts.
	approvalFile   string
	allowedSigners string
	// signer requires the approval to belong to one principal.
	signer string
}

// addApprovalFlags registers them.
//
// --approve and --approval are mutually exclusive because they are two answers
// to one question, and the weaker of them would win by being easier to type.
func addApprovalFlags(cmd *cobra.Command, options *approvalOptions) {
	flags := cmd.Flags()
	flags.StringVar(&options.digest, "approve", "",
		"Plan digest this operation is approved for; run without it to see the digest")
	flags.StringVar(&options.approver, "approver", "", "Who approved it")
	flags.StringVar(&options.planFile, "plan-file", "",
		"Path to write the plan to when the operation is refused, so it can be signed")
	flags.StringVar(&options.approvalFile, "approval", "",
		"Path to a plan file signed with \"ptah schema approve\"; the approver is the "+
			"principal the signature verifies as")
	flags.StringVar(&options.allowedSigners, "allowed-signers", "",
		"OpenSSH allowed_signers file listing approvers (default: ./.ptah/allowed_signers)")
	flags.StringVar(&options.signer, "signer", "",
		"Require the approval to belong to this principal")
	requireExclusiveOnCommandLine(cmd, "approve", "approval")
	requireExclusiveOnCommandLine(cmd, "approver", "approval")
}

// planFileVersion is the format of the file an approver signs.
//
// 2 names every fact the plan digest binds, rather than four of them. The
// acceptance of blocking findings was the one that mattered: under
// policy.require_signed_approval the file is the whole of what a person sees,
// and two plans differing only in whether both blocking findings were accepted
// rendered byte-identical apart from the digest (stokaro/ptah#2739). The line
// naming the verification report is also spelled `verification report` now,
// because it sits beside `verification passed` and a digest called
// `verification` read as the verdict.
//
// Nothing in Ptah parses this number -- [planDigestIn] reads the digest line
// and ignores the rest -- so it moves for the reader outside this repository
// who does.
const planFileVersion = 2

// planDigestKey is the line the digest is read back from.
const planDigestKey = "plan"

// approvalFor resolves the authorization for one plan.
//
// Three answers, and the caller must be able to tell them apart. No approval at
// all is a plan waiting for a decision; a typed digest is somebody asserting
// they made it; a verified signature is a key that covered these exact bytes.
func approvalFor(
	ctx context.Context, options approvalOptions, plan planIdentity,
) (*embedcutover.Approval, error) {
	if strings.TrimSpace(options.approvalFile) != "" {
		return verifiedApproval(ctx, options, plan)
	}
	if options.digest == "" {
		return nil, nil
	}
	return &embedcutover.Approval{
		PlanDigest: expandPlanDigest(plan, options.digest),
		Approver:   options.approver, GrantedAt: time.Now().UTC(),
	}, nil
}

// verifiedApproval checks a signature and the plan it covers.
//
// Both halves are required and neither is enough. The signature establishes
// that an allowed key covered these bytes; the digest inside them establishes
// that the bytes are about THIS plan. Without the second, a signature over any
// file an approver ever signed would authorize any cutover.
func verifiedApproval(
	ctx context.Context, options approvalOptions, plan planIdentity,
) (*embedcutover.Approval, error) {
	verified, err := planapproval.Verify(ctx, planapproval.VerifyOptions{
		PlanPath:       options.approvalFile,
		AllowedSigners: effectiveAllowedSigners(options.allowedSigners),
		Signer:         options.signer,
	})
	if err != nil {
		if errors.Is(err, planapproval.ErrNoApproval) {
			// A file nobody signed and a signature that does not check out are
			// different problems, and telling an operator their plan was
			// tampered with when it was merely unreviewed sends them looking
			// for an attacker.
			return nil, fmt.Errorf(
				"%s carries no approval; sign it with "+
					"\"ptah schema approve --plan %s --key <key>\"",
				options.approvalFile, options.approvalFile)
		}
		return nil, err
	}

	signed, err := planDigestIn(options.approvalFile)
	if err != nil {
		return nil, err
	}
	if signed != plan.digest {
		return nil, fmt.Errorf(
			"%s was signed for plan %s and the plan built now is %s; the evidence has moved, "+
				"so write the plan again and have it approved again",
			options.approvalFile, shortDigest(signed), shortDigest(plan.digest))
	}
	return &embedcutover.Approval{
		PlanDigest: plan.digest, Approver: verified.Signer,
		Signed: true, GrantedAt: time.Now().UTC(),
	}, nil
}

// effectiveAllowedSigners falls back to the in-repository default.
//
// A path rather than a search, for the reason the schema surface gives: an
// approver list picked up from outside the working tree would make the gate
// depend on the machine running it, and a gate that means different things on
// two machines is not a gate.
func effectiveAllowedSigners(explicit string) string {
	if explicit != "" {
		return explicit
	}
	return planapproval.DefaultAllowedSigners(".")
}

// planIdentity is what a plan file says, for the two verbs that write one.
type planIdentity struct {
	// operation names what would be done, so an approver reading the file knows
	// which verb they are authorizing.
	operation string
	// digest is what an approval binds to.
	digest string
	// lines are the facts about the plan a person needs in order to decide.
	lines []string
}

// writePlanFile records a refused plan where an approver can read and sign it.
//
// The digest alone would be enough for the machine and useless for the person:
// a signature over sixty-four hex characters attests to a number nobody could
// have checked. The file names the operation and what it would do, and the
// signature covers all of it.
func writePlanFile(out io.Writer, path string, plan planIdentity) error {
	if path == "" {
		return nil
	}
	var body strings.Builder
	fmt.Fprintf(&body, "ptah inference %s plan, format %d\n", plan.operation, planFileVersion)
	for _, line := range plan.lines {
		body.WriteString(line + "\n")
	}
	body.WriteString(planDigestKey + ": " + plan.digest + "\n")

	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		// Reported rather than fatal, for the reason an unwritable evidence
		// file is: the plan is already on the terminal, and a directory that is
		// not there is not a fact about the generation.
		return writeLines(out, bullet(fmt.Sprintf("the plan was not written: %v", err)))
	}
	return writeLines(out, bullet(fmt.Sprintf(
		"plan written to %s; sign it with "+
			"\"ptah schema approve --plan %s --key <key>\"",
		path, path)))
}

// planDigestIn reads the digest out of a signed plan file.
func planDigestIn(path string) (string, error) {
	body, err := os.ReadFile(path) //gosec:disable G304 -- the operator named this file on the command line
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	for line := range strings.SplitSeq(string(body), "\n") {
		if digest, found := strings.CutPrefix(strings.TrimSpace(line), planDigestKey+": "); found {
			return strings.TrimSpace(digest), nil
		}
	}
	return "", fmt.Errorf("%s carries no %q line, so there is nothing to check the signature against",
		path, planDigestKey)
}

// expandPlanDigest accepts the short digest a person reads off the terminal.
//
// A full digest is sixty-four characters and nobody retypes one correctly.
// Matching the short form against THIS plan is safe because it is only ever
// compared to this plan: a short form that does not match leaves the caller's
// own string, which then fails the comparison and names both.
func expandPlanDigest(plan planIdentity, digest string) string {
	if digest == plan.short() {
		return plan.digest
	}
	return digest
}

// short is the digest a person quotes.
func (p planIdentity) short() string {
	return shortDigest(p.digest)
}
