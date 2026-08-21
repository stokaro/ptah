// Package planapproval records and verifies that a human approved a specific
// migration plan, without an identity provider and without a service.
//
// # What an approval is here
//
// A detached SSH signature over the exact bytes of a saved plan file, verified
// against an allowed-signers list the repository carries. That is the same
// mechanism git uses for signed commits, and it answers the same question:
// which known key vouched for this exact content.
//
// The Cloud form of this needs an identity and a service because approval is a
// claim about a person, and a hosted product has somewhere to keep people.
// Ptah has neither and should not grow them (stokaro/ptah#1857). What it has
// instead is a plan with an identity -- statements bound to the fingerprints of
// the states they move between -- and that is enough to attest to.
//
// # Why the whole file rather than selected fields
//
// The signature covers the plan document byte for byte. A canonical digest over
// "the fields that matter" would need a rule for which fields matter, and every
// future field would inherit that decision silently: a plan that gained a
// destructive flag nobody signed would still verify. Signing the bytes means a
// plan that changed at all is a plan nobody approved.
//
// # Why Ptah never touches a private key
//
// Signing shells out to ssh-keygen. An approval is worth exactly as much as the
// key that made it, and a tool that reads private keys is a tool that can leak
// them. Verification shells out too, so the allowed-signers semantics are
// OpenSSH's own rather than a reimplementation that might differ at the edges
// -- principal matching, key revocation and validity windows included.
package planapproval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Namespace scopes the signature to this use.
//
// ssh-keygen refuses a signature made for another namespace, so a signature
// over some other document cannot be replayed as a plan approval even when the
// bytes happen to match.
const Namespace = "ptah-plan"

// SignatureSuffix is appended to a plan path to find its approval.
const SignatureSuffix = ".sig"

// ErrNoApproval reports a plan with no approval beside it.
//
// It is distinct from a failed verification on purpose: "nobody approved this"
// and "someone approved it and the signature does not check out" call for
// different actions, and a caller that could not tell them apart would report
// tampering where there was only an unreviewed plan.
var ErrNoApproval = errors.New("plan has no approval signature")

// Digest returns the identity an approval attests to: the plan file's bytes.
func Digest(plan []byte) string {
	sum := sha256.Sum256(plan)
	return hex.EncodeToString(sum[:])
}

// SignaturePath returns where a plan's approval lives.
func SignaturePath(planPath string) string {
	return planPath + SignatureSuffix
}

// Sign writes a detached approval for planPath using the operator's SSH key.
//
// The key is named, never read by Ptah: ssh-keygen opens it, prompts for a
// passphrase if it has one, and returns the armored signature.
func Sign(ctx context.Context, planPath, keyPath string) error {
	if strings.TrimSpace(keyPath) == "" {
		return errors.New("an SSH key path is required to sign a plan")
	}
	if _, err := os.Stat(planPath); err != nil {
		return fmt.Errorf("read plan %s: %w", planPath, err)
	}
	// ssh-keygen writes <input>.sig beside the input, which is the path
	// SignaturePath already names.
	cmd := exec.CommandContext(ctx, "ssh-keygen",
		"-Y", "sign", "-f", keyPath, "-n", Namespace, planPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ssh-keygen sign: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// VerifyOptions names what a verification checks against.
type VerifyOptions struct {
	// PlanPath is the plan whose approval is checked.
	PlanPath string
	// AllowedSigners is the OpenSSH allowed_signers file listing the keys whose
	// approval counts. Committing it to the repository is what makes the set of
	// approvers reviewable in the same place as the code.
	AllowedSigners string
	// Signer is the principal the signature must belong to. Empty accepts any
	// principal the allowed-signers file lists.
	Signer string
}

// Approval is a verified approval.
type Approval struct {
	// Signer is the principal ssh-keygen matched.
	Signer string
	// Digest is the plan identity the signature covers.
	Digest string
}

// Verify checks a plan's approval and reports who made it.
func Verify(ctx context.Context, opts VerifyOptions) (Approval, error) {
	if strings.TrimSpace(opts.AllowedSigners) == "" {
		return Approval{}, errors.New("an allowed-signers file is required to verify a plan approval")
	}
	if _, err := os.Stat(opts.AllowedSigners); err != nil {
		return Approval{}, fmt.Errorf("read allowed signers %s: %w", opts.AllowedSigners, err)
	}
	plan, err := os.ReadFile(opts.PlanPath)
	if err != nil {
		return Approval{}, fmt.Errorf("read plan %s: %w", opts.PlanPath, err)
	}
	signaturePath := SignaturePath(opts.PlanPath)
	if _, err := os.Stat(signaturePath); err != nil {
		if os.IsNotExist(err) {
			return Approval{}, fmt.Errorf("%w: %s", ErrNoApproval, signaturePath)
		}
		return Approval{}, fmt.Errorf("read approval %s: %w", signaturePath, err)
	}

	signer := opts.Signer
	if signer == "" {
		signer, err = principalFor(ctx, opts, plan, signaturePath)
		if err != nil {
			return Approval{}, err
		}
	}
	if err := runVerify(ctx, opts.AllowedSigners, signer, signaturePath, plan); err != nil {
		return Approval{}, err
	}
	return Approval{Signer: signer, Digest: Digest(plan)}, nil
}

// principalFor finds which listed principal the signature belongs to.
//
// ssh-keygen requires the principal up front rather than reporting it, so with
// no --signer given the only way to answer "who approved this" is to ask about
// each listed principal in turn. The alternative -- accepting any signature the
// file could verify without recording whose it was -- would make the approval
// unauditable, which is the one property it exists to have.
func principalFor(ctx context.Context, opts VerifyOptions, plan []byte, signaturePath string) (string, error) {
	principals, err := listPrincipals(opts.AllowedSigners)
	if err != nil {
		return "", err
	}
	if len(principals) == 0 {
		return "", fmt.Errorf("allowed signers %s lists no principals", opts.AllowedSigners)
	}
	for _, principal := range principals {
		if err := runVerify(ctx, opts.AllowedSigners, principal, signaturePath, plan); err == nil {
			return principal, nil
		}
	}
	// Both causes produce the same ssh-keygen failure, and naming only one
	// sends the operator to fix the wrong thing: a plan edited after approval
	// is by far the likelier of the two, and being told their key is
	// unauthorized would have them editing the signers file instead of looking
	// at the diff.
	return "", fmt.Errorf(
		"approval does not verify against %s: either the plan changed after it was approved, "+
			"or it was signed by a key that file does not list",
		opts.AllowedSigners)
}

func runVerify(ctx context.Context, allowedSigners, signer, signaturePath string, plan []byte) error {
	cmd := exec.CommandContext(ctx, "ssh-keygen",
		"-Y", "verify",
		"-f", allowedSigners,
		"-I", signer,
		"-n", Namespace,
		"-s", signaturePath)
	cmd.Stdin = bytes.NewReader(plan)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("approval does not verify for %q: %s",
			signer, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// listPrincipals reads the principals an allowed_signers file names.
//
// The format is `principal[,principal...] keytype key [comment]`, one per line,
// with # comments. Only the first field is read here; ssh-keygen does the
// cryptography.
func listPrincipals(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read allowed signers %s: %w", path, err)
	}
	var principals []string
	seen := make(map[string]bool)
	for line := range strings.SplitSeq(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) == 0 {
			continue
		}
		for principal := range strings.SplitSeq(fields[0], ",") {
			principal = strings.TrimSpace(principal)
			if principal == "" || seen[principal] {
				continue
			}
			seen[principal] = true
			principals = append(principals, principal)
		}
	}
	return principals, nil
}

// DefaultAllowedSigners is where a repository keeps its approver list.
func DefaultAllowedSigners(root string) string {
	return filepath.Join(root, ".ptah", "allowed_signers")
}
