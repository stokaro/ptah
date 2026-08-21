package schema

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/planapproval"
)

const (
	approvalPlanFlag           = "plan"
	approvalKeyFlag            = "key"
	approvalAllowedSignersFlag = "allowed-signers"
	approvalSignerFlag         = "signer"
)

// newSchemaApproveCommand returns `schema approve`.
//
// It is a sibling of `schema plan` rather than a child of it. This repository
// treats a command with children as a namespace that accepts no flags of its
// own -- nativeFlagUsages says so -- so attaching these to `schema plan` would
// have taken --schema-file off a verb that has always had it.
//
// Approval is recorded as a detached SSH signature over the plan file, the same
// mechanism git uses for signed commits. The Cloud form of this needs an
// identity and a service because approval is a claim about a person; Ptah has
// neither and should not grow them, so what it attests to is the plan
// (stokaro/ptah#1857).
func newSchemaApproveCommand() *cobra.Command {
	var planPath, keyPath string
	cmd := &cobra.Command{
		Use:   "approve",
		Short: "Sign a saved plan, recording that it was reviewed",
		Long: `Record approval of a saved plan by signing it with an SSH key.

The signature covers the plan file byte for byte and is written beside it as
<plan>.sig. A plan edited after approval no longer verifies, which is the case
a review gate exists to catch.

Ptah never reads the private key: ssh-keygen opens it, prompts for a passphrase
if it has one, and writes the signature. The key that signs is worth exactly
what the approval is worth.

"ptah schema verify-approval" checks the result, and "ptah schema apply --plan
--require-approval" refuses to execute a plan that does not verify.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if planPath == "" {
				return cmdutil.Fail(cmd, fmt.Errorf("--%s is required", approvalPlanFlag))
			}
			if err := planapproval.Sign(cmd.Context(), planPath, keyPath); err != nil {
				return cmdutil.Fail(cmd, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Approved %s\nSignature: %s\n",
				planPath, planapproval.SignaturePath(planPath))
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&planPath, approvalPlanFlag, "", "Path to the saved plan file (required)")
	flags.StringVar(&keyPath, approvalKeyFlag, "", "SSH private key that signs the approval (required)")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// newSchemaVerifyApprovalCommand returns `schema verify-approval`.
func newSchemaVerifyApprovalCommand() *cobra.Command {
	var planPath, allowedSigners, signer string
	cmd := &cobra.Command{
		Use:   "verify-approval",
		Short: "Check that a saved plan carries an approval from an allowed signer",
		Long: `Verify the approval beside a saved plan against an allowed-signers file.

The allowed-signers file is OpenSSH's own format and belongs in the repository:
committing it is what makes the set of approvers reviewable in the same place
as the code, and changing it a reviewable change.

Three outcomes are kept apart, because they call for different actions:
unapproved (no signature beside the plan), unverifiable (a signature no allowed
key accounts for, which includes a plan edited after approval), and approved,
which reports who approved it.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if planPath == "" {
				return cmdutil.Fail(cmd, fmt.Errorf("--%s is required", approvalPlanFlag))
			}
			approval, err := planapproval.Verify(cmd.Context(), planapproval.VerifyOptions{
				PlanPath:       planPath,
				AllowedSigners: effectiveAllowedSigners(allowedSigners),
				Signer:         signer,
			})
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Approved by %s\nPlan digest: %s\n",
				approval.Signer, approval.Digest)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&planPath, approvalPlanFlag, "", "Path to the saved plan file (required)")
	flags.StringVar(&allowedSigners, approvalAllowedSignersFlag, "",
		"OpenSSH allowed_signers file listing approvers (default: ./.ptah/allowed_signers)")
	flags.StringVar(&signer, approvalSignerFlag, "",
		"Require the approval to belong to this principal")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// effectiveAllowedSigners falls back to the in-repository default.
//
// The default is a path rather than a search: an approver list that could be
// picked up from outside the working tree would make the gate depend on the
// machine running it, and a gate that means different things on two machines is
// not a gate.
func effectiveAllowedSigners(explicit string) string {
	if explicit != "" {
		return explicit
	}
	return planapproval.DefaultAllowedSigners(".")
}

// requirePlanApproval enforces --require-approval before a plan executes.
//
// It reports the unapproved case in its own words rather than as a verification
// failure: a plan nobody reviewed and a plan whose approval does not check out
// are different problems, and telling an operator their plan was tampered with
// when it was merely unreviewed sends them looking for an attacker.
func requirePlanApproval(cmd *cobra.Command, planPath, allowedSigners, signer string) error {
	approval, err := planapproval.Verify(cmd.Context(), planapproval.VerifyOptions{
		PlanPath:       planPath,
		AllowedSigners: effectiveAllowedSigners(allowedSigners),
		Signer:         signer,
	})
	if err != nil {
		if errors.Is(err, planapproval.ErrNoApproval) {
			return fmt.Errorf(
				"--require-approval: %s carries no approval; sign it with `ptah schema approve`", planPath)
		}
		return fmt.Errorf("--require-approval: %w", err)
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "Plan approved by %s\n", approval.Signer)
	return nil
}
