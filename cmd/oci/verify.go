package oci

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/ocireferrers"
	"go.5x5.cz/ptah/internal/ociverify"
)

const policyFlag = "policy"

type verifyOptions struct {
	policy    string
	format    string
	plainHTTP bool
}

func newVerifyCommand() *cobra.Command {
	opts := verifyOptions{}
	cmd := &cobra.Command{
		Use:   "verify <oci-reference>",
		Short: "Check an artifact against a verification policy before it is consumed",
		Long: `Check an artifact against a verification policy before it is consumed.

A digest proves two artifacts are the same bytes and an integrity sum proves a
migration directory is internally consistent. Neither answers whether these
bytes should be applied to your database: rewriting a migration and rehashing
it produces an artifact that passes both.

The policy is a YAML file. Every field is a refusal an operator opted into, and
a policy declaring no requirement is rejected rather than accepted, because not
passing a policy and passing an empty one are different mistakes:

    version: 1
    require_digest_pin: true
    artifact_types:
      - application/vnd.stokaro.ptah.migration.v1
    require_annotations:
      - org.opencontainers.image.source
      - org.opencontainers.image.revision
    require_signature: true

Every requirement is evaluated rather than stopping at the first failure, so a
pipeline being fixed gets the whole list instead of one violation per run.

Read require_signature for what it measures. Ptah checks that a signature is
ATTACHED. It does not verify one: no key is loaded, no identity is checked, no
cryptography runs. Signing and cryptographic verification stay with cosign or
Notation, which own the trust material. The check still earns its place because
the failure it catches is the common one -- a pipeline that was meant to sign
and did not -- but it must not be mistaken for authenticity.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVerify(cmd, args[0], opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.policy, policyFlag, "", "Path to the verification policy (required)")
	flags.StringVar(&opts.format, formatFlag, ocireferrers.FormatText, "Output format: text or json")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.ExactArgs(1))
	return cmd
}

func runVerify(cmd *cobra.Command, reference string, opts verifyOptions) error {
	if strings.TrimSpace(opts.policy) == "" {
		return fmt.Errorf("--%s is required: verification with no policy would check nothing", policyFlag)
	}
	if err := ocireferrers.ValidateFormat(opts.format); err != nil {
		return err
	}
	policy, err := ociverify.LoadPolicy(opts.policy)
	if err != nil {
		return err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.plainHTTP})
	if err != nil {
		return err
	}
	report, err := ociverify.Verify(cmd.Context(), client, reference, policy)
	if err != nil {
		return err
	}
	if err := writeVerify(cmd.OutOrStdout(), opts.format, report); err != nil {
		return err
	}
	return report.Err()
}

func writeVerify(w io.Writer, format string, report ociverify.Report) error {
	if strings.EqualFold(strings.TrimSpace(format), ocireferrers.FormatJSON) {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	fmt.Fprintf(w, "Reference: %s\n", report.Reference)
	fmt.Fprintf(w, "Digest:    %s\n", report.Digest)
	for _, requirement := range report.Satisfied {
		fmt.Fprintf(w, "  met      %s\n", requirement)
	}
	for _, finding := range report.Findings {
		fmt.Fprintf(w, "  REFUSED  %s: %s\n", finding.Requirement, finding.Detail)
	}
	return nil
}
