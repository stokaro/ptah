package atlas

import (
	"errors"
	"strings"

	"github.com/spf13/cobra"
)

// This file owns the Atlas-compatible surface's `--dev-url` error boundary, the
// sibling stokaro/ptah#1451 measured and deliberately left open: `--dev-url`
// reaches the same client layer as `--url` and prints the same words, but it
// does so on a different set of verbs, and those verbs order it differently.
//
// Everything below is measured against the pinned community binary v1.3.0 on
// 2026-08-13, each exit status read from an unpiped invocation with standard
// output and standard error captured to separate files. Twelve compat verbs
// register `--dev-url`; six of them have a row on that binary and six answer
// `unknown flag: --dev-url` because the community version does not carry the
// flag at all. The measured contract for the six, every message on standard
// error with standard output empty and exit status 1 unless stated:
//
//	verb              --dev-url absent                  --dev-url ""                    --dev-url notadriver://x
//	migrate diff      required flag(s) "dev-url" not set  sql/sqlclient: missing driver   unknown driver
//	migrate lint      required flag(s) "dev-url" not set  sql/sqlclient: missing driver   unknown driver
//	migrate validate  exit 0, both streams empty        exit 0, both streams empty      unknown driver
//	schema apply      exit 0, plan on standard output   exit 0, plan on standard output unknown driver
//	schema diff       --dev-url cannot be empty         --dev-url cannot be empty       unknown driver
//	schema inspect    --dev-url cannot be empty         --dev-url cannot be empty       unknown driver
//
// Three facts are encoded there, and matching one does not match the others:
//
//   - The unknown-driver row is the same string `--url` already produces, so it
//     is spelled once in [atlasUnknownDriverError] and reused rather than
//     copied. It is the only row every one of the six shares, and before this
//     change not one of them printed it: five answered `unsupported --dev-url
//     dialect "notadriver://x"` and `migrate validate` wrapped a connector error
//     into 130 bytes.
//
//   - The absent/empty split, which `--url` also has but on different verbs.
//     `migrate diff` and `migrate lint` refuse an absent flag with cobra's own
//     required-flag wording; an explicitly empty value passes that check and is
//     answered by the client layer instead, because the check asks pflag's
//     Changed bit and not the string. A gate that refused an empty value up
//     front could not produce the missing-driver row at all.
//
//   - Placement. On `migrate diff` the migration directory's integrity gate
//     outranks the required-flag refusal -- measured, an unhashed directory with
//     no `--dev-url` prints `checksum file not found` -- while on `migrate lint`
//     the refusal wins over an unhashed directory. That is cobra's own order:
//     ValidateRequiredFlags runs after the pre-run hooks, so a verb that gates
//     integrity in a hook answers first and a verb that does not answers second.
//     The two call sites below sit on the two sides of that line deliberately.
//
// cobra's MarkFlagRequired is deliberately NOT used, for the same reason
// stokaro/ptah#1451 gave for `--url`: it tests pflag's Changed bit, which an
// atlas.hcl `dev` attribute never sets in this build. Measured on the pinned
// binary, `migrate lint --env local` with an env carrying `dev` exits 0, and
// ptah-compat exits 0 on it too. Marking the flag required would refuse both and
// delete a capability the surface has (AGENTS.md compatibility rule (c)). The
// refusals below therefore read the MERGED value and ask the Changed bit only to
// separate "never given" from "given empty".
//
// `docker://` values are passed through untouched, and what happens to them
// afterwards changed with stokaro/ptah#1468: they are now PROVISIONED into a
// real container rather than refused. Answering them here as an unknown driver
// would therefore not merely replace a clear diagnostic with a vague one, it
// would delete the capability, which is compatibility rule (c). The refusals
// that remain for a `docker://` value are the ones decidable from the URL text
// -- an image no engine table names, a form the pinned community binary rejects
// -- and they are internal/devdocker's, in that binary's own words. Every verb
// in this family reaches the provisioner: `migrate test` and `schema test` were
// the last two refusing the scheme outright, and stokaro/ptah#844 wired their
// native runners to it.
//
// None of this reaches native Ptah. `ptah schema inspect`, `ptah migrations
// lint` and the rest keep `unsupported --dev-url dialect "notadriver://x"`,
// which names the flag the operator has to change and quotes the whole value
// they typed. The community wording names a DRIVER problem when the real problem
// is a flag value, and it quotes only the scheme, so `notadriver://x` is
// reported as `"notadriver"` and the operator cannot see what they wrote.
// Matching it is this surface's contract, not an improvement (AGENTS.md
// compatibility rule (b)), so the clearer wording stays where it is not a
// compatibility promise. Measured before and after this change, native
// `schema inspect`, `schema diff`, `schema apply`, `schema plan`,
// `migrations lint`, `migrations validate` and `migrations generate` are
// byte-identical on both streams.

// atlasRequiredDevURLMessage is cobra's own ValidateRequiredFlags wording, which
// `migrate diff` and `migrate lint` print for a `--dev-url` that was never
// given. It is the plural spelling on both; unlike `--url`, no verb in this
// family was measured using the singular one.
const atlasRequiredDevURLMessage = `required flag(s) "dev-url" not set`

// atlasDevURLDockerScheme names the values this boundary must not answer. See
// the file comment: Ptah's own docker refusal is clearer than an unknown-driver
// verdict and is pinned by its own tests.
const atlasDevURLDockerScheme = "docker"

// atlasRequiredDevURLError returns the refusal for a `--dev-url` the verb
// requires before it opens anything.
func atlasRequiredDevURLError() error {
	return errors.New(atlasRequiredDevURLMessage)
}

// atlasDevURLOpenDiagnostic reports how this surface refuses rawURL as a dev
// database URL, or nil when the value is one the connector should be given a
// chance to open and report on itself.
//
// It answers an empty value, because on `migrate diff` and `migrate lint` an
// explicitly empty `--dev-url` is opened rather than refused, and the client
// layer's missing-driver verdict is what the pinned binary prints. It shares
// [atlasDatabaseURLDiagnostic] with the `--url` boundary instead of restating
// the two verdicts, so the two flags cannot drift apart on a scheme one of them
// learns to open later.
func atlasDevURLOpenDiagnostic(rawURL string) error {
	if atlasDevURLAnsweredElsewhere(rawURL) {
		return nil
	}
	return atlasDatabaseURLDiagnostic(rawURL)
}

// atlasDevURLDriverDiagnostic reports only the unknown-driver verdict, leaving
// an empty value to the caller's own refusal.
//
// It is the verb family whose empty `--dev-url` is not a driver question:
// `migrate validate` accepts one and exits 0, and the three schema verbs each
// answer it with a sentence about the dev database rather than about a driver.
// Those refusals are measured separately and keep their own wording; this only
// adds the row they all share.
func atlasDevURLDriverDiagnostic(rawURL string) error {
	if strings.TrimSpace(rawURL) == "" {
		return nil
	}
	return atlasDevURLOpenDiagnostic(rawURL)
}

// atlasDevURLGiven reports whether the operator supplied a dev database URL at
// all, by either spelling this surface accepts.
//
// The flag's Changed bit alone is not the answer: an atlas.hcl env supplying
// `dev` never sets it, because project configuration is resolved inside RunE and
// pflag only records a command-line assignment. The merged value alone is not
// the answer either, because it cannot tell an absent flag from `--dev-url ""`.
// Both are needed, and the pair is what separates the required-flag row from the
// missing-driver row.
func atlasDevURLGiven(cmd *cobra.Command, mergedDevURL string) bool {
	return cmd.Flags().Changed("dev-url") || strings.TrimSpace(mergedDevURL) != ""
}

// atlasDevURLInput is a verb's resolved `--dev-url`: the merged value, and
// whether the operator supplied one at all.
//
// The two travel together because neither answers the cell alone, and because
// they are settled at different moments from where they are used -- `migrate
// diff` resolves them before its directory gate runs and refuses several checks
// later, by which point the flag set no longer tells the same story.
type atlasDevURLInput struct {
	value string
	given bool
}

// atlasDevURLFrom reads both facts off a command whose atlas.hcl merge has
// already been applied to mergedDevURL.
func atlasDevURLFrom(cmd *cobra.Command, mergedDevURL string) atlasDevURLInput {
	return atlasDevURLInput{value: mergedDevURL, given: atlasDevURLGiven(cmd, mergedDevURL)}
}

// refuse is the whole refusal for the two verbs that mark the flag required:
// the cobra wording when it was never given, and the client layer's verdict on
// the value when it was.
func (in atlasDevURLInput) refuse() error {
	if !in.given {
		return atlasRequiredDevURLError()
	}
	return atlasDevURLOpenDiagnostic(in.value)
}

// requireAtlasDevURL is [atlasDevURLInput.refuse] for the verbs that still hold
// the command when they refuse.
func requireAtlasDevURL(cmd *cobra.Command, mergedDevURL string) error {
	return atlasDevURLFrom(cmd, mergedDevURL).refuse()
}

// atlasDevURLAnsweredElsewhere reports whether rawURL carries a scheme this
// boundary must leave to a more specific refusal.
func atlasDevURLAnsweredElsewhere(rawURL string) bool {
	scheme, ok := atlasURLScheme(rawURL)
	return ok && scheme == atlasDevURLDockerScheme
}
