package assist

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistconfig"
)

// Flag names, in one place because the help text and the diagnostics quote
// them.
const (
	profileFlag = "provider-profile"
	modelFlag   = "model"
	formatFlag  = "format"

	formatText  = "text"
	formatJSON  = "json"
	formatJSONL = "jsonl"
)

// providerOptions is what the provider commands take.
type providerOptions struct {
	profile string
	model   string
	format  string
}

// newProviderCommand returns the provider namespace.
func newProviderCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "provider",
		Short: "Inspect and test the model providers this machine can reach",
		Long: `A provider profile is one configured way to reach a model: a type, an endpoint,
a model identifier, and a credential reference.

Profiles come from your own configuration file and from the environment. An
exported OPENAI_API_KEY, ANTHROPIC_API_KEY or OLLAMA_HOST is enough to get a
profile without writing configuration at all.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newProviderListCommand(), newProviderTestCommand())
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// registerFormat adds the output selector every provider command carries.
func registerFormat(cmd *cobra.Command, opts *providerOptions) {
	cmd.Flags().StringVar(&opts.format, formatFlag, formatText,
		"Output format: text or json")
}

// newProviderListCommand returns "provider list".
func newProviderListCommand() *cobra.Command {
	opts := &providerOptions{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the configured provider profiles",
		Long: `List the provider profiles this machine can reach, where each came from, and
which one is the default.

No credential is read: listing profiles must not require the keys they name, so
a profile whose key is missing still appears here and fails at "provider test",
where the failure is the answer.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd, opts)
		},
	}
	registerFormat(cmd, opts)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// newProviderTestCommand returns "provider test".
func newProviderTestCommand() *cobra.Command {
	opts := &providerOptions{}
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Check that a provider profile works, by measuring it",
		Long: `Check one provider profile: that the endpoint answers, that it accepts the
credential, that it serves the model, and that the model can call a tool.

The last one is the capability Ptah Assist requires, and it is measured rather
than read off documentation -- a deployment that documents tool calling and one
that supports it are different things.

Nothing about your project is sent. The check is a fixed two-line prompt asking
the model to call one tool, so an operator can test a provider before deciding
whether to send it anything.

It exits 1 when the profile cannot be used, so a script can branch on it.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTest(cmd, opts)
		},
	}
	cmd.Flags().StringVar(&opts.profile, profileFlag, "",
		"Provider profile to test; the default profile when omitted")
	cmd.Flags().StringVar(&opts.model, modelFlag, "",
		"Model identifier, overriding the profile's")
	registerFormat(cmd, opts)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// validateFormat refuses an output format before any work happens.
func validateFormat(cmd *cobra.Command, format string) error {
	switch format {
	case formatText, formatJSON:
		return nil
	}
	return cmdutil.Fail(cmd, fmt.Errorf("--%s: unknown format %q; want %s or %s",
		formatFlag, format, formatText, formatJSON))
}

// validateConversationFormat refuses an output format for a surface that holds
// a conversation.
//
// Separate from [validateFormat] because `jsonl` is the record stream of a
// conversation as it happens, and a listing has no conversation to stream. A
// shared validator would accept it on `provider list` and then print a JSON
// document anyway.
func validateConversationFormat(cmd *cobra.Command, format string) error {
	switch format {
	case formatText, formatJSON, formatJSONL:
		return nil
	}
	return cmdutil.Fail(cmd, fmt.Errorf("--%s: unknown format %q; want %s, %s or %s",
		formatFlag, format, formatText, formatJSON, formatJSONL))
}

// profileReport is one row of "provider list".
type profileReport struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	BaseURL string `json:"base_url,omitempty"`
	Model   string `json:"model,omitempty"`
	// Credential is the REFERENCE, never a value. It is reported because "which
	// variable does this profile read" is the question an operator has when a
	// profile does not work.
	Credential string `json:"credential,omitempty"`
	Source     string `json:"source"`
	Default    bool   `json:"default"`
}

// listReport is what "provider list" prints in JSON mode.
type listReport struct {
	ConfigPath string          `json:"config_path,omitempty"`
	Default    string          `json:"default,omitempty"`
	Profiles   []profileReport `json:"profiles"`
}

// runList prints the configured profiles.
func runList(cmd *cobra.Command, opts *providerOptions) error {
	if err := validateFormat(cmd, opts.format); err != nil {
		return err
	}
	config, err := assistconfig.Load(assistconfig.Options{})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	report := listReport{
		ConfigPath: config.Path,
		Default:    config.Default,
		Profiles:   make([]profileReport, 0, len(config.Names())),
	}
	for _, name := range config.Names() {
		profile, profileErr := config.Profile(name)
		if profileErr != nil {
			return cmdutil.Fail(cmd, profileErr)
		}
		source := "configuration file"
		if config.Derived(name) {
			source = "environment"
		}
		report.Profiles = append(report.Profiles, profileReport{
			Name:       name,
			Type:       string(profile.Type),
			BaseURL:    profile.BaseURL,
			Model:      profile.Model,
			Credential: profile.Credential,
			Source:     source,
			Default:    name == config.Default,
		})
	}

	if opts.format == formatJSON {
		return writeJSON(cmd.OutOrStdout(), report)
	}
	writeListText(cmd.OutOrStdout(), report)
	return nil
}

// writeListText prints the human form.
func writeListText(out io.Writer, report listReport) {
	if len(report.Profiles) == 0 {
		fmt.Fprintln(out, "No provider profiles are configured.")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Export a key your provider already uses -- OPENAI_API_KEY,")
		fmt.Fprintln(out, "ANTHROPIC_API_KEY -- or point OLLAMA_HOST at a model server on this")
		fmt.Fprintf(out, "machine. For anything else, write %s in your Ptah configuration\n",
			assistconfig.FileName)
		fmt.Fprintln(out, "directory.")
		return
	}
	if report.ConfigPath != "" {
		fmt.Fprintf(out, "Configuration: %s\n\n", report.ConfigPath)
	}
	for _, profile := range report.Profiles {
		marker := " "
		if profile.Default {
			marker = "*"
		}
		fmt.Fprintf(out, "%s %s (%s, from the %s)\n", marker, profile.Name, profile.Type, profile.Source)
		fmt.Fprintf(out, "    endpoint:   %s\n", orNone(profile.BaseURL))
		fmt.Fprintf(out, "    model:      %s\n", orNone(profile.Model))
		fmt.Fprintf(out, "    credential: %s\n", orNone(profile.Credential))
	}
	if report.Default == "" {
		fmt.Fprintf(out, "\nNo default profile: name one with --%s, or set %s.\n",
			profileFlag, assistconfig.ProfileEnvVar)
		return
	}
	fmt.Fprintf(out, "\n* is the default. Test it with: ptah assist provider test\n")
}

// orNone renders an empty value as something a reader can distinguish from a
// blank line.
func orNone(value string) string {
	if value == "" {
		return "(none)"
	}
	return value
}

// testReport is what "provider test" prints.
type testReport struct {
	Profile string `json:"profile"`
	Type    string `json:"type"`
	BaseURL string `json:"base_url,omitempty"`
	Model   string `json:"model"`
	// Usable is the single field a script branches on. It is the conjunction of
	// the checks below rather than a separate opinion.
	Usable        bool     `json:"usable"`
	Reachable     bool     `json:"reachable"`
	Authenticated bool     `json:"authenticated"`
	ModelListed   bool     `json:"model_listed"`
	ToolCalling   bool     `json:"tool_calling"`
	LatencyMillis int64    `json:"latency_ms"`
	Notes         []string `json:"notes"`
}

// runTest measures one profile.
func runTest(cmd *cobra.Command, opts *providerOptions) error {
	if err := validateFormat(cmd, opts.format); err != nil {
		return err
	}
	loadOpts := assistconfig.Options{}
	config, err := assistconfig.Load(loadOpts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	profile, err := config.Select(opts.profile)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.model != "" {
		profile.Model = opts.model
	}
	provider, err := config.Provider(profile, loadOpts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	probe, err := provider.Probe(cmd.Context())
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	report := testReport{
		Profile:       profile.Name,
		Type:          string(profile.Type),
		BaseURL:       profile.BaseURL,
		Model:         provider.Model(),
		Reachable:     probe.Reachable,
		Authenticated: probe.Authenticated,
		ModelListed:   probe.ModelListed,
		ToolCalling:   probe.ToolCalling,
		LatencyMillis: probe.Latency.Milliseconds(),
		Notes:         probe.Notes,
	}
	if report.Notes == nil {
		report.Notes = make([]string, 0)
	}
	// Usable means Assist could run against this profile. Tool calling is part
	// of it rather than a warning beside it: without it there is no agent mode,
	// and #1488 states that Ptah must refuse rather than silently degrade into
	// generating SQL nobody validated.
	report.Usable = probe.Reachable && probe.Authenticated && probe.ToolCalling

	if opts.format == formatJSON {
		if writeErr := writeJSON(cmd.OutOrStdout(), report); writeErr != nil {
			return writeErr
		}
	} else {
		writeTestText(cmd.OutOrStdout(), report)
	}
	if !report.Usable {
		// The document is on stdout in both modes and the outcome is the exit
		// code, which is this tree's contract for a machine-readable command.
		return exitcode.New(1, unusableError(report))
	}
	return nil
}

// unusableError names the first check that failed, because the first is the one
// to fix.
func unusableError(report testReport) error {
	switch {
	case !report.Reachable:
		return fmt.Errorf("profile %q: the endpoint did not answer", report.Profile)
	case !report.Authenticated:
		return fmt.Errorf("profile %q: the endpoint refused the credential", report.Profile)
	case !report.ToolCalling:
		return fmt.Errorf("profile %q: %w", report.Profile, aiprovider.ErrToolCallingUnsupported)
	}
	return fmt.Errorf("profile %q is not usable", report.Profile)
}

// writeTestText prints the human form.
func writeTestText(out io.Writer, report testReport) {
	fmt.Fprintf(out, "%s (%s)\n", report.Profile, report.Type)
	fmt.Fprintf(out, "  endpoint:      %s\n", orNone(report.BaseURL))
	fmt.Fprintf(out, "  model:         %s\n", report.Model)
	fmt.Fprintf(out, "  reachable:     %s\n", measured[report.Reachable])
	fmt.Fprintf(out, "  credential:    %s\n", credentialWord[report.Authenticated])
	fmt.Fprintf(out, "  model listed:  %s\n", measured[report.ModelListed])
	fmt.Fprintf(out, "  tool calling:  %s\n", measured[report.ToolCalling])
	fmt.Fprintf(out, "  round trip:    %d ms\n", report.LatencyMillis)
	for _, note := range report.Notes {
		fmt.Fprintf(out, "  note:          %s\n", note)
	}
}

// measured renders a checked property, and is a lookup rather than a branch
// because the value is data being printed rather than a decision being made.
var measured = map[bool]string{true: "yes", false: "no"}

// credentialWord renders the credential check in its own words, because "no"
// against a credential reads as "there was none" rather than "it was refused".
var credentialWord = map[bool]string{true: "accepted", false: "refused or not checked"}

// writeJSON prints a document the way every other machine-readable Ptah command
// does.
func writeJSON(out io.Writer, document any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(document)
}
