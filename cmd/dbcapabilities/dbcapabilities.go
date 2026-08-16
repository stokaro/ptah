// Package dbcapabilities implements "ptah db capabilities", which connects to a
// live database and reports what Ptah has established about it: the server it
// identified, the capability preset it will plan with and how that preset was
// reached, what this repository promises about the release line, and every
// capability key with its value there.
//
// It is the surface stokaro/ptah#1230 asks for. Before it, the resolution was
// computed on every connection and then dropped after two DEBUG log lines, so
// "Ptah works against one server with this version but not another" had no
// answer an operator could obtain.
package dbcapabilities

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/capabilityprobe"
	"go.5x5.cz/ptah/internal/serverprofile"
)

const (
	dbURLFlag = "db-url"

	formatText = "text"
	formatJSON = "json"
)

type options struct {
	dbURL          string
	connectTimeout string
	format         string
}

// NewCapabilitiesCommand returns the verb that reports a live server's
// capability profile.
func NewCapabilitiesCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "capabilities",
		Short: "Report what Ptah can do against a live database",
		Long: `Report the capability profile Ptah resolves for a live database.

The output names the server Ptah identified, the capability preset it plans
with and how that preset was chosen, the support level Ptah declares for the
release line, and every capability key with its value on this target.

A release line Ptah does not test is reported as best-effort, not refused:
capabilities are resolved for it and the operations they allow are performed.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCapabilities(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	flags.StringVar(&opts.format, "format", formatText, "Output format: text or json")
}

func runCapabilities(cmd *cobra.Command, opts *options) error {
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.format != formatText && opts.format != formatJSON {
		return fmt.Errorf("invalid --format value %q: expected text or json", opts.format)
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(conn)

	info := conn.Info()

	// The banner and the product version are read here rather than inside
	// serverprofile so that the profile stays a pure function of strings, and
	// asked through capabilityprobe rather than with a query written here so
	// that this verb and the capability probe cannot disagree about which
	// version a SQL Server is. @@VERSION opens with a marketing year; the
	// probe's reader is what looks past it.
	profile := serverprofile.For(
		info.Dialect,
		info.Version,
		capabilityprobe.ProductVersion(cmd.Context(), conn, info.Dialect),
	)

	return writeProfile(cmd.OutOrStdout(), opts.format, profile)
}

func writeProfile(w io.Writer, format string, profile serverprofile.Profile) error {
	if format == formatJSON {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(profile)
	}
	return writeProfileText(w, profile)
}

// writeProfileText renders the profile for a person.
//
// Every FIELD the JSON carries appears here, in the same order, because a text
// mode that summarized would make --format json the only complete answer and
// the reader most likely to need the complete answer is the one diagnosing by
// eye. The two forms are not identical, and the difference runs one way only:
// each capability's registry documentation is in the JSON and not here, since
// twenty-five doc sentences would bury the twenty-five verdicts they annotate.
func writeProfileText(w io.Writer, profile serverprofile.Profile) error {
	if err := writeProfileHeader(w, profile); err != nil {
		return err
	}
	if err := writeProfileTraits(w, profile.Traits); err != nil {
		return err
	}
	return writeProfileCapabilities(w, profile.Capabilities)
}

func writeProfileHeader(w io.Writer, profile serverprofile.Profile) error {
	fields := [][2]string{
		{"Dialect", profile.Dialect},
		{"Server version", orNone(profile.Server.Version)},
		{"Banner", orNone(profile.Server.Banner)},
		{"Capability preset", presetDescription(profile.Preset)},
		{"Preset source", string(profile.Preset.Source)},
		{"Support level", string(profile.Certification.Level)},
		{"Release line", orNone(lineDescription(profile.Certification))},
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, field := range fields {
		if _, err := fmt.Fprintf(tw, "%s:\t%s\n", field[0], field[1]); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	// The note and the reason go below the aligned block rather than inside
	// it: both are sentences, and a sentence in a two-column table wraps into
	// the column and stops being readable.
	if profile.Preset.Note != "" {
		if _, err := fmt.Fprintf(w, "\nNote: %s\n", profile.Preset.Note); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "\n%s: %s\n", profile.Certification.Level, profile.Certification.Reason)
	return err
}

func writeProfileTraits(w io.Writer, traits capability.Traits) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "\nBehavior:"); err != nil {
		return err
	}
	rows := [][2]string{
		{"identifier_limit", traits.Identifiers.String()},
		{"enum_modeling", string(traits.EnumModeling)},
		{"foreign_key_reference", string(traits.ForeignKeyReference)},
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(tw, "  %s\t%s\n", row[0], row[1]); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeProfileCapabilities(w io.Writer, capabilities []serverprofile.Capability) error {
	if _, err := fmt.Fprintln(w, "\nCapabilities:"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, entry := range capabilities {
		if _, err := fmt.Fprintf(tw, "  %s\t%s\n", entry.Key, supportedWord[entry.Supported]); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// supportedWord spells the boolean out. "true"/"false" would read as a claim
// about the key rather than about the server, and the two words below are the
// ones the documentation and the refusal messages already use.
//
// It is a lookup rather than a function taking the boolean because revive
// reports such a parameter as control coupling, and it is right: the value
// selects the answer rather than being one.
var supportedWord = map[bool]string{
	true:  "supported",
	false: "unsupported",
}

func presetDescription(preset serverprofile.Preset) string {
	if preset.Name == "" {
		return fmt.Sprintf("(unnamed, resolved for %s)", preset.Dialect)
	}
	if preset.Dialect == "" {
		return preset.Name
	}
	return fmt.Sprintf("%s (%s)", preset.Name, preset.Dialect)
}

func lineDescription(certification serverprofile.Certification) string {
	if certification.Line == "" {
		return ""
	}
	if certification.Label == "" {
		return certification.Line
	}
	return fmt.Sprintf("%s (%s)", certification.Line, certification.Label)
}

// orNone keeps an empty field visible. A blank value in aligned output reads as
// a rendering fault; "none" reads as the answer it is, and both of the fields
// that can be empty here — an unreadable version, an undeclared line — are
// exactly what the reader came to find out.
func orNone(value string) string {
	if value == "" {
		return "none"
	}
	return value
}
