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
	// Asked here rather than left to WriteProfile, which asks it again: a
	// --format typo resolved after the dial reports a connection failure for a
	// target the operator never meant to reach.
	if err := validateFormat(opts.format); err != nil {
		return err
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

	return WriteProfile(cmd.OutOrStdout(), opts.format, profile)
}

// validateFormat rejects a format value both entry points have to reject the
// same way. Two spellings of this message is how the pre-connect check and the
// renderer come to disagree about which values exist.
func validateFormat(format string) error {
	if format != formatText && format != formatJSON {
		return fmt.Errorf("invalid --format value %q: expected text or json", format)
	}
	return nil
}

// WriteProfile renders a profile to w in the named format, and is exported
// because the renderer is otherwise reachable only through a live server.
//
// Every three-way choice below is decided by what one particular server said:
// a preset with no name, a release line carrying a label, a banner no version
// could be read from. Driving those through the cobra command needs a SQL
// Server 2022, a MariaDB behind a mysql:// URL and a server that answers
// nothing about itself, so before this seam existed the only branch any test
// took was SQLite's — the one target that needs no server — and the other arms
// of orNone, presetDescription and lineDescription were unasserted. A hand-built
// [serverprofile.Profile] reaches all of them, and [serverprofile.For] is pure
// for the same reason.
//
// runCapabilities calls this, so the seam is the path the command takes rather
// than a second renderer that can drift from it.
func WriteProfile(w io.Writer, format string, profile serverprofile.Profile) error {
	if err := validateFormat(format); err != nil {
		return err
	}
	if format == formatJSON {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(profile)
	}
	return writeProfileText(w, profile)
}

// writeProfileText renders the profile for a person.
//
// Every FIELD the JSON carries reaches this form too, because a text mode that
// summarized would make --format json the only complete answer and the reader
// most likely to need the complete answer is the one diagnosing by eye. That
// includes Server.Product, which is the "which product does the server SAY it
// is" answer and is exactly what a reader who reached a MariaDB through a
// mysql:// URL came to find out.
//
// The two forms differ in two ways, both deliberate. The ORDER here is reading
// order rather than the struct's: the version and the product Ptah parsed stand
// above the banner they were parsed out of, so an operator checking a parse
// against the raw string reads downward, while the JSON declares the banner
// first. The CONTENTS differ one way only: each capability's registry
// documentation is in the JSON and not here, since twenty-five doc sentences
// would bury the twenty-five verdicts they annotate.
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
		{"Server product", orNone(profile.Server.Product)},
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
// a rendering fault; "none" reads as the answer it is, and every field it
// guards — a version no banner yielded, a banner naming no product, a server
// that said nothing about itself, an undeclared release line — is exactly what
// the reader came to find out.
func orNone(value string) string {
	if value == "" {
		return "none"
	}
	return value
}
