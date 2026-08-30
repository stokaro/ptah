// Package generate implements "ptah schema render", which renders the desired
// schema from Go entity annotations or local schema files as dialect-specific
// SQL DDL.
package generate

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/serverversion"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/servertarget"
)

const (
	rootDirFlag      = "root-dir"
	schemaFileFlag   = "schema-file"
	schemaCmdFlag    = "schema-cmd"
	schemaFormatFlag = "schema-format"
	dialectFlag      = "dialect"
)

const schemaCmdUsage = "External program whose stdout is the desired schema " +
	"(for example an ORM exporter). Run directly without a shell, split on " +
	`whitespace, so arguments cannot contain spaces. Example: "go run ./loader"`

type options struct {
	rootDirs      []string
	schemaFiles   []string
	schemaCmd     string
	schemaFormat  string
	dialect       string
	serverVersion string
	plainHTTP     bool
	configPath    string
	envName       string
}

func NewGenerateCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "render",
		Short: "Render a desired schema as SQL",
		Long: `Render a desired schema as SQL.

The desired schema may come from repeatable local SQL, YAML, HCL, or DBML
files, repeatable directories of Go annotations, an OCI schema artifact, or an
external program. Sources combine into one schema. Configured external_schema
programs require --allow-external-schema.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return generateCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)

	return cmd
}

// dialectUsage names every dialect --dialect takes.
//
// The list is discovered rather than written out. capability.DefaultDialects
// returns the names capability.ForDialect has a preset for, which is the same
// set core/renderer builds a renderer for, so a dialect Ptah gains is named
// here by the act of gaining a preset. internal/ddltx already treats that
// function as the authority for "a dialect Ptah has".
//
// The written-out version was the alternative, and it is what this replaces: it
// listed nine names and omitted oracle, which the flag accepted and rendered
// real Oracle DDL for, from the day the dialect landed until it was measured.
func dialectUsage() string {
	return "Database dialect (" + strings.Join(capability.DefaultDialects(), ", ") + "). " +
		"If empty, attempts the built-in review targets and emits output only if every target succeeds"
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.rootDirs, rootDirFlag, nil, "Root directory to scan for Go entities (repeatable; multiple roots merge into one composite schema; defaults to ./)")
	flags.StringArrayVar(&opts.schemaFiles, schemaFileFlag, nil, "SQL, YAML, HCL, DBML, or OCI desired-schema source (repeatable; combines with other sources)")
	flags.StringVar(&opts.schemaCmd, schemaCmdFlag, "", schemaCmdUsage)
	flags.StringVar(&opts.schemaFormat, schemaFormatFlag, "sql", "Format of the --schema-cmd output: sql, hcl, or yaml")
	flags.StringVar(&opts.dialect, dialectFlag, "", dialectUsage())
	serverversion.Register(flags, &opts.serverVersion)
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	flags.StringVar(&opts.configPath, dbcli.ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./ptah.yaml when present)")
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterExternalSchemaOptInFlag(flags)
}

func generateCommand(cmd *cobra.Command, opts *options) error {
	stdout := cmd.OutOrStdout()
	stderr := cmd.ErrOrStderr()

	// Resolved before anything is read so an unusable --server-version is
	// reported as the usage error it is, rather than behind whatever the first
	// schema source has to say for itself.
	target, err := resolveServerTarget(opts)
	if err != nil {
		return err
	}
	if target.Note != "" {
		fmt.Fprintf(stderr, "warning: %s\n", target.Note)
	}

	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return err
	}
	commands, err := dbcli.ResolveExternalSchemaCommands(
		cmd,
		opts.schemaCmd,
		opts.schemaFormat,
		projectCfg,
	)
	if err != nil {
		return err
	}

	// The render dialect also hints SQL parsing for both schema files and command
	// output, so the two SQL sources are treated consistently.
	result, err := schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:    opts.rootDirs,
		SchemaFiles: opts.schemaFiles,
		Commands:    commands,
		Dialect:     opts.dialect,
		PlainHTTP:   opts.plainHTTP,
		Logf:        func(format string, args ...any) { fmt.Fprintf(stderr, format+"\n", args...) },
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(stderr, "Found %d tables, %d fields, %d indexes, %d enums, %d embedded fields\n",
		len(result.Tables), len(result.Fields), len(result.Indexes), len(result.Enums), len(result.EmbeddedFields))
	fmt.Fprintln(stderr)
	fmt.Fprintln(stderr, getDependencyInfo(result))
	fmt.Fprintln(stderr)

	// Determine which dialects to generate
	dialects := []string{
		"postgres", "mysql", "mariadb", "sqlite", "clickhouse",
		"cockroachdb", "yugabytedb", "sqlserver", "spanner",
	}
	if opts.dialect != "" {
		dialects = []string{opts.dialect}
	}

	var rendered bytes.Buffer
	for _, d := range dialects {
		reportDialectScopeOmissions(stderr, result, d)
		statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(result, d, renderCapabilities(d, target))
		if err != nil {
			return fmt.Errorf("error rendering %s schema: %w", d, err)
		}

		if len(dialects) > 1 {
			fmt.Fprintf(&rendered, "-- %s schema\n\n", d)
		}
		for i, statement := range statements {
			fmt.Fprintf(&rendered, "-- Statement %d/%d\n%s\n\n", i+1, len(statements), statement)
		}
	}

	if _, err := rendered.WriteTo(stdout); err != nil {
		return fmt.Errorf("write rendered schema: %w", err)
	}

	return nil
}

// reportDialectScopeOmissions names, on stderr, every declared object this
// target is not rendering because the declaration scoped it elsewhere.
//
// An object that is absent from a target's desired state is indistinguishable
// from one that was never declared, and that is the whole failure this feature
// removes: silence is what made an operator spend an afternoon looking for a
// function that was never going to appear. The report goes to stderr rather
// than into the statement list because stdout is DDL somebody pipes into a
// database, and a note about a declaration is not a statement.
func reportDialectScopeOmissions(stderr io.Writer, db *schemamodel.Database, dialect string) {
	for _, omitted := range schemamodel.OmissionsForDialect(db, dialect) {
		fmt.Fprintf(stderr, "note: %s: %s %s is declared for %s and is not part of this target's schema\n",
			dialect, omitted.Kind, omitted.Name, strings.Join(omitted.Dialects, ", "))
	}
}

// resolveServerTarget maps --server-version onto the capability preset the
// render plans against.
//
// A nil Capabilities in the returned target means no server was pinned, and
// the render falls back to each dialect's default exactly as it did before the
// flag existed. The refusal of a value that names no server is the whole point
// of going through servertarget rather than capability.ForServerVersion, which
// answers an unreadable string with the dialect default and says nothing —
// correct for a live SELECT version(), wrong for a string a person typed.
func resolveServerTarget(opts *options) (servertarget.Target, error) {
	if opts.serverVersion == "" {
		return servertarget.Target{}, nil
	}
	if opts.dialect == "" {
		return servertarget.Target{}, fmt.Errorf(
			"--%s requires --%s: with no dialect the command renders every supported target, "+
				"and one server version cannot describe all of them",
			serverversion.FlagName, dialectFlag)
	}
	dialect := platform.NormalizeDialect(opts.dialect)
	if dialect == "" {
		// The dialect names no platform, so there is nothing to resolve the
		// version against. Reporting nothing here hands the diagnosis to the
		// renderer, whose "unsupported database dialect" names the flag that
		// is actually wrong.
		return servertarget.Target{}, nil
	}
	target, err := servertarget.Resolve(dialect, opts.serverVersion)
	if err != nil {
		// The resolver's own sentence names the value, the dialect and the
		// remedy, and there are two of them; re-stating one here is how the
		// other comes to be reported as the wrong thing.
		return servertarget.Target{}, fmt.Errorf("invalid --%s: %w", serverversion.FlagName, err)
	}
	return target, nil
}

// renderCapabilities picks the capability set one dialect renders against.
//
// capability.ForDialect is what renderer.GetOrderedCreateStatements passes on
// its own, so an unpinned render is byte-identical to the call this replaced.
func renderCapabilities(dialect string, target servertarget.Target) capability.Capabilities {
	if target.Capabilities == nil {
		return capability.ForDialect(dialect)
	}
	return target.Capabilities
}
