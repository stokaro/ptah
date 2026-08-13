package atlas

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/migration/migrator"
)

type atlasMigrateSetOptions struct {
	url             string
	dir             string
	dirFormat       string
	atlasEnv        string
	revisionsSchema string
}

type atlasMigrateSetPreparation struct {
	options atlasMigrateSetOptions
	dir     atlasargs.LocalDir
	format  atlasmigrateimport.Format
	project atlasProject
}

func newAtlasMigrateSetCommand() *cobra.Command {
	opts := atlasMigrateSetOptions{
		dir:       "file://migrations",
		dirFormat: atlasDirFormatDefault,
	}
	cmd := &cobra.Command{
		Use:   "set [flags] [version]",
		Short: "Set migration revision state",
		Long: `Edit Atlas revision metadata to consider every migration through the
given version applied, without executing migration SQL.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAtlasMigrateSet(cmd, opts, args)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL")
	flags.StringVar(&opts.dir, "dir", opts.dir, "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", opts.dirFormat, "Migration directory format")
	flags.StringVar(&opts.revisionsSchema, "revisions-schema", "", "Schema for the revision table")
	cmdutil.ConfigureCommandArgs(cmd, nil)
	return cmd
}

func atlasMigrateSetExactArgs(cmd *cobra.Command, args []string) error {
	if err := cobra.ExactArgs(1)(cmd, args); err != nil {
		return failAtlasCommand(cmd, err)
	}
	return nil
}

func runAtlasMigrateSet(
	cmd *cobra.Command,
	opts atlasMigrateSetOptions,
	args []string,
) (runErr error) {
	prepared, err := prepareAtlasMigrateSet(cmd, opts)
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	defer closeAtlasProject(&prepared.project, &runErr)
	opts = prepared.options
	// No required-flag check on purpose. The pinned community binary v1.3.0 has
	// none here either: an absent --url is opened as the empty string and the
	// client layer answers `sql/sqlclient: missing driver`. Measured, that
	// refusal loses to the checksum gate below -- `migrate set <v> --dir
	// <unhashed>` with no --url prints `checksum file not found` -- so leaving
	// the URL to the gate in front of the connection is also what puts these two
	// diagnostics in the measured order. See cmd/atlas/compat_url_diagnostic.go.
	source, err := prepared.project.captureLocal(prepared.dir)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("atlas migrate set --dir: %w", err))
	}
	captured, err := captureAtlasDirSource(source.FileSystem, prepared.format)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("atlas migrate set --dir: %w", err))
	}
	// Integrity gate (#974). `migrate set` writes revision rows that declare a
	// directory's history applied, so running it against a directory nothing has
	// verified records a state derived from files that may have drifted.
	// Measured against the pinned community binary v1.3.0, an unhashed or
	// tampered directory is refused here exactly as on `migrate validate`.
	//
	// The refusal deliberately precedes both the database connection and the
	// positional-arity check below: on the community binary a `migrate set` with
	// zero or two positionals against an unhashed directory prints the checksum
	// refusal, not an arity error. Returned bare so the stdout guidance block
	// and the `Error: ...` line land where that binary puts them.
	if err := verifyAtlasApplyChecksum(cmd, captured.gateFS(), prepared.format); err != nil {
		return err
	}
	migrationFS, err := captured.migrationFS(source.Display)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("atlas migrate set --dir: %w", err))
	}
	// Resolved from the same verified snapshot the conversion above reads, and
	// after it, so a directory this build refuses to convert never gets as far
	// as being addressed by token.
	tokens, err := captured.versionTokens()
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("atlas migrate set --dir: %w", err))
	}

	if err := atlasDatabaseURLDiagnostic(opts.url); err != nil {
		return failAtlasCommand(cmd, err)
	}
	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("error connecting to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	// Atlas validates the environment, migration directory, and database
	// connection before the positional version. Binary compatibility tests pin
	// this otherwise surprising diagnostic and side-effect order.
	if err := atlasMigrateSetExactArgs(cmd, args); err != nil {
		return err
	}
	version, err := resolveAtlasMigrateSetVersion(tokens, args[0])
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	result, err := atlasmigrate.Set(cmd.Context(), conn, version, atlasmigrate.SetOptions{
		Dir:             source.Display,
		FS:              migrationFS,
		AtlasEnv:        opts.atlasEnv,
		RevisionsSchema: opts.revisionsSchema,
	})
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	if err := writeAtlasMigrateSetResult(cmd, tokens, result); err != nil {
		return failAtlasCommand(cmd, err)
	}
	return nil
}

func prepareAtlasMigrateSet(
	cmd *cobra.Command,
	opts atlasMigrateSetOptions,
) (
	prepared atlasMigrateSetPreparation,
	returnErr error,
) {
	mode := ignoreMissingEnvSelection
	if !cmd.Flags().Changed("url") {
		mode = reportMissingEnvSelection
	}
	project, loaded, err := openAtlasProjectForCommand(cmd, mode)
	if err != nil {
		return atlasMigrateSetPreparation{}, err
	}
	prepared.project = project
	defer closeAtlasProjectOnError(&prepared.project, &returnErr)
	projectCfg := project.Config
	if loaded {
		opts.url = dbcli.EffectiveString(
			cmd,
			"url",
			opts.url,
			projectCfg.StringValue(projectconfig.StringDatabaseURL),
		)
		opts.dir = dbcli.EffectiveString(
			cmd,
			"dir",
			opts.dir,
			projectCfg.StringValue(projectconfig.StringMigrationDir),
		)
		opts.dirFormat = dbcli.EffectiveString(
			cmd,
			"dir-format",
			opts.dirFormat,
			projectCfg.StringValue(projectconfig.StringMigrationFormat),
		)
		opts.atlasEnv = dbcli.EffectiveString(
			cmd,
			dbcli.EnvFlagName,
			opts.atlasEnv,
			projectCfg.StringValue(projectconfig.StringEnvName),
		)
		opts.revisionsSchema = dbcli.EffectiveString(
			cmd,
			"revisions-schema",
			opts.revisionsSchema,
			projectCfg.StringValue(projectconfig.StringMigrationRevisionsSchema),
		)
	}
	if opts.dir == "" {
		return prepared, fmt.Errorf("migrations directory is required")
	}
	// The flag value is checked before --dir is parsed, where it was checked
	// when only the Atlas layout was accepted, so an invocation carrying two bad
	// values keeps printing the same one of them. The query spelling lives in
	// --dir and joins the resolution below.
	if _, err := resolveAtlasVerbDirFormat(cmd.ErrOrStderr(), "set", opts.dirFormat, nil); err != nil {
		return prepared, err
	}

	var localDir atlasargs.LocalDir
	if loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present {
		localDir, err = project.localDirWithQuery(opts.dir)
	} else {
		localDir, err = atlasargs.ParseLocalDir(opts.dir)
	}
	if err != nil {
		return prepared, fmt.Errorf("atlas migrate set --dir: %w", err)
	}
	format, err := resolveAtlasVerbDirFormat(cmd.ErrOrStderr(), "set", opts.dirFormat, localDir.Query)
	if err != nil {
		return prepared, err
	}
	// Preserve Atlas-compatible directory diagnostics; the subsequent
	// CaptureLocal call remains the authoritative rooted read and rejects any
	// path changed after this check.
	info, err := project.statLocalDir(localDir)
	if err != nil {
		return prepared, fmt.Errorf("sql/migrate: %w", err)
	}
	if !info.IsDir() {
		return prepared, fmt.Errorf("sql/migrate: %q is not a dir", localDir.Path)
	}
	prepared.options = opts
	prepared.dir = localDir
	prepared.format = format
	return prepared, nil
}

// resolveAtlasMigrateSetVersion turns the positional operand into the version
// the migrator addresses migrations by.
//
// On a directory with a version space of its own — today only a converted
// Flyway directory — the operand is the SOURCE version token, matched byte for
// byte, and the ordering key it projects to stops being an accepted spelling.
// That is the decision stokaro/ptah#1206 asks to be made explicitly rather than
// left to fall out of the implementation, and both halves of it are measured
// against the pinned community binary v1.3.0 on `V1.sql` + `V2__ok.sql`:
//
//	migrate set 1                    community exit 0    here, before: exit 1
//	migrate set 4611686018427469511  community exit 1    here, before: exit 0
//
// The second row is the reason this is not merely a convenience. Accepting a
// version the binary being mirrored refuses is the one direction parity never
// allows, and the ordering key was the ONLY spelling that worked here.
//
// The not-found message quotes the operand as typed, which is also measured:
// the community binary answers `migration with version "abc" not found` and
// `migration with version "0" not found` on that directory, so a token that is
// not an int64 and a token that is out of range are the same answer as a token
// that simply names no file. Ptah reported `--version "abc" is not a valid
// migration version: strconv.ParseInt ...` and `--version must be greater than
// zero`, which are diagnostics about int64 parsing on a directory that has no
// int64 versions to parse.
//
// A native Atlas directory, and every plain-numeric-prefix converted layout,
// keeps the int64 parsing it had: their file names ARE the versions, so there is
// no second spelling to translate, and the golang-migrate control in
// stokaro/ptah#1206 stays green by construction.
func resolveAtlasMigrateSetVersion(tokens flywayVersionTokens, value string) (int64, error) {
	if tokens.translates() {
		version, ok := tokens.resolve(value)
		if !ok {
			return 0, fmt.Errorf("migration with version %q not found", value)
		}
		return version, nil
	}
	version, err := atlasmigrate.ParseMigrationVersionFlag("version", value)
	if err != nil {
		return 0, err
	}
	if version == 0 {
		return 0, fmt.Errorf("migration version must be greater than zero")
	}
	return version, nil
}

// writeAtlasMigrateSetResult renders the summary in the version spelling the
// operand was given in, so a directory addressed by its Flyway tokens is
// reported back in them: `Current version is 1.5 (2 set)` with `+ 1 (a)` and
// `+ 1.5 (b)`, measured on the pinned community binary v1.3.0. On every other
// layout render is the decimal form of the version and the output is unchanged.
func writeAtlasMigrateSetResult(
	cmd *cobra.Command,
	tokens flywayVersionTokens,
	result migrator.AtlasRevisionSetResult,
) error {
	if len(result.Set) == 0 && len(result.Removed) == 0 {
		return nil
	}
	changes := make([]string, 0, 2)
	if len(result.Set) > 0 {
		changes = append(changes, fmt.Sprintf("%d set", len(result.Set)))
	}
	if len(result.Removed) > 0 {
		changes = append(changes, fmt.Sprintf("%d removed", len(result.Removed)))
	}

	out := cmd.OutOrStdout()
	if _, err := fmt.Fprintf(
		out,
		"Current version is %s (%s):\n\n",
		tokens.render(result.CurrentVersion),
		strings.Join(changes, ", "),
	); err != nil {
		return fmt.Errorf("write migrate set summary: %w", err)
	}
	for _, revision := range result.Set {
		if err := writeAtlasMigrateSetRevision(out, tokens, "+", revision); err != nil {
			return err
		}
	}
	for _, revision := range result.Removed {
		if err := writeAtlasMigrateSetRevision(out, tokens, "-", revision); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(out); err != nil {
		return fmt.Errorf("write migrate set terminator: %w", err)
	}
	return nil
}

func writeAtlasMigrateSetRevision(
	out io.Writer,
	tokens flywayVersionTokens,
	action string,
	revision migrator.AtlasRevisionChange,
) error {
	version := tokens.render(revision.Version)
	if revision.Description == "" {
		if _, err := fmt.Fprintf(out, "  %s %s\n", action, version); err != nil {
			return fmt.Errorf("write migrate set revision: %w", err)
		}
		return nil
	}
	if _, err := fmt.Fprintf(out, "  %s %s (%s)\n", action, version, revision.Description); err != nil {
		return fmt.Errorf("write migrate set revision: %w", err)
	}
	return nil
}
