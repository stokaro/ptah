package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/editor"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

type atlasMigrateDiffOptions struct {
	toURLs      []string
	devURL      string
	dirURL      string
	dirFormat   string
	format      string
	schemas     []string
	lockTimeout string
	dryRun      bool
	qualifier   string
	edit        bool
}

func newAtlasMigrateDiffCommand() *cobra.Command {
	opts := atlasMigrateDiffOptions{}
	cmd := &cobra.Command{
		Use:   "diff [flags] [name]",
		Short: "Compute migration diff against a desired schema",
		Long: `Atlas OSS ` + "`atlas migrate diff`" + ` command path.

Drops all tables in the --dev-url database, replays the local migration
directory on it, compares the resulting state to --to desired-state sources,
and writes new Atlas-style migration files plus atlas.sum when changes are
found. Use a disposable dev database. --to accepts local .hcl, .yaml, .yml,
and .sql schema files, one directly connectable database URL, one local Atlas
migration directory, or one env://<attribute> reference resolved through the
evaluated atlas.hcl env. A database used as --to must differ from --dev-url.
Use --schema to limit the comparison to selected schema names.
--qualifier prefixes every object in the generated statements with a custom
schema qualifier when working on a single schema. When the atlas.hcl env
enables diff.concurrent_index.create, new indexes are planned as CREATE INDEX
CONCURRENTLY; files carrying such statements are tagged with the Atlas
` + "`-- atlas:txmode none`" + ` directive, and plans mixing them with
transactional statements are split into a transactional file followed by a
concurrent-index file. atlas.sum is updated only after every migration file
was written. With --edit the generated migration files open in $VISUAL or
$EDITOR before the directory checksum is finalized. Docker dev databases
remain an explicit follow-up gap. When --env is set, the selected atlas.hcl env
can provide schema.src, dev, migration.dir, format.migrate.diff, and supported
diff policy values.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := "migration"
			if len(args) == 1 {
				name = args[0]
			}
			return runAtlasMigrateDiff(cmd, opts, name)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema target URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to replay migrations and compute the diff")
	flags.StringVar(&opts.dirURL, "dir", "file://migrations", "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", "atlas", "Migration directory format; only atlas is implemented")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schemas to diff")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring Atlas migration directory locks")
	flags.StringVar(&opts.qualifier, "qualifier", "", "Qualify tables with a custom qualifier when working on a single schema")
	flags.BoolVar(&opts.edit, "edit", false, "Edit the generated migration files")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the generated migration file to stdout instead of writing it")
	if err := flags.MarkHidden("dry-run"); err != nil {
		panic(err)
	}
	cmdutil.ConfigureCommandArgs(cmd, nil)
	return cmd
}

func runAtlasMigrateDiff(
	cmd *cobra.Command,
	opts atlasMigrateDiffOptions,
	name string,
) (runErr error) {
	formatConfigured := cmd.Flags().Changed("format")
	policy := atlasschema.DiffPolicy{}
	mode := ignoreMissingEnvSelection
	if needsAtlasMigrateDiffConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	project, loaded, err := openAtlasProjectForCommand(cmd, mode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer closeAtlasProject(&project, &runErr)
	projectCfg := project.Config
	if loaded {
		opts.devURL = dbcli.EffectiveString(
			cmd,
			"dev-url",
			opts.devURL,
			projectCfg.StringValue(projectconfig.StringDevURL),
		)
		opts.dirURL = dbcli.EffectiveString(
			cmd,
			"dir",
			opts.dirURL,
			projectCfg.StringValue(projectconfig.StringMigrationDir),
		)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatMigrateDiff)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatConfigured = formatConfigured || formatValue.Present
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	projectEnv := atlassource.ProjectEnv{}
	if loaded {
		projectEnv, err = atlasSourceProjectEnv(cmd, projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if loaded && !cmd.Flags().Changed("to") &&
		(len(projectCfg.SchemaSources) > 0 || atlasExternalSchemaConfigured(projectCfg)) {
		opts.toURLs = []string{"env://src"}
	}
	desired, err := prepareAtlasMigrateDiffSource(opts, projectEnv)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	qualifier, err := atlasmigrate.ParseQualifier(opts.qualifier)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if formatConfigured && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	format := atlasreport.NormalizeMigrateDiffFormat(opts.format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	lockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	var localDir atlasargs.LocalDir
	if loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present {
		localDir, err = project.localDirWithQuery(opts.dirURL)
	} else {
		localDir, err = atlasargs.ParseLocalDir(opts.dirURL)
	}
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate diff --dir: %w", err))
	}
	// Same reasoning as `migrate new`: this verb writes a migration and a fresh
	// atlas.sum without running the integrity gate, so accepting a query here
	// would relax a refusal into a write over a directory nothing verified.
	// See stokaro/ptah#1086.
	if len(localDir.Query) > 0 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"atlas migrate diff --dir: migration directory URL query parameters are not supported for this command",
		))
	}
	migrationsDir, err := resolveMigrateDiffDirectory(localDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("resolve migration directory: %w", err))
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.devURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --dev-url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	var preparePublication func([]string) error
	if opts.edit {
		preparePublication = func(stagedPaths []string) error {
			return editor.Open(cmd.Context(), "", stagedPaths...)
		}
	}
	diffResult, err := atlasmigrate.GenerateDiff(cmd.Context(), conn, atlasmigrate.DiffOptions{
		Dir:                  migrationsDir,
		Desired:              desired,
		SourceConnectTimeout: dbcli.DefaultConnectTimeout,
		Name:                 name,
		Format:               format,
		Schemas:              opts.schemas,
		LockTimeout:          lockTimeout,
		Policy:               policy,
		Qualifier:            qualifier,
		DryRun:               opts.dryRun,
		PreparePublication:   preparePublication,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if diffResult.Synced {
		fmt.Fprintln(cmd.OutOrStdout(), "The migration directory is synced with the desired state, no changes to be made")
		return nil
	}
	if opts.dryRun {
		fmt.Fprint(cmd.OutOrStdout(), diffResult.SQL)
		return nil
	}
	for _, migrationPath := range diffResult.MigrationPaths {
		fmt.Fprintf(cmd.OutOrStdout(), "Created migration file: %s\n", migrationPath)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Updated migration checksum: %s\n", diffResult.SumPath)
	return nil
}

func resolveMigrateDiffDirectory(dir atlasargs.LocalDir) (string, error) {
	if dir.AllowedRoot != "" {
		return pathguard.ResolveWithinRoot(dir.Path, dir.AllowedRoot)
	}
	return pathguard.ResolveCLIPath(dir.Path)
}

func needsAtlasMigrateDiffConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("to") ||
		!cmd.Flags().Changed("dev-url")
}

func prepareAtlasMigrateDiffSource(
	opts atlasMigrateDiffOptions,
	projectEnv atlassource.ProjectEnv,
) (atlassource.Set, error) {
	if len(opts.toURLs) == 0 {
		return atlassource.Set{}, fmt.Errorf("--to is required")
	}
	if strings.TrimSpace(opts.devURL) == "" {
		return atlassource.Set{}, fmt.Errorf("--dev-url is required")
	}
	dirFormat := strings.ToLower(strings.TrimSpace(opts.dirFormat))
	if dirFormat != "" && dirFormat != string(migrator.MigrationDirFormatAtlas) {
		return atlassource.Set{}, fmt.Errorf("atlas migrate diff currently writes Atlas-format migration directories only")
	}
	if opts.edit && opts.dryRun {
		return atlassource.Set{}, fmt.Errorf("atlas migrate diff --edit cannot be combined with --dry-run: dry runs write no migration file to edit")
	}
	if strings.HasPrefix(strings.TrimSpace(opts.devURL), "docker://") {
		return atlassource.Set{}, fmt.Errorf("atlas migrate diff accepts docker --dev-url values, but Ptah requires a directly connectable dev database URL")
	}
	toSet, err := atlassource.ClassifySet("--to", opts.toURLs, projectEnv)
	if err != nil {
		return atlassource.Set{}, err
	}
	if err := toSet.EnsureDevDatabase(opts.devURL); err != nil {
		return atlassource.Set{}, err
	}
	if err := toSet.EnsureDevIsolation(opts.devURL); err != nil {
		return atlassource.Set{}, err
	}
	if _, _, err := atlassource.PinDialect(opts.devURL, toSet); err != nil {
		return atlassource.Set{}, err
	}
	return toSet, nil
}
