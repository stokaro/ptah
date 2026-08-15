package schema

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

const (
	planDBURLFlag      = "db-url"
	planRootDirFlag    = "root-dir"
	planSchemaFileFlag = "schema-file"
	planDevURLFlag     = "dev-url"
	planExcludeFlag    = "exclude"
	planNameFlag       = "name"
	planOutputFlag     = "output"
	planSaveFlag       = "save"
	planDryRunFlag     = "dry-run"
)

type schemaPlanOptions struct {
	dbURL          string
	rootDirs       []string
	schemaFiles    []string
	devURL         string
	exclude        []string
	name           string
	output         string
	save           bool
	dryRun         bool
	plainHTTP      bool
	connectTimeout string
	configPath     string
	envName        string
}

func newSchemaPlanCommand() *cobra.Command {
	opts := schemaPlanOptions{}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Save a fingerprinted declarative apply plan",
		Long: `Compute the declarative schema plan from the --db-url target database to the
local desired-state sources and save it as a fingerprinted local plan file
(JSON, format version 1).

"ptah schema apply --plan <path>" executes the saved plan after verifying the
database still matches the plan's source fingerprint, so a reviewed plan is
exactly what runs — a drifted target refuses to execute. The desired state
comes from Go annotations (--root-dir) or native schema files (--schema-file,
repeatable; sources merge into one composite schema). Pass --save or
--output <path> to write the plan file, or --dry-run to print the plan
document without saving it.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaPlan(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, planDBURLFlag, "", "Target database URL the plan applies to (required)")
	flags.StringArrayVar(&opts.rootDirs, planRootDirFlag, nil, "Root directory to scan for Go entities (repeatable)")
	flags.StringArrayVar(&opts.schemaFiles, planSchemaFileFlag, nil, "YAML, HCL, or SQL schema file describing the desired state (repeatable)")
	flags.StringVar(&opts.devURL, planDevURLFlag, "", "Dev database URL; must match the target dialect when set")
	flags.StringArrayVar(&opts.exclude, planExcludeFlag, nil, "Schema objects to exclude from planning (Atlas-style selectors)")
	flags.StringVar(&opts.name, planNameFlag, "", "Plan name recorded in the plan file")
	flags.StringVar(&opts.output, planOutputFlag, "", "Plan file output path (default <name>"+atlasschema.PlanFileSuffix+")")
	flags.BoolVar(&opts.save, planSaveFlag, false, "Save the plan to a local plan file")
	flags.BoolVar(&opts.dryRun, planDryRunFlag, false, "Print the plan file document without saving it")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	cmd.MarkFlagsMutuallyExclusive(planSaveFlag, planDryRunFlag)
	cmd.MarkFlagsMutuallyExclusive(planOutputFlag, planDryRunFlag)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaPlan(cmd *cobra.Command, opts schemaPlanOptions) error {
	if err := sqlitevirtual.ValidateExplicitURLToggle(opts.dbURL); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.dbURL = dbcli.EffectiveString(
		cmd,
		planDBURLFlag,
		opts.dbURL,
		projectCfg.StringValue(projectconfig.StringDatabaseURL),
	)
	opts.devURL = dbcli.EffectiveString(
		cmd,
		planDevURLFlag,
		opts.devURL,
		projectCfg.StringValue(projectconfig.StringDevURL),
	)
	policy := nativeDiffPolicy(projectCfg)

	if strings.TrimSpace(opts.dbURL) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("database URL is required"))
	}
	if dialect, dialectErr := atlasurl.DialectFromURL(opts.dbURL); dialectErr == nil {
		if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if len(opts.rootDirs) == 0 && len(opts.schemaFiles) == 0 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"a desired schema source is required: pass --%s and/or --%s",
			planRootDirFlag, planSchemaFileFlag))
	}
	if !opts.save && strings.TrimSpace(opts.output) == "" && !opts.dryRun {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"pass --%s or --%s <path> to write a local plan file, or --%s to preview the plan document",
			planSaveFlag, planOutputFlag, planDryRunFlag))
	}
	if strings.ContainsAny(opts.name, `/\`) {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s must not contain path separators; use --%s to choose the plan file location", planNameFlag, planOutputFlag))
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(
		dbcli.EffectiveString(
			cmd,
			dbcli.ConnectTimeoutFlagName,
			opts.connectTimeout,
			projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
		))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --%s: %w", planDBURLFlag, err))
	}
	defer dbschema.CloseAndWarn(conn)

	desired, err := schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:    opts.rootDirs,
		SchemaFiles: opts.schemaFiles,
		Dialect:     conn.Info().Dialect,
		PlainHTTP:   opts.plainHTTP,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	plan, err := atlasschema.PreparePlanFile(cmd.Context(), conn, atlasschema.PlanFileOptions{
		Name:    opts.name,
		DevURL:  opts.devURL,
		Desired: desired,
		Exclude: opts.exclude,
		Policy:  policy,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !plan.HasChanges() {
		fmt.Fprintln(cmd.OutOrStdout(), "Schema is synced, no changes to be made.")
		return nil
	}
	document, err := atlasschema.MarshalPlanFile(plan)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.dryRun {
		if _, err := cmd.OutOrStdout().Write(document); err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("write plan preview: %w", err))
		}
		return nil
	}

	printSchemaApplyPlan(cmd.OutOrStdout(), plan.SQL())
	path := strings.TrimSpace(opts.output)
	if path == "" {
		path = plan.Name + atlasschema.PlanFileSuffix
	}
	if err := os.WriteFile(path, document, 0o644); err != nil { // #nosec G306 -- plan files are meant to be reviewed and shared, 0644 like migration files
		return cmdutil.Fail(cmd, fmt.Errorf("write plan file: %w", err))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Plan saved to file://%s\n", path)
	return nil
}
