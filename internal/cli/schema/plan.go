package schema

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/schemaroot"
	"ptah.run/internal/schemaload"
	"ptah.run/internal/sqlitevirtual"
)

const (
	planDBURLFlag      = "db-url"
	planRootDirFlag    = "root-dir"
	planSchemaFileFlag = "schema-file"
	planDevURLFlag     = "dev-url"
	planExcludeFlag    = "exclude"
	planProtectedTable = "protected-table"
	planNameFlag       = "name"
	planOutputFlag     = "output"
	planSaveFlag       = "save"
	planDryRunFlag     = "dry-run"
	planJSONFlag       = "json"
)

type schemaPlanOptions struct {
	dbURL           string
	rootDirs        []string
	schemaFiles     []string
	devURL          string
	exclude         []string
	protectedTables []string
	name            string
	output          string
	save            bool
	dryRun          bool
	jsonOutput      bool
	plainHTTP       bool
	connectTimeout  string
	configPath      string
	envName         string
}

func newSchemaPlanCommand() *cobra.Command {
	opts := schemaPlanOptions{}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Save a fingerprinted direct apply plan",
		Long: `Compute the direct schema plan from the --db-url target database to the
local desired-schema sources and save it as a fingerprinted local plan file
(JSON, format version 1).

"ptah schema apply --plan <path>" executes the saved plan after verifying the
database still matches the plan's source fingerprint, so a reviewed plan is
exactly what runs — a drifted target refuses to execute. The desired schema
comes from native schema files or OCI artifacts (--schema-file, repeatable),
or Go annotations (--root-dir, repeatable). Sources merge into one composite
schema. Pass --save or
--output <path> to write the plan file, or --dry-run to print the plan
document without saving it.

--json prints one versioned JSON document on standard output, on success and
on failure: whether the plan holds changes, the plan document with each
statement's severity, the plan digest, and a typed refusal code when planning
refused. With --dry-run the document carries the plan in place of printing it.
Everything written for a person goes to standard error.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaPlan(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, planDBURLFlag, "", "Target database URL the plan applies to (required)")
	flags.StringArrayVar(&opts.rootDirs, planRootDirFlag, nil, "Root directory to scan for Go entities (repeatable)")
	flags.StringArrayVar(&opts.schemaFiles, planSchemaFileFlag, nil, "SQL, YAML, HCL, DBML, or OCI desired-schema source (repeatable)")
	flags.StringVar(&opts.devURL, planDevURLFlag, "", "Dev database URL; must match the target dialect when set")
	flags.StringArrayVar(&opts.exclude, planExcludeFlag, nil, "Schema objects to exclude from planning (Atlas-style selectors)")
	flags.StringArrayVar(&opts.protectedTables, planProtectedTable, nil,
		"Declared row set this plan refuses to change, by table or schema.table; repeat to add more. There is no override")
	flags.StringVar(&opts.name, planNameFlag, "", "Plan name recorded in the plan file")
	flags.StringVar(&opts.output, planOutputFlag, "", "Plan file output path (default <name>"+atlasschema.PlanFileSuffix+")")
	flags.BoolVar(&opts.save, planSaveFlag, false, "Save the plan to a local plan file")
	flags.BoolVar(&opts.dryRun, planDryRunFlag, false, "Print the plan file document without saving it")
	flags.BoolVar(&opts.jsonOutput, planJSONFlag, false,
		"Print the run's result as one versioned JSON document, on success and on failure")
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
	human := opts.humanOutput(cmd)
	evidence, err := planSchema(cmd, opts, human)
	if opts.jsonOutput {
		evidence.Err = err
		if writeErr := writeReport(cmd.OutOrStdout(), atlasschema.NewPlanReport(evidence)); writeErr != nil && err == nil {
			err = writeErr
		}
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

// planSchema computes the plan and saves or prints it as opts ask. What a
// person reads goes to human; the plan document itself goes to standard output
// only under --dry-run without --json, where it is the command's output.
//
// It returns the evidence a --json run reports, and the error the command
// fails with. The evidence carries no plan when the error is not nil.
func planSchema(cmd *cobra.Command, opts schemaPlanOptions, human io.Writer) (atlasschema.PlanEvidence, error) {
	if err := sqlitevirtual.ValidateExplicitURLToggle(opts.dbURL); err != nil {
		return atlasschema.PlanEvidence{}, err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return atlasschema.PlanEvidence{}, err
	}
	schemaSourceEnv, err := dbcli.SchemaSourceProjectEnv(cmd, projectCfg)
	if err != nil {
		return atlasschema.PlanEvidence{}, err
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
		return atlasschema.PlanEvidence{}, fmt.Errorf("database URL is required")
	}
	if dialect, dialectErr := atlasurl.DialectFromURL(opts.dbURL); dialectErr == nil {
		if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
			return atlasschema.PlanEvidence{}, err
		}
	}
	if len(opts.rootDirs) == 0 && len(opts.schemaFiles) == 0 {
		return atlasschema.PlanEvidence{}, fmt.Errorf(
			"a desired schema source is required: pass --%s and/or --%s",
			planRootDirFlag, planSchemaFileFlag)
	}
	if !opts.save && strings.TrimSpace(opts.output) == "" && !opts.dryRun {
		return atlasschema.PlanEvidence{}, fmt.Errorf(
			"pass --%s or --%s <path> to write a local plan file, or --%s to preview the plan document",
			planSaveFlag, planOutputFlag, planDryRunFlag)
	}
	if strings.ContainsAny(opts.name, `/\`) {
		return atlasschema.PlanEvidence{}, fmt.Errorf("--%s must not contain path separators; use --%s to choose the plan file location", planNameFlag, planOutputFlag)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(
		dbcli.EffectiveString(
			cmd,
			dbcli.ConnectTimeoutFlagName,
			opts.connectTimeout,
			projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
		))
	if err != nil {
		return atlasschema.PlanEvidence{}, err
	}

	declaredVars, err := dbcli.DeclaredVars(cmd)
	if err != nil {
		return atlasschema.PlanEvidence{}, err
	}
	loadOptions := schemaload.Options{
		RootDirs:        opts.rootDirs,
		SchemaFiles:     opts.schemaFiles,
		ProjectEnv:      schemaSourceEnv,
		EnvSelectorFlag: dbcli.SchemaSourceEnvSelectorFlag(cmd),
		PlainHTTP:       opts.plainHTTP,
		Vars:            declaredVars,
	}
	// An artifact is accepted or refused before the target is opened; see
	// [schemaload.ResolveBeforeConnect].
	resolved, isArtifact, err := schemaload.ResolveBeforeConnect(cmd.Context(), loadOptions)
	if err != nil {
		return atlasschema.PlanEvidence{}, err
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return atlasschema.PlanEvidence{}, fmt.Errorf("connect to --%s: %w", planDBURLFlag, err)
	}
	defer dbschema.CloseAndWarn(conn)

	var desired *schemamodel.Database
	if isArtifact {
		desired = resolved.Database
	} else {
		loadOptions.Dialect = conn.Info().Dialect
		desired, err = schemaload.LoadContext(cmd.Context(), loadOptions)
		if err != nil {
			return atlasschema.PlanEvidence{}, err
		}
	}

	plan, err := atlasschema.PreparePlanFile(cmd.Context(), conn, atlasschema.PlanFileOptions{
		ProjectRoot:     schemaroot.Of(opts.rootDirs),
		Name:            opts.name,
		DevURL:          opts.devURL,
		Desired:         desired,
		Exclude:         opts.exclude,
		Policy:          policy,
		ProtectedTables: opts.protectedTables,
		Diagnostics:     cmd.ErrOrStderr(),
	})
	if err != nil {
		return atlasschema.PlanEvidence{}, err
	}
	if !plan.HasChanges() {
		fmt.Fprintln(human, "Schema is synced, no changes to be made.")
		return atlasschema.PlanEvidence{Plan: &plan}, nil
	}
	document, err := atlasschema.MarshalPlanFile(plan)
	if err != nil {
		return atlasschema.PlanEvidence{}, err
	}
	evidence := atlasschema.PlanEvidence{Plan: &plan, Document: document}
	if opts.dryRun {
		// Under --json the report carries the document, and printing it a
		// second time would put two JSON values on one stream.
		if opts.jsonOutput {
			return evidence, nil
		}
		if _, err := cmd.OutOrStdout().Write(document); err != nil {
			return atlasschema.PlanEvidence{}, fmt.Errorf("write plan preview: %w", err)
		}
		return evidence, nil
	}

	printSchemaApplyPlan(human, plan.SQL())
	path := strings.TrimSpace(opts.output)
	if path == "" {
		path = plan.Name + atlasschema.PlanFileSuffix
	}
	if err := os.WriteFile(path, document, 0o644); err != nil { // #nosec G306 -- plan files are meant to be reviewed and shared, 0644 like migration files
		return atlasschema.PlanEvidence{}, fmt.Errorf("write plan file: %w", err)
	}
	fmt.Fprintf(human, "Plan saved to file://%s\n", path)
	evidence.Path = path
	return evidence, nil
}
