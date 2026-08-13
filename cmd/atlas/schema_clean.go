package atlas

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemaclean"
)

type atlasSchemaCleanOptions struct {
	url         string
	dryRun      bool
	format      string
	autoApprove bool
	include     []string
	exclude     []string
}

// scoped reports whether a selector narrowed the cleanup plan. A scoped run
// executes the plan itself; an unscoped run keeps the whole-database drop.
func (o atlasSchemaCleanOptions) scoped() bool {
	return len(o.include) > 0 || len(o.exclude) > 0
}

func newAtlasSchemaCleanCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	opts := atlasSchemaCleanOptions{}
	long := `Atlas OSS ` + "`atlas schema clean`" + ` command path.

Cleans user-owned schema objects through Ptah's destructive database cleanup
runtime. The implementation supports direct database URLs, dry-run planning,
explicit auto-approval, and Atlas Go-template output over the cleanup plan.`
	if !policy.IsStrictCE() {
		long += `

--include and --exclude narrow the cleanup to part of the database, using the
same selectors as ` + "`schema apply`" + `, ` + "`schema diff`" + ` and
` + "`schema inspect`" + `: --include positively selects top-level objects and
--exclude subtracts from the result. Child objects (a table's foreign keys)
ride along with their parent. A narrowed run executes exactly the changes it
printed, one statement at a time, instead of the whole-database drop an
unflagged run performs — so what the plan lists is what is destroyed.
PostgreSQL-family scoped drops use RESTRICT and one transaction: selected known
dependents run first, and the server refuses a parent when an unselected object
still depends on it.`
	}
	cmd := &cobra.Command{
		Use:   "clean",
		Short: "Clean database schema objects",
		Long:  long,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if policy.IsStrictCE() && cmd.Flags().Changed("dry-run") {
				return failAtlasStrictCompatGate(cmd, "ptah-compat schema clean --dry-run")
			}
			if policy.IsStrictCE() && cmd.Flags().Changed("format") {
				return failAtlasStrictCompatGate(cmd, "ptah-compat schema clean --format")
			}
			return runAtlasSchemaClean(cmd, opts, policy)
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to clean")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show planned cleanup without applying it")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.BoolVar(&opts.autoApprove, "auto-approve", false, "Skip interactive approval")
	if !policy.IsStrictCE() {
		flags.StringArrayVar(&opts.include, "include", nil, "Schema objects to include in the cleanup")
		flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from the cleanup")
	}
	if err := cmdflags.DisableEnvBinding(flags, "auto-approve"); err != nil {
		panic(err)
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgsHint("name the database with -u/--url"))
	return cmd
}

func runAtlasSchemaClean(
	cmd *cobra.Command,
	opts atlasSchemaCleanOptions,
	policy atlascompatpolicy.Policy,
) error {
	formatOutput := cmd.Flags().Changed("format")
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.url = dbcli.EffectiveString(
			cmd,
			"url",
			opts.url,
			projectCfg.StringValue(projectconfig.StringDatabaseURL),
		)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatSchemaClean)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatOutput = formatOutput || formatValue.Present
		// An atlas.hcl exclude that every other schema verb honors must reach
		// the destructive one too. Ignoring it here is the dangerous direction:
		// an operator who excluded a table from their schema workflow would
		// still watch this command drop it.
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
	}
	if policy.IsStrictCE() && formatOutput {
		return failAtlasStrictCompatGate(cmd, "ptah-compat schema clean --format")
	}
	if formatOutput && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if formatOutput {
		if err := atlasreport.ValidateSchemaCleanTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	// Selector syntax is checked before the database is contacted, so a
	// malformed pattern cannot half-clean a database on its way to failing.
	if err := atlasfilter.ValidateResourceIncludeSelectors(opts.include); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := atlasfilter.ValidateExcludeSelectors(opts.exclude); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Absent and empty are different answers on this verb. The pinned community
	// binary v1.3.0 refuses an absent --url with the plural spelling, but lets
	// `--url ""` through to the client layer, which answers
	// `sql/sqlclient: missing driver` -- the behavior of a check that asks
	// whether the flag was given, not what it holds. The project-config value is
	// already folded into opts.url above, so a url from atlas.hcl satisfies this
	// exactly as a flag does. See cmd/atlas/compat_url_diagnostic.go.
	if !cmd.Flags().Changed("url") && strings.TrimSpace(opts.url) == "" {
		return cmdutil.Fail(cmd, atlasRequiredURLError(atlasRequiredURLPlural))
	}
	if err := atlasDatabaseURLDiagnostic(opts.url); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	cancel()
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := inspectAtlasSchemaCleanPlan(policy, conn)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.scoped() {
		plan, err = scopeAtlasSchemaCleanPlan(plan, atlasfilter.Scope{
			Include:       opts.include,
			Exclude:       opts.exclude,
			DefaultSchema: conn.Info().Schema,
		}, conn.Info().Dialect)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if formatOutput && !opts.dryRun {
		if err := validateAtlasSchemaCleanActualFormat(opts, conn, plan); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if formatOutput && opts.dryRun {
		rendered, err := renderAtlasSchemaCleanFormat(opts, conn, plan, false)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), rendered)
	} else if !formatOutput {
		printAtlasSchemaCleanPlan(cmd.OutOrStdout(), opts, conn, plan)
	}
	if opts.dryRun {
		return nil
	}

	ok := true
	if opts.autoApprove {
		if !formatOutput {
			fmt.Fprintln(cmd.OutOrStdout(), "Auto-approval enabled; skipping interactive confirmation.")
			fmt.Fprintln(cmd.OutOrStdout())
		}
	} else {
		if formatOutput {
			rendered, err := renderAtlasSchemaCleanFormat(opts, conn, plan, false)
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			fmt.Fprint(cmd.OutOrStdout(), rendered)
			fmt.Fprintln(cmd.OutOrStdout())
		}
		var err error
		ok, err = promptAtlasSchemaCleanConfirmation(cmd.OutOrStdout(), cmd.InOrStdin())
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if !ok {
		return nil
	}

	if err := applyAtlasSchemaClean(cmd, opts, policy, conn, plan); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if formatOutput {
		rendered, err := renderAtlasSchemaCleanFormat(opts, conn, plan, true)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), rendered)
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema clean completed successfully.")
	return nil
}

// applyAtlasSchemaClean destroys what the plan describes.
//
// A full-mode unscoped run keeps the whole-database drop, which is what it has
// always been and what the writer's DropAllTables implements. Scoped and strict
// runs execute the validated plan itself: selectors must not be cosmetic, and
// strict mode must not re-inventory and destroy an object that appeared while
// the operator was reading the confirmation prompt.
func applyAtlasSchemaClean(
	cmd *cobra.Command,
	opts atlasSchemaCleanOptions,
	policy atlascompatpolicy.Policy,
	conn *dbschema.DatabaseConnection,
	plan schemaclean.Plan,
) error {
	if policy.IsStrictCE() {
		return schemaclean.ApplyPlanWithOptions(
			cmd.Context(),
			conn,
			plan,
			schemaclean.ApplyPlanOptions{ValidateBeforeExecute: func(executor dbschematypes.SchemaExecutor) error {
				validationConn := conn
				if executor != nil {
					validationConn = conn.WithExecutor(executor)
				}
				fresh, err := inspectAtlasSchemaCleanPlan(policy, validationConn)
				if err != nil {
					return err
				}
				if !slices.Equal(plan.Objects, fresh.Objects) || !slices.Equal(plan.Changes, fresh.Changes) {
					return errors.New("schema changed after cleanup confirmation; rerun schema clean and review the new plan")
				}
				return nil
			}},
		)
	}
	if opts.scoped() {
		return schemaclean.ApplyPlan(cmd.Context(), conn, plan)
	}
	return schemaclean.Apply(cmd.Context(), conn)
}

func inspectAtlasSchemaCleanPlan(
	policy atlascompatpolicy.Policy,
	conn *dbschema.DatabaseConnection,
) (schemaclean.Plan, error) {
	inspectOpts := schemaclean.InspectOptions{}
	if policy.IsStrictCE() {
		inspectOpts.ValidateSchema = func(schema *dbschematypes.DBSchema) error {
			owned := schemaclean.SnapshotWithinWriterScope(
				schema,
				conn.Info().Dialect,
				conn.Info().Schema,
			)
			return policy.ValidateSchemaCleanSnapshot(dbschematogo.ConvertDBSchemaToGoSchema(owned))
		}
	}
	plan, err := schemaclean.InspectWithOptions(conn, inspectOpts)
	if err != nil {
		return schemaclean.Plan{}, err
	}
	for _, object := range plan.Objects {
		if err := policy.ValidateSchemaCleanObject(atlascompatpolicy.LiveSchemaObject{
			Kind:             object.Type,
			Name:             object.Name,
			ImplicitSequence: object.Implicit,
		}); err != nil {
			return schemaclean.Plan{}, err
		}
	}
	return plan, nil
}

func printAtlasSchemaCleanPlan(
	out io.Writer,
	opts atlasSchemaCleanOptions,
	conn *dbschema.DatabaseConnection,
	plan schemaclean.Plan,
) {
	if opts.dryRun {
		fmt.Fprintf(out, "[DRY RUN] Would clean schema objects from database %s\n", dbschema.FormatDatabaseURL(opts.url))
	} else {
		fmt.Fprintf(out, "Cleaning schema objects from database %s\n", dbschema.FormatDatabaseURL(opts.url))
	}
	fmt.Fprintf(out, "Connected to %s database successfully.\n", conn.Info().Dialect)
	fmt.Fprintf(out, "Planned cleanup changes: %d\n", len(plan.Changes))
	for _, change := range plan.Changes {
		fmt.Fprintf(out, "- %s\n", change.Cmd)
	}
	if opts.dryRun {
		fmt.Fprintln(out, "[DRY RUN] No changes were applied.")
	}
}

func renderAtlasSchemaCleanFormat(
	opts atlasSchemaCleanOptions,
	conn *dbschema.DatabaseConnection,
	plan schemaclean.Plan,
	applied bool,
) (string, error) {
	report := atlasreport.NewSchemaClean(atlasreport.SchemaCleanOptions{
		Driver:  conn.Info().Dialect,
		URL:     opts.url,
		DryRun:  opts.dryRun,
		Applied: applied,
		Plan:    plan,
	})
	var out bytes.Buffer
	if err := atlasreport.WriteSchemaClean(&out, opts.format, report); err != nil {
		return "", err
	}
	return out.String(), nil
}

func validateAtlasSchemaCleanActualFormat(
	opts atlasSchemaCleanOptions,
	conn *dbschema.DatabaseConnection,
	plan schemaclean.Plan,
) error {
	if _, err := renderAtlasSchemaCleanFormat(opts, conn, plan, false); err != nil {
		return err
	}
	_, err := renderAtlasSchemaCleanFormat(opts, conn, plan, true)
	return err
}

func promptAtlasSchemaCleanConfirmation(prompt io.Writer, input io.Reader) (bool, error) {
	reader := bufio.NewReader(input)
	fmt.Fprintln(prompt, "WARNING: This operation will permanently delete all supported schema objects.")
	fmt.Fprint(prompt, "Type 'DELETE EVERYTHING' to confirm this destructive operation: ")
	confirmation, err := reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("read schema clean confirmation: %w", err)
	}
	if strings.TrimSpace(confirmation) != "DELETE EVERYTHING" {
		fmt.Fprintln(prompt, "Schema clean canceled.")
		return false, nil
	}
	fmt.Fprint(prompt, "Last chance. Type 'YES I AM SURE' to proceed: ")
	confirmation, err = reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("read schema clean confirmation: %w", err)
	}
	if strings.TrimSpace(confirmation) != "YES I AM SURE" {
		fmt.Fprintln(prompt, "Schema clean canceled.")
		return false, nil
	}
	fmt.Fprintln(prompt)
	return true, nil
}
