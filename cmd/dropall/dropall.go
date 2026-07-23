package dropall

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/schemaclean"
)

const (
	dbURLFlag       = "db-url"
	dryRunFlag      = "dry-run"
	autoApproveFlag = "auto-approve"
)

type options struct {
	dbURL          string
	dryRun         bool
	autoApprove    bool
	connectTimeout string
}

func NewDropAllCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "drop-all",
		Short: "Drop ALL tables and enums in database (VERY DANGEROUS!)",
		Long: `Drop ALL tables and enums from the database, not just those defined in Go entities.

🚨 EXTREME WARNING: This operation will permanently delete EVERYTHING in the database!
This will delete ALL tables and enums, including those not defined in your Go entities.
ALL DATA WILL BE LOST!`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return dropAllCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)

	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Show what would be executed without making actual changes")
	flags.BoolVar(&opts.autoApprove, autoApproveFlag, false, "Skip interactive approval for destructive cleanup")
	if err := cmdflags.DisableEnvBinding(flags, autoApproveFlag); err != nil {
		panic(err)
	}
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
}

func dropAllCommand(cmd *cobra.Command, opts *options) error {
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	out := cmd.OutOrStdout()
	if opts.dryRun {
		fmt.Fprintf(out, "[DRY RUN] Would drop ALL tables and enums from database %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
		fmt.Fprintln(out, "=== DRY RUN: DROP ALL TABLES FROM DATABASE ===")
	} else {
		fmt.Fprintf(out, "Dropping ALL tables and enums from database %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
		fmt.Fprintln(out, "=== DROP ALL TABLES FROM DATABASE ===")
	}
	fmt.Fprintln(out)

	// 1. Connect to database
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	fmt.Fprintf(out, "Connected to %s database successfully!\n", conn.Info().Dialect)
	fmt.Fprintln(out)

	plan, err := schemaclean.Inspect(conn)
	if err != nil {
		return err
	}

	// 2. Show extreme warning and ask for confirmation (skip confirmation in dry run or auto-approve mode)
	switch {
	case opts.dryRun:
		fmt.Fprintln(out, "[DRY RUN] This would permanently delete ALL tables and enums!")
		fmt.Fprintln(out, "[DRY RUN] This would delete EVERYTHING in the database, not just your Go entities!")
		fmt.Fprintln(out, "[DRY RUN] This would result in ALL DATA BEING LOST!")
		fmt.Fprintln(out)
	case opts.autoApprove:
		fmt.Fprintln(out, "Auto-approval enabled; skipping interactive confirmation.")
		fmt.Fprintln(out)
	default:
		reader := bufio.NewReader(cmd.InOrStdin())
		fmt.Fprintln(out, "EXTREME WARNING: This operation will permanently delete ALL tables and enums!")
		fmt.Fprintln(out, "This will delete EVERYTHING in the database, not just your Go entities!")
		fmt.Fprintln(out, "This action cannot be undone!")
		fmt.Fprintln(out, "ALL DATA WILL BE LOST!")
		fmt.Fprintln(out)
		fmt.Fprint(out, "Type 'DELETE EVERYTHING' to confirm this destructive operation: ")

		confirmation, err := readLine(reader)
		if err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}

		if confirmation != "DELETE EVERYTHING" {
			fmt.Fprintln(out, "Operation canceled.")
			return nil
		}

		fmt.Fprintln(out)
		fmt.Fprint(out, "Last chance! Type 'YES I AM SURE' to proceed: ")
		confirmation, err = readLine(reader)
		if err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}

		if confirmation != "YES I AM SURE" {
			fmt.Fprintln(out, "Operation canceled.")
			return nil
		}
	}

	// 3. Drop all tables and enums
	if opts.dryRun {
		fmt.Fprintf(out, "[DRY RUN] Would drop %d supported schema objects from database...\n", len(plan.Changes))
	} else {
		fmt.Fprintf(out, "Dropping %d supported schema objects from database...\n", len(plan.Changes))
	}
	_, err = schemaclean.Execute(conn, schemaclean.Options{DryRun: opts.dryRun})
	if err != nil {
		return fmt.Errorf("error dropping all tables: %w", err)
	}

	if opts.dryRun {
		fmt.Fprintln(out, "[DRY RUN] Drop all operations completed successfully!")
		fmt.Fprintln(out, "[DRY RUN] Database would be empty for supported cleanup object types.")
	} else {
		fmt.Fprintln(out, "All tables and enums dropped successfully!")
		fmt.Fprintln(out, "Database is now empty for supported cleanup object types.")
	}
	return nil
}

// readLine reads a complete line from stdin, including spaces
func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Remove the trailing newline
	return strings.TrimSpace(line), nil
}
