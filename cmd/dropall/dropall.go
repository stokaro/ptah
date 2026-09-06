// Package dropall implements "ptah db drop-all", the destructive command that
// drops all tables and enums from the target database.
package dropall

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/cmdflags"
	"ptah.run/cmd/internal/cmdutil"
	"ptah.run/cmd/internal/dbcli"
	"ptah.run/dbschema"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/schemaclean"
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
		Short: "Drop every schema object in a live database (VERY DANGEROUS!)",
		Long: `Drop every schema object the target database holds.

🚨 EXTREME WARNING: this cannot be undone, and ALL DATA WILL BE LOST.

The scope is the database, not a schema Ptah declares. Objects Ptah never
created are dropped along with the ones it did. Which kinds go depends on the
dialect: tables everywhere, and views, materialized views, enums, types,
functions, sequences and foreign keys where that dialect's writer removes them.
SQLite keeps Ptah's revision table. Everything else in the database goes, so
"ptah migrations status" afterwards still reports the old version and
"ptah migrations up" finds nothing pending against an emptied database. Use
"ptah migrations baseline" to put the recorded history back in step.

Run --dry-run first. It connects, reports how many objects would be dropped,
and changes nothing.

Without --dry-run the command asks for two confirmations before it proceeds.
--auto-approve skips both, and it is the one flag here with no PTAH_* variable,
so no environment can turn the prompts off by accident.`,
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
	// Not bound to a PTAH_* variable, unlike every other flag here: a variable
	// that skips the confirmation on a verb that drops every object in a
	// database is one an environment could set by accident. See the same
	// decision on `schema apply` (stokaro/ptah#852).
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
		fmt.Fprintf(out, "[DRY RUN] Would drop ALL tables and enums from database %s\n", dburldisplay.Format(opts.dbURL))
		fmt.Fprintln(out, "=== DRY RUN: DROP ALL TABLES FROM DATABASE ===")
	} else {
		fmt.Fprintf(out, "Dropping ALL tables and enums from database %s\n", dburldisplay.Format(opts.dbURL))
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

	plan, err := schemaclean.Inspect(cmd.Context(), conn)
	if err != nil {
		return err
	}

	// 2. Show extreme warning and ask for confirmation (skip confirmation in dry run or auto-approve mode)
	switch {
	case opts.dryRun:
		fmt.Fprintln(out, "[DRY RUN] This would permanently delete every schema object in the database!")
		fmt.Fprintln(out, "[DRY RUN] This would delete EVERYTHING in the database, including objects Ptah did not create!")
		fmt.Fprintln(out, "[DRY RUN] This would result in ALL DATA BEING LOST!")
		fmt.Fprintln(out)
	case opts.autoApprove:
		fmt.Fprintln(out, "Auto-approval enabled; skipping interactive confirmation.")
		fmt.Fprintln(out)
	default:
		reader := bufio.NewReader(cmd.InOrStdin())
		fmt.Fprintln(out, "EXTREME WARNING: This operation will permanently delete every schema object in the database!")
		fmt.Fprintln(out, "This will delete EVERYTHING in the database, including objects Ptah did not create!")
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
	_, err = schemaclean.Execute(cmd.Context(), conn, schemaclean.Options{DryRun: opts.dryRun})
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
