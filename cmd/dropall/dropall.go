package dropall

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
)

const (
	dbURLFlag  = "db-url"
	dryRunFlag = "dry-run"
)

type options struct {
	dbURL          string
	dryRun         bool
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
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
}

func dropAllCommand(_ *cobra.Command, opts *options) error {
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if opts.dryRun {
		fmt.Printf("[DRY RUN] Would drop ALL tables and enums from database %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
		fmt.Println("=== DRY RUN: DROP ALL TABLES FROM DATABASE ===")
	} else {
		fmt.Printf("Dropping ALL tables and enums from database %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
		fmt.Println("=== DROP ALL TABLES FROM DATABASE ===")
	}
	fmt.Println()

	// 1. Connect to database
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	fmt.Printf("Connected to %s database successfully!\n", conn.Info().Dialect)
	fmt.Println()

	// Set dry run mode on the writer
	conn.SchemaWriter().SetDryRun(opts.dryRun)

	// 2. Show extreme warning and ask for confirmation (skip confirmation in dry run mode)
	if opts.dryRun {
		fmt.Println("ℹ️  [DRY RUN] This would permanently delete ALL tables and enums!")
		fmt.Println("ℹ️  [DRY RUN] This would delete EVERYTHING in the database, not just your Go entities!")
		fmt.Println("ℹ️  [DRY RUN] This would result in ALL DATA BEING LOST!")
		fmt.Println()
	} else {
		fmt.Println("🚨 EXTREME WARNING: This operation will permanently delete ALL tables and enums!")
		fmt.Println("🚨 This will delete EVERYTHING in the database, not just your Go entities!")
		fmt.Println("🚨 This action cannot be undone!")
		fmt.Println("🚨 ALL DATA WILL BE LOST!")
		fmt.Println()
		fmt.Print("Type 'DELETE EVERYTHING' to confirm this destructive operation: ")

		confirmation, err := readLine()
		if err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}

		if confirmation != "DELETE EVERYTHING" {
			fmt.Println("Operation canceled.")
			return nil
		}

		fmt.Println()
		fmt.Print("⚠️  Last chance! Type 'YES I AM SURE' to proceed: ")
		confirmation, err = readLine()
		if err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}

		if confirmation != "YES I AM SURE" {
			fmt.Println("Operation canceled.")
			return nil
		}
	}

	// 3. Drop all tables and enums
	if opts.dryRun {
		fmt.Println("[DRY RUN] Would drop all tables and enums from database...")
	} else {
		fmt.Println("Dropping all tables and enums from database...")
	}
	err = conn.SchemaWriter().DropAllTables()
	if err != nil {
		return fmt.Errorf("error dropping all tables: %w", err)
	}

	if opts.dryRun {
		fmt.Println("✅ [DRY RUN] Drop all operations completed successfully!")
		fmt.Println("🔥 [DRY RUN] Database would be completely empty!")
	} else {
		fmt.Println("✅ All tables and enums dropped successfully!")
		fmt.Println("🔥 Database is now completely empty!")
	}
	return nil
}

// readLine reads a complete line from stdin, including spaces
func readLine() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Remove the trailing newline
	return strings.TrimSpace(line), nil
}
