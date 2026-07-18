package dropall

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
)

var dropAllCmd = &cobra.Command{
	Use:   "drop-all",
	Short: "Drop ALL tables and enums in database (VERY DANGEROUS!)",
	Long: `Drop ALL tables and enums from the database, not just those defined in Go entities.
	
🚨 EXTREME WARNING: This operation will permanently delete EVERYTHING in the database!
This will delete ALL tables and enums, including those not defined in your Go entities.
ALL DATA WILL BE LOST!`,
	RunE: dropAllCommand,
}

const (
	dbURLFlag  = "db-url"
	dryRunFlag = "dry-run"
)

var dropAllFlags = map[string]cobraflags.Flag{
	dbURLFlag: &cobraflags.StringFlag{
		Name:  dbURLFlag,
		Value: "",
		Usage: "Database URL (required). Example: postgres://localhost:5432/dbname",
	},
	dryRunFlag: &cobraflags.BoolFlag{
		Name:  dryRunFlag,
		Value: false,
		Usage: "Show what would be executed without making actual changes",
	},
	dbcli.ConnectTimeoutFlagName: dbcli.NewConnectTimeoutFlag(),
}

var dropAllFlagsRegistered bool

func NewDropAllCommand() *cobra.Command {
	if !dropAllFlagsRegistered {
		cobraflags.RegisterMap(dropAllCmd, dropAllFlags)
		dropAllFlagsRegistered = true
	}

	return dropAllCmd
}

func dropAllCommand(_ *cobra.Command, _ []string) error {
	dbURL := dropAllFlags[dbURLFlag].GetString()
	dryRun := dropAllFlags[dryRunFlag].GetBool()

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if dryRun {
		fmt.Printf("[DRY RUN] Would drop ALL tables and enums from database %s\n", dbschema.FormatDatabaseURL(dbURL))
		fmt.Println("=== DRY RUN: DROP ALL TABLES FROM DATABASE ===")
	} else {
		fmt.Printf("Dropping ALL tables and enums from database %s\n", dbschema.FormatDatabaseURL(dbURL))
		fmt.Println("=== DROP ALL TABLES FROM DATABASE ===")
	}
	fmt.Println()

	// 1. Connect to database
	connectTimeout, err := dbcli.ParseConnectTimeout(dropAllFlags[dbcli.ConnectTimeoutFlagName].GetString())
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	fmt.Printf("Connected to %s database successfully!\n", conn.Info().Dialect)
	fmt.Println()

	// Set dry run mode on the writer
	conn.Writer().SetDryRun(dryRun)

	// 2. Show extreme warning and ask for confirmation (skip confirmation in dry run mode)
	if dryRun {
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
			fmt.Println("Operation cancelled.")
			return nil
		}

		fmt.Println()
		fmt.Print("⚠️  Last chance! Type 'YES I AM SURE' to proceed: ")
		confirmation, err = readLine()
		if err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}

		if confirmation != "YES I AM SURE" {
			fmt.Println("Operation cancelled.")
			return nil
		}
	}

	// 3. Drop all tables and enums
	if dryRun {
		fmt.Println("[DRY RUN] Would drop all tables and enums from database...")
	} else {
		fmt.Println("Dropping all tables and enums from database...")
	}
	err = conn.Writer().DropAllTables()
	if err != nil {
		return fmt.Errorf("error dropping all tables: %w", err)
	}

	if dryRun {
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
