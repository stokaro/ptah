package readdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
)

const (
	dbURLFlag = "db-url"
)

type options struct {
	dbURL          string
	connectTimeout string
	schemas        string
}

func NewReadDBCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "read",
		Short: "Read schema from database",
		Long: `Read and display the current schema from the specified database.

This command connects to the database and reads the existing schema,
displaying tables, columns, indexes, and constraints in a formatted output.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return readDBCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
}

func readDBCommand(_ *cobra.Command, opts *options) error {
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	fmt.Printf("Reading schema from database: %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
	fmt.Println("=== DATABASE SCHEMA ===")
	fmt.Println()

	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	// Connect to the database
	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		fmt.Println()
		fmt.Println("Make sure:")
		fmt.Println("1. The database URL is correct")
		fmt.Println("2. The database server is running")
		fmt.Println("3. You have the correct permissions")
		fmt.Println("4. The database exists")
		if connectTimeout > 0 {
			fmt.Printf("5. The connection completes within --connect-timeout (currently %s)\n", connectTimeout)
		} else {
			fmt.Println("5. --connect-timeout is disabled; the call will not time out at the application layer")
		}
		return err
	}
	defer dbschema.CloseAndWarn(conn)

	fmt.Printf("Connected to %s database successfully!\n", conn.Info().Dialect)
	fmt.Println()

	// Read the schema
	schemas := dbcli.ParseSchemas(opts.schemas)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return fmt.Errorf("error reading schema: %w", err)
	}

	// Format and display the schema
	dbsch := dbschematogo.ConvertDBSchemaToGoSchema(schema)
	info := conn.Info()
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(dbsch, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error rendering schema: %w", err)
	}
	output := strings.Join(statements, ";\n") + ";"
	fmt.Print(output)

	return nil
}
