package readdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
)

var readDBCmd = &cobra.Command{
	Use:   "read",
	Short: "Read schema from database",
	Long: `Read and display the current schema from the specified database.
	
This command connects to the database and reads the existing schema,
displaying tables, columns, indexes, and constraints in a formatted output.`,
	RunE: readDBCommand,
}

const (
	dbURLFlag = "db-url"
)

var readDBFlags = map[string]cobraflags.Flag{
	dbURLFlag: &cobraflags.StringFlag{
		Name:  dbURLFlag,
		Value: "",
		Usage: "Database URL (required). Example: postgres://localhost:5432/dbname",
	},
	dbcli.ConnectTimeoutFlagName: dbcli.NewConnectTimeoutFlag(),
	dbcli.SchemasFlagName:        dbcli.NewSchemasFlag(),
}

var readDBFlagsRegistered bool

func NewReadDBCommand() *cobra.Command {
	if !readDBFlagsRegistered {
		cobraflags.RegisterMap(readDBCmd, readDBFlags)
		readDBFlagsRegistered = true
	}
	cmdutil.ConfigureCommand(readDBCmd)
	return readDBCmd
}

func readDBCommand(_ *cobra.Command, _ []string) error {
	dbURL := readDBFlags[dbURLFlag].GetString()

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	fmt.Printf("Reading schema from database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Println("=== DATABASE SCHEMA ===")
	fmt.Println()

	connectTimeout, err := dbcli.ParseConnectTimeout(readDBFlags[dbcli.ConnectTimeoutFlagName].GetString())
	if err != nil {
		return err
	}

	// Connect to the database
	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, dbURL)
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
	schemas := dbcli.ParseSchemas(readDBFlags[dbcli.SchemasFlagName].GetString())
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
