// Package readdb implements "ptah db read", which introspects a live database
// and displays its tables, columns, indexes, and constraints.
package readdb

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/rolescope"
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

func readDBCommand(cmd *cobra.Command, opts *options) error {
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	stderr := cmd.ErrOrStderr()
	fmt.Fprintf(stderr, "Reading schema from database: %s\n", dbschema.FormatDatabaseURL(opts.dbURL))

	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	// Connect to the database
	connectCtx, cancelConnect := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		fmt.Fprintf(stderr, "Error connecting to database: %v\n\n", err)
		fmt.Fprintln(stderr, "Make sure:")
		fmt.Fprintln(stderr, "1. The database URL is correct")
		fmt.Fprintln(stderr, "2. The database server is running")
		fmt.Fprintln(stderr, "3. You have the correct permissions")
		fmt.Fprintln(stderr, "4. The database exists")
		if connectTimeout > 0 {
			fmt.Fprintf(stderr, "5. The connection completes within --connect-timeout (currently %s)\n", connectTimeout)
		} else {
			fmt.Fprintln(stderr, "5. --connect-timeout is disabled; the call will not time out at the application layer")
		}
		return err
	}
	defer dbschema.CloseAndWarn(conn)

	fmt.Fprintf(stderr, "Connected to %s database successfully!\n", conn.Info().Dialect)

	// Read the schema
	schemas := dbcli.ParseSchemas(opts.schemas)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return fmt.Errorf("error reading schema: %w", err)
	}

	// A description scoped to the roles the schemas use omits roles that exist
	// on the server, and an operator reading the output has no other way to
	// learn that. See stokaro/ptah#1267.
	rolescope.ReportUndescribed(stderr, schema)

	// Format and display the schema
	dbsch := dbschematogo.ConvertDBSchemaToGoSchema(schema)
	info := conn.Info()
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(dbsch, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error rendering schema: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), strings.Join(statements, "\n\n"))

	return nil
}
