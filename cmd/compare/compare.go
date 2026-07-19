package compare

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare generated schema with database",
	Long: `Compare the schema generated from Go entities with the current database schema.
	
This command shows differences between what your Go entities define and what
currently exists in the database, helping you identify what needs to be migrated.`,
	RunE: compareCommand,
}

const (
	rootDirFlag  = "root-dir"
	dbURLFlag    = "db-url"
	exitCodeFlag = "exit-code"
)

var compareFlags = map[string]cobraflags.Flag{
	rootDirFlag: &cobraflags.StringFlag{
		Name:  rootDirFlag,
		Value: "./",
		Usage: "Root directory to scan for Go entities",
	},
	dbURLFlag: &cobraflags.StringFlag{
		Name:  dbURLFlag,
		Value: "",
		Usage: "Database URL (required). Example: postgres://localhost:5432/dbname",
	},
	exitCodeFlag: &cobraflags.BoolFlag{
		Name:  exitCodeFlag,
		Value: false,
		Usage: "Exit with 1 when the schema diff is non-empty",
	},
	dbcli.ConnectTimeoutFlagName: dbcli.NewConnectTimeoutFlag(),
	dbcli.SchemasFlagName:        dbcli.NewSchemasFlag(),
}

var compareFlagsRegistered bool

func NewCompareCommand() *cobra.Command {
	if !compareFlagsRegistered {
		cobraflags.RegisterMap(compareCmd, compareFlags)
		compareFlagsRegistered = true
	}
	cmdutil.ConfigureCommand(compareCmd)
	return compareCmd
}

func compareCommand(cmd *cobra.Command, _ []string) error {
	out := cmd.OutOrStdout()
	rootDir := compareFlags[rootDirFlag].GetString()
	dbURL := compareFlags[dbURLFlag].GetString()
	exitOnDiff := compareFlags[exitCodeFlag].GetBool()

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	fmt.Fprintf(out, "Comparing schema from %s with database %s\n", rootDir, dbschema.FormatDatabaseURL(dbURL))
	fmt.Fprintln(out, "=== SCHEMA COMPARISON ===")
	fmt.Fprintln(out)

	// 1. Parse Go entities
	absPath, err := filepath.Abs(rootDir)
	if err != nil {
		return fmt.Errorf("error resolving path: %w", err)
	}

	result, err := goschema.ParseDir(absPath)
	if err != nil {
		return fmt.Errorf("error parsing Go entities: %w", err)
	}

	// 2. Connect to database and read schema
	connectTimeout, err := dbcli.ParseConnectTimeout(compareFlags[dbcli.ConnectTimeoutFlagName].GetString())
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

	schemas := dbcli.ParseSchemas(compareFlags[dbcli.SchemasFlagName].GetString())
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return fmt.Errorf("error reading database schema: %w", err)
	}

	// 3. Compare schemas (dialect-aware: MySQL/MariaDB RESTRICT == NO ACTION)
	info := conn.Info()
	diff := schemadiff.CompareWithDialect(result, dbSchema, info.Dialect)

	// 4. Display differences
	output, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(diff, result, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error generating schema diff SQL: %w", err)
	}
	fmt.Fprint(out, output)

	if exitOnDiff {
		return nonEmptyDiffExitCode(diff)
	}
	return nil
}

func nonEmptyDiffExitCode(diff *difftypes.SchemaDiff) error {
	if diff.HasChanges() {
		return exitcode.New(1, errors.New("schema diff is non-empty"))
	}
	return nil
}
