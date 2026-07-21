package compare

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

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

const (
	rootDirFlag  = "root-dir"
	dbURLFlag    = "db-url"
	exitCodeFlag = "exit-code"
)

type options struct {
	rootDir        string
	dbURL          string
	exitOnDiff     bool
	connectTimeout string
	schemas        string
}

func NewCompareCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare generated schema with database",
		Long: `Compare the schema generated from Go entities with the current database schema.

This command shows differences between what your Go entities define and what
currently exists in the database, helping you identify what needs to be migrated.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return compareCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.rootDir, rootDirFlag, "./", "Root directory to scan for Go entities")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.BoolVar(&opts.exitOnDiff, exitCodeFlag, false, "Exit with 1 when the schema diff is non-empty")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
}

func compareCommand(cmd *cobra.Command, opts *options) error {
	out := cmd.OutOrStdout()

	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	fmt.Fprintf(out, "Comparing schema from %s with database %s\n", opts.rootDir, dbschema.FormatDatabaseURL(opts.dbURL))
	fmt.Fprintln(out, "=== SCHEMA COMPARISON ===")
	fmt.Fprintln(out)

	// 1. Parse Go entities
	absPath, err := filepath.Abs(opts.rootDir)
	if err != nil {
		return fmt.Errorf("error resolving path: %w", err)
	}

	result, err := goschema.ParseDir(absPath)
	if err != nil {
		return fmt.Errorf("error parsing Go entities: %w", err)
	}

	// 2. Connect to database and read schema
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

	schemas := dbcli.ParseSchemas(opts.schemas)
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

	if opts.exitOnDiff {
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
