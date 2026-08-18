// Package root wires the Ptah command tree.
package root

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/db"
	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/introspect"
	"go.5x5.cz/ptah/cmd/license"
	"go.5x5.cz/ptah/cmd/migrations"
	"go.5x5.cz/ptah/cmd/oci"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/cmd/seed"
	sqlcmd "go.5x5.cz/ptah/cmd/sql"
	"go.5x5.cz/ptah/cmd/version"
	"go.5x5.cz/ptah/cmd/viz"
	"go.5x5.cz/ptah/internal/buildinfo"
)

const envPrefix = "PTAH"

// NewRootCommand returns the root Ptah command with every subcommand registered.
func NewRootCommand() *cobra.Command {
	info := buildinfo.Resolve()
	cmd := &cobra.Command{
		Use:   "ptah",
		Short: "Ptah schema management and migration tooling",
		Long:  rootLongDescription,
		// Version is what makes cobra register --version/-v at all; the
		// template below is what makes those spellings answer with the same
		// bytes as the `version` subcommand (stokaro/ptah#1064).
		Version: info.Version,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.SetVersionTemplate(versionTemplate(info))
	cmdutil.ConfigureCommandArgs(cmd, nil)

	cmd.AddCommand(introspect.NewIntrospectCommand())
	cmd.AddCommand(schema.NewSchemaCommand())
	cmd.AddCommand(db.NewDBCommand())
	cmd.AddCommand(migrations.NewMigrationsCommand())
	cmd.AddCommand(oci.NewCommand())
	cmd.AddCommand(seed.NewSeedCommand())
	cmd.AddCommand(sqlcmd.NewSQLCommand())
	cmd.AddCommand(viz.NewCommand())
	cmd.AddCommand(version.NewVersionCommand())
	cmd.AddCommand(license.NewLicenseCommand())

	cmdflags.InstallEnvBinding(envPrefix, cmd)

	return cmd
}

// versionTemplate renders build metadata in the exact format the `version`
// subcommand prints, so `ptah version`, `ptah --version` and `ptah -v` emit
// identical bytes instead of cobra's built-in "ptah version <v>" one-liner
// (stokaro/ptah#1064). A caller should not have to know which spelling it used
// in order to parse the answer.
//
// Cobra parses the string it is given as a text/template, so braces arriving
// from a build stamp are escaped rather than interpreted as template actions.
func versionTemplate(info buildinfo.Info) string {
	var block strings.Builder
	buildinfo.Write(&block, info)
	return strings.ReplaceAll(block.String(), "{{", `{{"{{"}}`)
}

// Execute runs the root command and exits the process with the command's
// declared exit-code contract.
func Execute(args ...string) {
	ExecuteCommand(NewRootCommand(), args...)
}

// ExecuteCommand runs cmd and exits the process with Ptah's CLI exit-code
// contract.
func ExecuteCommand(cmd *cobra.Command, args ...string) {
	if code := runCommand(cmd, args...); code != 0 {
		os.Exit(code) //revive:disable-line:deep-exit root owns the process exit contract
	}
}

// runCommand runs cmd and reports the status the process should exit with. It
// exists so that every release the command set up -- signal delivery above all
// -- is torn down before os.Exit, which runs no deferred function, and so that
// the exit-code contract can be exercised without ending the test binary.
func runCommand(cmd *cobra.Command, args ...string) int {
	cmd.SetArgs(args)

	// An interrupt cancels the command rather than killing the process, so the
	// releases it defers -- a dev-database container above all -- actually run
	// before the exit. See withInterruptCancel.
	ctx, interrupted, release := withInterruptCancel(context.Background(), cmd.ErrOrStderr())
	defer release()
	cmd.SetContext(ctx)

	err := executeWithRecovery(cmd)
	if sig := interrupted(); sig != nil {
		// An interrupted command reports the interrupt, not whatever error the
		// cancelation happened to surface as. What that status is belongs to
		// the surface: see interruptExitCode.
		return interruptExitCode(cmd, sig)
	}
	if err != nil {
		return exitcode.Code(err, 2)
	}
	return 0
}

func executeWithRecovery(cmd *cobra.Command) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = exitcode.New(2, fmt.Errorf("internal error: %v", recovered))
			fmt.Fprintf(cmd.ErrOrStderr(), "%s: %v\n", cmdutil.ErrorPrefix(cmd), err)
		}
	}()

	executed, err := cmd.ExecuteC()
	if executed == nil {
		executed = cmd
	}
	return cmdutil.NormalizeCommandError(executed, err, 2)
}

const rootLongDescription = `Ptah generates database schemas from Go entities,
compares desired schemas with live databases, and manages database migrations.

It supports PostgreSQL-family targets, MySQL, MariaDB, SQLite, ClickHouse, and
Spanner-oriented schema workflows. Scripts that expect the Atlas CLI can use
the separate ptah-compat binary, a drop-in Atlas replacement.`
