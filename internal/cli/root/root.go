// Package root wires the Ptah command tree.
package root

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/internal/buildinfo"
	assistcmd "ptah.run/internal/cli/assist"
	"ptah.run/internal/cli/banner"
	"ptah.run/internal/cli/db"
	"ptah.run/internal/cli/inference"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/cli/introspect"
	"ptah.run/internal/cli/license"
	mcpcmd "ptah.run/internal/cli/mcp"
	"ptah.run/internal/cli/migrations"
	"ptah.run/internal/cli/oci"
	"ptah.run/internal/cli/project"
	"ptah.run/internal/cli/schema"
	"ptah.run/internal/cli/seed"
	sqlcmd "ptah.run/internal/cli/sql"
	"ptah.run/internal/cli/version"
	"ptah.run/internal/cli/viz"
)

const envPrefix = "PTAH"

// withBanner draws the Ptah identity above the root command's own help.
//
// Cobra reaches the help three ways -- a bare `ptah`, `ptah --help` and `ptah
// help` -- and routes all three through one function, so the decision lives
// here once rather than at each entry. What keeps it out of a pipeline is the
// terminal half of the condition: a conformance run capturing stdout, a script
// reading the command list and an editor client all get the help alone.
//
// The banner is scoped to the root's own help. Cobra hands a child's help to
// the same function, and a wordmark above `ptah schema render --help` is
// chrome between the reader and the flags they asked for.
func withBanner(cmd *cobra.Command, release string) {
	help := cmd.HelpFunc()
	cmd.SetHelpFunc(func(target *cobra.Command, args []string) {
		out := target.OutOrStdout()
		banner.PrintIf(out, helpDrawsBanner(cmd, target) && banner.Wanted(out), "ptah", release)
		help(target, args)
	})
}

// helpDrawsBanner reports whether a help call is the root's own.
//
// It is separate from the terminal question so that each half can be measured.
// banner.Wanted answers false for every writer a test can hand it, so a
// combined predicate would assert "writes nothing" and stay green against a
// routing rule that had stopped selecting anything at all.
func helpDrawsBanner(root, target *cobra.Command) bool {
	return target == root
}

// NewRootCommand returns the root Ptah command with every subcommand registered.
func NewRootCommand() *cobra.Command {
	info := buildinfo.Resolve()
	cmd := &cobra.Command{
		Use:   "ptah",
		Short: rootShortDescription,
		Long:  rootLongDescription,
		// Version is what makes cobra register --version/-v at all; the
		// template below is what makes those spellings answer with the same
		// bytes as the `version` subcommand (stokaro/ptah#1064).
		Version: info.Version,
		// A bare `ptah` is the entry screen, and the entry screen is the
		// help. The banner belongs to whoever is reading it, so the help
		// function below decides whether to draw it and this reaches the
		// help like every other path into it.
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.SetVersionTemplate(versionTemplate(info))
	withBanner(cmd, info.Version)
	cmdutil.ConfigureCommandArgs(cmd, nil)

	cmd.AddCommand(inference.NewCommand())
	cmd.AddCommand(introspect.NewIntrospectCommand())
	cmd.AddCommand(schema.NewSchemaCommand())
	cmd.AddCommand(db.NewDBCommand())
	cmd.AddCommand(migrations.NewMigrationsCommand())
	cmd.AddCommand(oci.NewCommand())
	cmd.AddCommand(project.NewProjectCommand())
	cmd.AddCommand(seed.NewSeedCommand())
	cmd.AddCommand(sqlcmd.NewSQLCommand())
	cmd.AddCommand(viz.NewCommand())
	cmd.AddCommand(mcpcmd.NewCommand())
	cmd.AddCommand(assistcmd.NewCommand())
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

// runCommand is RunContext under a context the process's own signals cancel.
// It exists so that every release the command set up -- signal delivery above
// all -- is torn down before os.Exit, which runs no deferred function, and so
// that the exit-code contract can be exercised without ending the test binary.
func runCommand(cmd *cobra.Command, args ...string) int {
	// An interrupt cancels the command rather than killing the process, so the
	// releases it defers -- a dev-database container above all -- actually run
	// before the exit. See withInterruptCancel.
	ctx, interrupted, release := withInterruptCancel(context.Background(), cmd.ErrOrStderr())
	defer release()
	ctx, workFinished := cmdutil.WithWorkReport(ctx)

	code := RunContext(ctx, cmd, args...)
	sig := interrupted()
	if sig == nil {
		return code
	}
	// A verb that reported its work finished had nothing left for the signal to
	// stop, so the status it earned stands: `ptah migrations up` whose last
	// migration committed before the signal exits 0 against a database it fully
	// migrated. Anything else -- a failure, or a command that ends because it
	// was stopped -- reports the interrupt, not whatever error the cancelation
	// happened to surface as. What that status is belongs to the surface: see
	// interruptExitCode.
	if code == 0 && workFinished() {
		return code
	}
	return interruptExitCode(cmd, sig)
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

// rootShortDescription is the product in one line.
//
// It says what Ptah manages rather than listing the verbs it offers: the verb
// list read as the whole product while persistent inference state was half of
// it, and a list of six is also a list that goes stale. `.goreleaser.yaml`
// carries the same sentence for `brew info`, and a test holds the two together
// (stokaro/ptah#2361).
const rootShortDescription = "Ptah manages database change across schemas and " +
	"persistent inference state"

// rootLongDescription is the first sentence most users read about Ptah, so it
// describes the product rather than one of the six sources a schema can be
// written in.
//
// It names no engine and no engine count on purpose. Ptah renders for ten
// dialects, the number moves, and a list here is one more place for it to go
// stale -- the previous text named five families and left out Oracle, SQL
// Server, CockroachDB and YugabyteDB, all of which render. "ptah db
// capabilities" answers for the server the reader actually has.
//
// It also stops short of calling ptah-compat a drop-in replacement.
// docs/STYLE_GUIDE.md and atlas/license-boundary.md both reserve that claim for
// what the conformance evidence proves, and the conformance page says in as
// many words that green reports do not prove it.
const rootLongDescription = `Ptah manages database change across schemas and
persistent inference state.

For schemas, it compares a desired schema with a live database and either writes
versioned migrations or applies an approved plan directly. Use either route, or
both, across supported databases.

For inference state, it builds a candidate embedding generation beside the
active one, calls an external embedding endpoint, verifies the result, and
switches consumers with a rollback path.

Run "ptah db capabilities --db-url <url>" to see what Ptah resolves for a
specific server. Scripts written for the Atlas CLI can use the separate
ptah-compat binary, which presents an Atlas-compatible command surface.`
