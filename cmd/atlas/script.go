package atlas

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasscript"
)

type scriptOptions struct {
	url   string
	file  string
	run   string
	quiet bool
}

// newAtlasScriptCommand builds `ptah-compat script`.
//
// Registered only outside the strict CE profile, and that is the whole
// placement decision. The pinned community binary does not register `script` at
// all, so under `PTAH_ATLAS_STRICT_COMPAT=1` -- which is where the conformance
// measurement runs -- this verb must not exist, or the measurement would report
// a divergence Ptah introduced. Outside it, an Atlas Pro user's scripts keep
// working under ptah-compat, which is what stokaro/ptah#951 asks for
// (stokaro/ptah#1017).
func newAtlasScriptCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "script",
		Short: "Run a declared data operation",
		Long: `Run a data operation declared in HCL: a masked report, a transactional
mutation, or a batched loop.

The three kinds are the block's first label. A query script reads and reports
and writes nothing. An exec script runs inside one transaction, and a failed
step, an unmet condition or an unmet expect_rows rolls all of it back. A loop
script walks an iterator and runs its body once per batch, each batch in its own
transaction, so a failure undoes that batch and leaves the earlier ones standing.

Masks rewrite values on the way out: REDACT, PARTIAL, HASH and REPLACE, applied
in declaration order with the first match winning.

The surface is reproduced from published information. Where that material is
silent the script is refused by name rather than run on a guess, because a data
operation accepted with a meaning nobody stated is the one that changes the
wrong rows quietly.`,
		RunE: runAtlasGroupHelp,
	}

	opts := scriptOptions{}
	for _, kind := range []atlasscript.Kind{
		atlasscript.KindLoop, atlasscript.KindQuery, atlasscript.KindExec,
	} {
		cmd.AddCommand(newAtlasScriptKindCommand(kind, &opts))
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func newAtlasScriptKindCommand(kind atlasscript.Kind, opts *scriptOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   string(kind),
		Short: "Run a declared " + string(kind) + " script",
		Long:  "Run a declared " + string(kind) + " script from --file against --url.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasScript(cmd, kind, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", "Database URL the script runs against (required)")
	flags.StringVar(&opts.file, "file", "", "HCL file declaring the script (required)")
	flags.StringVar(&opts.run, "run", "", "Name of the script to run; required when the file declares more than one")
	flags.BoolVar(&opts.quiet, "quiet", false, "Print only the script's product, not the per-step report")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasScript(cmd *cobra.Command, kind atlasscript.Kind, opts *scriptOptions) error {
	if opts.file == "" {
		return failAtlasCommand(cmd, fmt.Errorf(`required flag(s) "file" not set`))
	}
	if opts.url == "" {
		return failAtlasCommand(cmd, fmt.Errorf(`required flag(s) "url" not set`))
	}

	data, err := os.ReadFile(opts.file) // #nosec G304 -- the operator named this file
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("read --file: %w", err))
	}
	scripts, err := atlasscript.Parse(data, opts.file)
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	script, err := selectScript(scripts, kind, opts.run)
	if err != nil {
		return failAtlasCommand(cmd, err)
	}

	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.url)
	if err != nil {
		return failAtlasCommand(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer func() { _ = conn.Close() }()

	runOpts := atlasscript.RunOptions{Out: cmd.OutOrStdout()}
	if !opts.quiet {
		runOpts.Report = cmd.ErrOrStderr()
	}
	return runSelectedScript(cmd, conn, script, runOpts)
}

// runSelectedScript dispatches on the kind the block declared.
func runSelectedScript(
	cmd *cobra.Command, conn *dbschema.DatabaseConnection, script atlasscript.Script, runOpts atlasscript.RunOptions,
) error {
	ctx := cmd.Context()
	switch script.Kind {
	case atlasscript.KindQuery:
		_, err := atlasscript.RunQuery(ctx, conn, script, runOpts)
		return reportScriptResult(cmd, err)
	case atlasscript.KindExec:
		_, err := atlasscript.RunExec(ctx, conn, script, runOpts)
		return reportScriptResult(cmd, err)
	case atlasscript.KindLoop:
		_, err := atlasscript.RunLoop(ctx, conn, script, runOpts)
		return reportScriptResult(cmd, err)
	default:
		return failAtlasCommand(cmd, fmt.Errorf("unsupported script kind %q", script.Kind))
	}
}

// reportScriptResult separates a guard that held from a script that failed.
//
// A condition that does not hold is the script working -- a purge guarded by
// "only if there is something to purge" and finding nothing did its job -- so
// it exits 0 with a sentence rather than as a failure. Anything else is a
// failure and exits like one.
func reportScriptResult(cmd *cobra.Command, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, atlasscript.ErrConditionFalse) {
		fmt.Fprintf(cmd.ErrOrStderr(), "%s\nNothing to do.\n", err)
		return nil
	}
	return failAtlasCommand(cmd, err)
}

// selectScript picks the script to run, and refuses rather than guessing when
// the file holds more than one of the requested kind.
//
// Guessing here would run a data operation the operator did not name. --run is
// how they name it, and the refusal lists what is available so they can.
func selectScript(scripts []atlasscript.Script, kind atlasscript.Kind, name string) (atlasscript.Script, error) {
	matching := make([]atlasscript.Script, 0, len(scripts))
	for _, script := range scripts {
		if script.Kind != kind {
			continue
		}
		if name != "" && script.Name != name {
			continue
		}
		matching = append(matching, script)
	}

	switch len(matching) {
	case 1:
		return matching[0], nil
	case 0:
		if name != "" {
			return atlasscript.Script{}, fmt.Errorf(
				"the file declares no %s script named %q (it declares: %s)",
				kind, name, describeScripts(scripts))
		}
		return atlasscript.Script{}, fmt.Errorf(
			"the file declares no %s script (it declares: %s)", kind, describeScripts(scripts))
	default:
		return atlasscript.Script{}, fmt.Errorf(
			"the file declares %d %s scripts; name one with --run (it declares: %s)",
			len(matching), kind, describeScripts(scripts))
	}
}

func describeScripts(scripts []atlasscript.Script) string {
	if len(scripts) == 0 {
		return "nothing"
	}
	described := make([]string, 0, len(scripts))
	for _, script := range scripts {
		described = append(described, fmt.Sprintf("%s %q", script.Kind, script.Name))
	}
	return strings.Join(described, ", ")
}
