// Package migraterollout implements `ptah migrations rollout`: the native
// surface of the staged rollout in internal/migrationrollout.
//
// The shared package decides which target runs when and what its outcome
// means. This package reads the definition and the state file, binds the
// release, and runs each target as `ptah migrations up --json` in a process of
// its own -- the same binary, the same command, the same guards. A process per
// target is what lets targets run in parallel: a command run in this process
// holds the process-wide logging and telemetry state for its whole duration.
package migraterollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/internal/buildinfo"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/execpolicy"
	"ptah.run/internal/migrationrollout"
)

const (
	definitionFlag    = "definition"
	migrationsDirFlag = "migrations-dir"
	stateFileFlag     = "state-file"
	retryFailedFlag   = "retry-failed"
	jsonFlag          = "json"
)

// planCommand is the command path a plan failure document names.
const planCommand = "migrations rollout plan"

// mode says whether an invocation starts a rollout or resumes one.
type mode int

const (
	startMode mode = iota
	resumeMode
)

// command is the command path a document of this mode names.
func (m mode) command() string {
	if m == resumeMode {
		return "migrations rollout resume"
	}
	return "migrations rollout run"
}

// verb is how progress output names what the invocation does.
func (m mode) verb() string {
	if m == resumeMode {
		return "Resuming"
	}
	return "Starting"
}

// NewRolloutCommand returns the `rollout` namespace of `ptah migrations`.
func NewRolloutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollout",
		Short: "Roll a migration release out to many databases in stages",
		Long: `Roll one versioned migration release out to many databases in stages.

A rollout definition names the target databases and groups them into stages: a
canary first, then the stages that depend on it, each with its own concurrency,
failure policy and success requirement. Every target runs "ptah migrations up"
in a process of its own, so every guard that command applies to one database
applies to each target unchanged.

The run is bound to one release -- the content digest of a local directory or
the digest of an OCI artifact -- and records the outcome of every stage and
target in a state file. "resume" continues an interrupted or failed run after
reading each started target back from its own revision table.

Only versioned migrations are rolled out. Direct schema changes are not: a
schema plan is computed for one database and authorizes nothing on another.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	cmd.AddCommand(newPlanCommand(), newRunCommand(), newResumeCommand())
	return cmd
}

type planOptions struct {
	definition string
	jsonOutput bool
}

func newPlanCommand() *cobra.Command {
	opts := planOptions{}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Validate a rollout definition and print its stages",
		Long: `Validate a rollout definition and print the stages and targets it resolves
to, in the order they run. Nothing is contacted: no database, no registry.

Every broken rule is reported at once: duplicate targets, unknown references,
a target in two stages or none, dependency cycles, and a stage that could start
before the canary succeeds.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPlan(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.definition, definitionFlag, "", "Path to the rollout definition, a YAML file (required)")
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false, "Print the resolved plan as one JSON document")
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func runPlan(cmd *cobra.Command, opts planOptions) error {
	plan, err := loadPlan(opts.definition)
	if err != nil && opts.jsonOutput {
		return failDocument(cmd.OutOrStdout(), migrationrollout.PlanContractVersion, planCommand, err)
	}
	if err != nil {
		return err
	}
	if opts.jsonOutput {
		return writeJSON(cmd.OutOrStdout(), plan.Document())
	}
	writePlan(cmd.OutOrStdout(), plan)
	return nil
}

type runOptions struct {
	definition    string
	migrationsDir string
	stateFile     string
	plainHTTP     bool
	jsonOutput    bool
	retryFailed   bool
}

func newRunCommand() *cobra.Command {
	opts := runOptions{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start a staged rollout of a migration release",
		Long: `Start a staged rollout of one migration release and record it in a new
state file.

The release is bound before anything runs: a local directory is captured once
and every target reads that copy, and an oci:// tag is resolved to its digest
once and every target pulls that digest. Each target then runs
"ptah migrations up --json" in a process of its own, with its own migration
lock, integrity check, dirty-revision guard and pre-migration checks.

The state file must not exist yet; "resume" continues a recorded run. Exit
status 0 means every stage succeeded and 1 means the rollout ran and a stage
failed or was blocked; the state file says which target did what.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRollout(cmd, opts, startMode)
		},
	}
	registerRunFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func newResumeCommand() *cobra.Command {
	opts := runOptions{}
	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Continue a staged rollout recorded in a state file",
		Long: `Continue a staged rollout recorded in a state file.

The definition must resolve to the plan the run started with, and the release
must hold the content the run is bound to: a changed directory or a tag that
now names another digest is refused.

Every target the run started is read back from its own revision table before
anything runs. A target that holds every migration of the release counts as
succeeded. A target with a dirty revision row is never retried by a rollout:
recover it with "ptah migrations up --allow-dirty" or "ptah migrations repair"
against that database first. A failed target with a clean revision table runs
again only with --retry-failed. Targets the run never started run in turn.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRollout(cmd, opts, resumeMode)
		},
	}
	registerRunFlags(cmd, &opts)
	cmd.Flags().BoolVar(&opts.retryFailed, retryFailedFlag, false,
		"Run again a failed or partial target whose revision table is clean; a dirty revision row is never retried")
	// A decision to run a failed target again is taken by whoever types it, so
	// an exported variable cannot take it for every resume that follows.
	if err := cmdflags.DisableEnvBinding(cmd.Flags(), retryFailedFlag); err != nil {
		panic(err)
	}
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerRunFlags(cmd *cobra.Command, opts *runOptions) {
	flags := cmd.Flags()
	flags.StringVar(&opts.definition, definitionFlag, "", "Path to the rollout definition, a YAML file (required)")
	flags.StringVar(&opts.migrationsDir, migrationsDirFlag, "", "Local directory or oci:// reference holding the migration release (required)")
	flags.StringVar(&opts.stateFile, stateFileFlag, "", "Path to the rollout state file, which records every stage and target outcome (required)")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false, "Print the rollout state as one JSON document; progress goes to standard error")
}

// runRollout starts or resumes a rollout.
func runRollout(cmd *cobra.Command, opts runOptions, m mode) error {
	state, err := executeRollout(cmd, opts, m)
	if state == nil && opts.jsonOutput {
		return failDocument(cmd.OutOrStdout(), migrationrollout.StateContractVersion, m.command(), err)
	}
	if state == nil {
		return err
	}
	if opts.jsonOutput {
		if writeErr := writeJSON(cmd.OutOrStdout(), state); writeErr != nil {
			return errors.Join(err, writeErr)
		}
	} else {
		writeSummary(cmd.OutOrStdout(), state, opts.stateFile)
	}
	if err != nil {
		return err
	}
	// Every stage the run could reach is decided, so an interrupt arriving
	// now stopped nothing; see cmdutil.ReportWorkFinished.
	cmdutil.ReportWorkFinished(cmd.Context())
	if state.Status != migrationrollout.RunSucceeded {
		return exitcode.New(1, fmt.Errorf("the rollout %s: %s", state.Status, unmetStages(state)))
	}
	return nil
}

// executeRollout does the work of run and resume. It returns a nil state when
// it refused before any target ran.
func executeRollout(cmd *cobra.Command, opts runOptions, m mode) (*migrationrollout.State, error) {
	if err := requireFlags(opts); err != nil {
		return nil, err
	}
	if err := refuseGroupBypasses(os.LookupEnv); err != nil {
		return nil, err
	}
	plan, err := loadPlan(opts.definition)
	if err != nil {
		return nil, err
	}
	unlock, err := migrationrollout.LockState(opts.stateFile)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unlock() }()
	var state *migrationrollout.State
	if m == resumeMode {
		state, err = migrationrollout.ReadState(opts.stateFile)
	} else {
		err = refuseExistingState(opts.stateFile)
	}
	if err != nil {
		return nil, err
	}
	bound, err := bindRelease(cmd.Context(), opts.migrationsDir, opts.plainHTTP)
	if err != nil {
		return nil, err
	}
	defer func() { _ = bound.cleanup() }()
	if m == resumeMode {
		if err := state.Bind(plan, bound.release); err != nil {
			return nil, err
		}
		state.Definition = opts.definition
	} else {
		state = migrationrollout.NewState(plan, bound.release, opts.definition, time.Now())
		if err := migrationrollout.CreateState(opts.stateFile, state); err != nil {
			return nil, err
		}
	}
	state.PtahVersion = buildinfo.Resolve().Version
	return state, runTargets(cmd, opts, plan, state, bound, m)
}

func runTargets(
	cmd *cobra.Command,
	opts runOptions,
	plan migrationrollout.Plan,
	state *migrationrollout.State,
	bound boundRelease,
	m mode,
) error {
	program, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate the ptah binary each target runs: %w", err)
	}
	workDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("read the working directory: %w", err)
	}
	diagnostics := newSyncWriter(cmd.ErrOrStderr())
	progress := io.Writer(diagnostics)
	if !opts.jsonOutput {
		progress = cmd.OutOrStdout()
	}
	writeHeader(progress, state, m)
	runner := &migrationrollout.Runner{
		Executor: &processExecutor{
			program:         program,
			location:        bound.location,
			plainHTTP:       opts.plainHTTP,
			allowConfigExec: execpolicy.From(cmd.Context()).Mode == execpolicy.RunSelected,
			environ:         childEnvironment(os.Environ(), plan),
			workDir:         workDir,
			relay:           diagnostics,
		},
		Save:    func(state *migrationrollout.State) error { return migrationrollout.WriteState(opts.stateFile, state) },
		Observe: newReporter(progress).observe,
	}
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	if m == resumeMode {
		return runner.Resume(ctx, plan, state, migrationrollout.ResumeOptions{RetryFailed: opts.retryFailed})
	}
	return runner.Run(ctx, plan, state)
}

func requireFlags(opts runOptions) error {
	var missing []string
	for name, value := range map[string]string{
		definitionFlag:    opts.definition,
		migrationsDirFlag: opts.migrationsDir,
		stateFileFlag:     opts.stateFile,
	} {
		if value == "" {
			missing = append(missing, "--"+name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	slices.Sort(missing)
	return fmt.Errorf("%s required", strings.Join(missing, ", "))
}

func refuseExistingState(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("the state file %s already records a rollout; continue it with \"ptah migrations rollout resume\", "+
			"or name a new --%s to start another", path, stateFileFlag)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read rollout state: %w", err)
	}
	return nil
}

// loadPlan reads and resolves a definition file.
func loadPlan(path string) (migrationrollout.Plan, error) {
	if path == "" {
		return migrationrollout.Plan{}, fmt.Errorf("--%s required", definitionFlag)
	}
	data, err := os.ReadFile(path) // #nosec G304 -- the operator's own --definition
	if err != nil {
		return migrationrollout.Plan{}, fmt.Errorf("read rollout definition: %w", err)
	}
	definition, err := migrationrollout.Decode(data)
	if err != nil {
		return migrationrollout.Plan{}, fmt.Errorf("%s: %w", path, err)
	}
	plan, err := definition.Resolve(migrationrollout.ResolveOptions{BaseDir: filepath.Dir(path)})
	if err != nil {
		return migrationrollout.Plan{}, fmt.Errorf("%s: %w", path, err)
	}
	return plan, nil
}

// failDocument prints err as the failure document of command on out and
// returns it, so a --json caller parsing standard output always finds one
// document there.
func failDocument(out io.Writer, contractVersion int, command string, err error) error {
	return errors.Join(err, writeJSON(out, errorkind.NewFailure(contractVersion, command, err)))
}

func writeJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write rollout document: %w", err)
	}
	return nil
}
