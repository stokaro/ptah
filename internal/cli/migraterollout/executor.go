package migraterollout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/envbinding"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/migrationrollout"
	"ptah.run/migration/migrator"
)

// envPrefix is the prefix the native tree binds its flags' variables with.
const envPrefix = "PTAH"

// pipeDrainDelay bounds how long a finished target run may keep its output
// open through a process it started, such as a pre-migration hook left running
// in the background.
const pipeDrainDelay = 5 * time.Second

// processExecutor runs each target as `ptah migrations up --json` and reads
// each back as `ptah migrations status --json`, in processes of their own.
type processExecutor struct {
	// program is the ptah binary running the rollout.
	program string
	// location is the --migrations-dir every target run reads.
	location        string
	plainHTTP       bool
	allowConfigExec bool
	// environ is the environment every target run starts from; the database
	// URL is added per target.
	environ []string
	workDir string
	// relay receives each run's standard error, a line at a time.
	relay *syncWriter
}

// Apply runs `ptah migrations up --json` against target and returns its run
// document.
func (e *processExecutor) Apply(ctx context.Context, target migrationrollout.Target) migrationrollout.Result {
	args := e.arguments("up", target)
	if target.ExpectSequencePath != "" {
		args = append(args, "--expect-sequence", target.ExpectSequencePath)
	}
	if target.AllowDestructive {
		args = append(args, "--allow-destructive")
	}
	stdout, err := e.run(ctx, target, args)
	if notStarted, ok := errors.AsType[*startError](err); ok {
		return migrationrollout.Result{NotStarted: true, Err: notStarted}
	}
	var run migrator.RunResult
	if decodeErr := decodeDocument(stdout, &run); decodeErr != nil {
		return migrationrollout.Result{Err: fmt.Errorf("the run left no document (%w): %w", decodeErr, describeExit(err))}
	}
	return migrationrollout.Result{Run: &run}
}

// Inspect reads target's revision state with `ptah migrations status --json`.
func (e *processExecutor) Inspect(ctx context.Context, target migrationrollout.Target) (*migrator.MigrationStatus, error) {
	stdout, err := e.run(ctx, target, e.arguments("status", target))
	if err != nil {
		var failure errorkind.Failure
		if decodeErr := decodeDocument(stdout, &failure); decodeErr == nil && failure.Error != nil {
			return nil, errors.New(failure.Error.Message)
		}
		return nil, describeExit(err)
	}
	var status migrator.MigrationStatus
	if err := decodeDocument(stdout, &status); err != nil {
		return nil, fmt.Errorf("read the status document: %w", err)
	}
	if status.ContractVersion != migrator.StatusContractVersion {
		return nil, fmt.Errorf("the status document has contract version %d, and this build reads %d",
			status.ContractVersion, migrator.StatusContractVersion)
	}
	return &status, nil
}

// arguments are what every run of verb against target is given. The values the
// rollout decides are passed as flags, which outrank the PTAH_* variables a run
// would otherwise read.
func (e *processExecutor) arguments(verb string, target migrationrollout.Target) []string {
	args := []string{
		"migrations", verb,
		"--migrations-dir", e.location,
		"--json",
		// A JSON log shares standard output with the document; text logs go
		// to standard error, which the rollout relays.
		"--log-format=text",
		// One address cannot serve a metrics endpoint for several runs.
		"--metrics-addr=",
	}
	if target.ConfigPath != "" {
		args = append(args, "--config", target.ConfigPath)
	}
	if e.plainHTTP {
		args = append(args, "--plain-http")
	}
	if e.allowConfigExec {
		args = append(args, "--allow-config-exec")
	}
	return args
}

// startError is a run that could not be started, so nothing reached the
// target.
type startError struct{ err error }

func (e *startError) Error() string { return "the run could not start: " + e.err.Error() }
func (e *startError) Unwrap() error { return e.err }

// run starts the program with args against target and waits for it. The
// database URL travels in the environment rather than on the command line,
// where any user of the machine could read it. When ctx ends, the run is asked
// to stop once; see interruptProcess.
func (e *processExecutor) run(ctx context.Context, target migrationrollout.Target, args []string) ([]byte, error) {
	cmd := exec.Command(e.program, args...) // #nosec G204 -- the program is this ptah binary, and args are flags it defines
	cmd.Env = targetEnvironment(e.environ, target)
	cmd.Dir = e.workDir
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	lines := &lineWriter{relay: e.relay, target: target}
	cmd.Stderr = lines
	cmd.WaitDelay = pipeDrainDelay
	configureProcess(cmd)
	if err := cmd.Start(); err != nil {
		return nil, &startError{err: err}
	}
	stop := context.AfterFunc(ctx, func() { interruptProcess(cmd.Process) })
	err := cmd.Wait()
	stop()
	lines.flush()
	return stdout.Bytes(), err
}

// childEnvironment is the environment every target run starts from: this
// process's, without the database URL the run would otherwise inherit and
// without any variable a target's url_env names. A run, and every hook or
// config program it starts, would otherwise hold the credentials of every
// other target in the plan; [targetEnvironment] gives each run back its own.
func childEnvironment(environ []string, plan migrationrollout.Plan) []string {
	dropped := []string{envbinding.Name(envPrefix, "db-url")}
	for _, stage := range plan.Stages {
		for _, target := range stage.Targets {
			if target.URLEnv != "" {
				dropped = append(dropped, target.URLEnv)
			}
		}
	}
	return slices.DeleteFunc(slices.Clone(environ), func(entry string) bool {
		name, _, _ := strings.Cut(entry, "=")
		return slices.Contains(dropped, name)
	})
}

// targetEnvironment is the environment of one run against target: base, the
// database URL, and the target's own url_env variable when it has one, so a
// hook or config that reads it finds the value the rollout resolved.
func targetEnvironment(base []string, target migrationrollout.Target) []string {
	environ := append(slices.Clone(base), envbinding.Name(envPrefix, "db-url")+"="+target.URL)
	if target.URLEnv != "" {
		environ = append(environ, target.URLEnv+"="+target.URL)
	}
	return environ
}

// decodeDocument reads the one JSON document a run printed.
func decodeDocument(stdout []byte, into any) error {
	if len(bytes.TrimSpace(stdout)) == 0 {
		return errors.New("standard output is empty")
	}
	if err := json.NewDecoder(bytes.NewReader(stdout)).Decode(into); err != nil {
		return fmt.Errorf("standard output is not a run document: %w", err)
	}
	return nil
}

// describeExit says how a run ended when it left nothing to read.
func describeExit(err error) error {
	if err == nil {
		return errors.New("it exited 0")
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return fmt.Errorf("it ended with %s", exitErr.ProcessState)
	}
	return err
}

// syncWriter serializes writes from several runs onto one stream.
type syncWriter struct {
	mu  sync.Mutex
	out io.Writer
}

func newSyncWriter(out io.Writer) *syncWriter { return &syncWriter{out: out} }

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.out.Write(p)
}

// lineWriter relays one run's standard error a line at a time, each line
// prefixed with the target's name and stripped of the target's credentials.
type lineWriter struct {
	relay   *syncWriter
	target  migrationrollout.Target
	pending []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.pending = append(w.pending, p...)
	for {
		end := bytes.IndexByte(w.pending, '\n')
		if end < 0 {
			return len(p), nil
		}
		w.emit(w.pending[:end])
		w.pending = w.pending[end+1:]
	}
}

func (w *lineWriter) flush() {
	if len(w.pending) > 0 {
		w.emit(w.pending)
		w.pending = nil
	}
}

func (w *lineWriter) emit(line []byte) {
	text := dburldisplay.RedactError(w.target.URL, errors.New(string(line))).Error()
	_, _ = fmt.Fprintf(w.relay, "[%s] %s\n", w.target.Name, text)
}
