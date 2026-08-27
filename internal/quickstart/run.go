package quickstart

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Options configure one run of one program.
type Options struct {
	// PtahDir is prepended to PATH for the shell, so that the `ptah` the page
	// spells is the binary under test rather than whatever is installed.
	PtahDir string
	// WorkRoot is where the throwaway working directory is created. Empty
	// selects the operating system's temporary directory.
	WorkRoot string
	// Keep leaves the working directory behind for inspection.
	Keep bool
	// Timeout bounds one program. Zero selects tenMinutes.
	Timeout time.Duration
	// ShellPath overrides the interpreter. Empty resolves it from PATH.
	ShellPath string
}

const defaultTimeout = 10 * time.Minute

// Failure is one assertion that did not hold, in terms a reader of the page can
// act on without opening this package.
type Failure struct {
	// Page is the page the failing step is published on.
	Page string
	// Line is the 1-indexed line of the step's block.
	Line int
	// Step is the step's position among the program's steps.
	Step int
	// Command is the step's script as the page publishes it.
	Command string
	// Problem says what did not hold.
	Problem string
	// Stream is the stream the page said to look at, when there is one.
	Stream Stream
	// Expected is the block the page shows.
	Expected []string
	// Missing is the first expected line that was not found.
	Missing string
	// Got is what the step actually wrote to that stream.
	Got string
}

// Result is one program's run.
type Result struct {
	// Page is the page path.
	Page string
	// Shell is the shell the program was written for.
	Shell Shell
	// WorkDir is the throwaway directory the program ran in.
	WorkDir string
	// Script is the generated script, kept for reporting.
	Script string
	// ExitCode is the shell's exit status.
	ExitCode int
	// Failures are the assertions that did not hold.
	Failures []Failure
	// Steps and Asserted count what actually ran and what was checked.
	Steps    int
	Asserted int
}

// OK reports whether the program ran to the end and every expectation held.
func (r *Result) OK() bool { return r.ExitCode == 0 && len(r.Failures) == 0 }

// Run executes one page's program for one shell and checks every expectation
// the page publishes for it.
func Run(ctx context.Context, page *Page, shell Shell, opts Options) (*Result, error) {
	program, ok := page.Program(shell)
	if !ok {
		return nil, fmt.Errorf("%s publishes no %s steps", page.Path, shell)
	}

	script, err := RenderScript(program)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", page.Path, err)
	}

	staged, err := stage(program, script, opts)
	if err != nil {
		return nil, err
	}
	defer staged.cleanup()

	captured, err := execute(ctx, shell, staged, opts)
	if err != nil {
		return nil, err
	}

	result := &Result{
		Page: page.Path, Shell: shell, WorkDir: staged.workDir, Script: script,
		Steps: program.Steps(), ExitCode: captured.exitCode,
	}
	result.Failures, result.Asserted = Check(page.Path, program, captured.stdout, captured.stderr)
	return result, nil
}

// staging is where one program runs: a throwaway working directory, and the
// generated script beside rather than inside it.
type staging struct {
	workDir    string
	scriptPath string
	cleanup    func()
}

// capture is what one run of the generated script wrote, kept apart by stream.
type capture struct {
	stdout   string
	stderr   string
	exitCode int
}

// stage writes the script outside the working directory, because a page whose
// last step deletes its own directory would otherwise delete the script it is
// running from.
func stage(program *Program, script string, opts Options) (*staging, error) {
	root := opts.WorkRoot
	if root == "" {
		root = os.TempDir()
	}
	base, err := os.MkdirTemp(root, "ptah-quickstart-")
	if err != nil {
		return nil, fmt.Errorf("creating a working directory: %w", err)
	}
	cleanup := func() {
		if !opts.Keep {
			_ = os.RemoveAll(base)
		}
	}

	workDir := filepath.Join(base, "work")
	if err := os.Mkdir(workDir, 0o755); err != nil {
		cleanup()
		return nil, fmt.Errorf("creating a working directory: %w", err)
	}

	name := "script.sh"
	if program.Shell == PowerShell {
		name = "script.ps1"
	}
	scriptPath := filepath.Join(base, name)
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		cleanup()
		return nil, fmt.Errorf("writing the generated script: %w", err)
	}
	return &staging{workDir: workDir, scriptPath: scriptPath, cleanup: cleanup}, nil
}

func execute(ctx context.Context, shell Shell, staged *staging, opts Options) (*capture, error) {
	name, args, err := shellCommand(shell, staged.scriptPath, opts.ShellPath)
	if err != nil {
		return nil, err
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var outBuffer, errBuffer bytes.Buffer
	// #nosec G204 -- the interpreter comes from PATH and the only argument is a
	// script this package generated into its own temporary directory.
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = staged.workDir
	cmd.Stdout = &outBuffer
	cmd.Stderr = &errBuffer
	cmd.Stdin = nil
	cmd.Env = environment(opts.PtahDir)

	runErr := cmd.Run()
	captured := &capture{stdout: outBuffer.String(), stderr: errBuffer.String()}
	var exitError *exec.ExitError
	switch {
	case runErr == nil:
	case errors.As(runErr, &exitError):
		captured.exitCode = exitError.ExitCode()
	default:
		return nil, fmt.Errorf("running %s: %w", name, runErr)
	}
	return captured, nil
}

// shellCommand resolves the interpreter for a shell.
//
// On Windows the default is powershell.exe rather than pwsh: Windows PowerShell
// is what ships with the operating system, so it is the shell a reader of the
// page has, and pwsh is the fallback for a machine that carries only it.
func shellCommand(shell Shell, scriptPath, override string) (name string, args []string, err error) {
	if shell == Bash {
		if override == "" {
			override = "bash"
		}
		found, lookErr := exec.LookPath(override)
		if lookErr != nil {
			return "", nil, fmt.Errorf("no %s on PATH to run the Bash steps with: %w", override, lookErr)
		}
		return found, []string{scriptPath}, nil
	}

	candidates := []string{"pwsh"}
	if runtime.GOOS == "windows" {
		candidates = []string{"powershell", "pwsh"}
	}
	if override != "" {
		candidates = []string{override}
	}
	for _, candidate := range candidates {
		found, lookErr := exec.LookPath(candidate)
		if lookErr != nil {
			continue
		}
		return found, []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", scriptPath}, nil
	}
	return "", nil, fmt.Errorf("none of %s is on PATH to run the PowerShell steps with", strings.Join(candidates, ", "))
}

func environment(ptahDir string) []string {
	env := os.Environ()
	if ptahDir == "" {
		return env
	}
	prefixed := ptahDir + string(os.PathListSeparator) + os.Getenv("PATH")
	for i, entry := range env {
		if strings.HasPrefix(strings.ToUpper(entry), "PATH=") {
			env[i] = "PATH=" + prefixed
			return env
		}
	}
	return append(env, "PATH="+prefixed)
}

// Check compares one program's published output blocks against the two streams
// a run of it produced.
//
// It is separate from Run so that the assertion engine can be exercised over a
// transcript, with no shell and no binary involved. That is what makes the
// proof that this runner can fail a plain unit test rather than something only
// a workflow can demonstrate.
func Check(page string, program *Program, stdout, stderr string) (failures []Failure, asserted int) {
	outChunks, outSeen := splitOnSentinels(stdout, program.Steps())
	errChunks, errSeen := splitOnSentinels(stderr, program.Steps())

	for _, action := range program.Actions {
		if action.Kind != ActionStep {
			continue
		}
		index := action.Number - 1
		if !outSeen[index] {
			failures = append(failures, Failure{
				Page: page, Line: action.Line, Step: action.Number, Command: action.Body,
				Problem: "the step did not finish; the run stopped here",
				Got:     strings.TrimRight(errChunks[index], "\n"),
			})
			return failures, asserted
		}
		for _, expectation := range action.Expectations {
			chunk, seen := outChunks[index], outSeen[index]
			if expectation.Stream == Stderr {
				chunk, seen = errChunks[index], errSeen[index]
			}
			asserted++
			failure, ok := checkContains(chunk, expectation)
			if seen && ok {
				continue
			}
			if !seen {
				// Never skip the assertion here. A stream whose boundary went
				// missing is a broken run, and treating it as nothing to check
				// would report the same success as a passing assertion.
				failure = Failure{
					Problem:  "the step's " + string(expectation.Stream) + " was never delimited, so nothing could be checked",
					Stream:   expectation.Stream,
					Expected: expectation.Lines,
					Got:      strings.TrimRight(chunk, "\n"),
				}
			}
			failure.Page, failure.Line, failure.Step, failure.Command = page, action.Line, action.Number, action.Body
			failures = append(failures, failure)
		}
	}
	return failures, asserted
}

// checkContains asserts that every expected line appears, in order, in the
// stream the page named.
//
// Containment rather than equality, because the page's own word is "includes"
// and because at least one command prints an absolute path the page does not
// show.
func checkContains(got string, expectation Expectation) (Failure, bool) {
	gotLines := splitTrimmed(got)
	position := 0
	for _, want := range expectation.Lines {
		trimmed := strings.TrimRight(want, " \t\r")
		if trimmed == "" {
			continue
		}
		found := -1
		for i := position; i < len(gotLines); i++ {
			if gotLines[i] == trimmed {
				found = i
				break
			}
		}
		if found < 0 {
			return Failure{
				Problem:  "the page shows a line the command did not print",
				Stream:   expectation.Stream,
				Expected: expectation.Lines,
				Missing:  trimmed,
				Got:      strings.TrimRight(got, "\n"),
			}, false
		}
		position = found + 1
	}
	return Failure{}, true
}

func splitTrimmed(text string) []string {
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t\r")
	}
	return lines
}
