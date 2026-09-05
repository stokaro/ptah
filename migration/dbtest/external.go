package dbtest

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// ErrExternalNotAuthorized is returned when a case contains an external step
// and the run did not authorize one.
//
// It is a load failure rather than a case failure, and it happens before any
// database is provisioned: a test directory that asks to run a program is a
// decision the operator makes, not a result the report describes.
var ErrExternalNotAuthorized = errors.New("external test step is not authorized")

// DefaultExternalTimeout bounds an external step that does not finish.
//
// A test runner that can hang forever on a program it started is a test runner
// that cannot be used unattended, and the bound is Ptah's rather than the
// document's: a test file cannot raise, lower, or remove it.
const DefaultExternalTimeout = 5 * time.Minute

// ExternalStep runs a program as a test step.
//
// The program is executed directly, never through a shell: Program is an
// argument vector whose first element is the executable, so a value containing
// spaces, quotes, semicolons or redirections is one argument rather than a
// command line. Nothing in a test file is interpreted as shell syntax.
//
// Running one is refused unless [Options.AllowExternalCommands] is set. The
// step names a program on the machine running the suite, which is a larger
// authority than the rest of a test file has, so it is granted rather than
// assumed.
type ExternalStep struct {
	// Program is the argument vector. The first element is the executable and
	// must be present; a relative name is resolved through PATH.
	Program []string `yaml:"program"`
	// WorkingDir is the directory the program runs in. Empty means the
	// process's own working directory.
	WorkingDir string `yaml:"working_dir"`
	// Output, when set, requires the program's combined output to equal this
	// value after surrounding whitespace is removed.
	Output *string `yaml:"output"`
	// Match, when set, requires the program's combined output to match this
	// unanchored regular expression.
	Match string `yaml:"match"`
}

// validate rejects a step that cannot run or whose expectation is ambiguous.
//
// Both refusals happen at load time, before any database exists, which is what
// keeps a malformed external step from provisioning one first.
func (e *ExternalStep) validate() error {
	if len(e.Program) == 0 || strings.TrimSpace(e.Program[0]) == "" {
		return fmt.Errorf("external requires a program to run")
	}
	if e.Output != nil && strings.TrimSpace(e.Match) != "" {
		return fmt.Errorf("external must set at most one of output or match, but both are set")
	}
	if strings.TrimSpace(e.Match) == "" {
		return nil
	}
	if _, err := regexp.Compile(e.Match); err != nil {
		return fmt.Errorf("external match %q is not a valid regular expression: %w", e.Match, err)
	}
	return nil
}

// runExternal executes the step's program and checks its expectation.
//
// The context bounds the run: cancellation reaches the process, and a step with
// no deadline of its own inherits [DefaultExternalTimeout] so an unattended
// suite cannot be held open by a program that never exits.
func (r *runner) runExternal(ctx context.Context, step *ExternalStep) (passed bool, detail string) {
	timeout := r.externalTimeout
	if timeout <= 0 {
		timeout = DefaultExternalTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// #nosec G204 -- the argument vector is authorized configuration, not user
	// input: an external step runs only when the caller set
	// Options.AllowExternalCommands, and Program is executed as argv with no
	// shell, so its elements cannot become command syntax.
	command := exec.CommandContext(ctx, step.Program[0], step.Program[1:]...)
	command.Dir = step.WorkingDir

	raw, err := command.CombinedOutput()
	output := strings.TrimSpace(string(raw))
	if err != nil {
		return false, fmt.Sprintf("external %v failed: %v: %s", step.Program, err, output)
	}
	return checkExternalOutput(step, output)
}

// checkExternalOutput applies whichever expectation the step carries.
//
// A step with neither only asserts that the program exited successfully, which
// is the useful default for a fixture whose job is a side effect rather than a
// value.
func checkExternalOutput(step *ExternalStep, output string) (passed bool, detail string) {
	if step.Output != nil {
		expected := strings.TrimSpace(*step.Output)
		if output != expected {
			return false, fmt.Sprintf("expected external output %q, got %q", expected, output)
		}
		return true, fmt.Sprintf("external output %q", output)
	}
	if strings.TrimSpace(step.Match) != "" {
		pattern, err := regexp.Compile(step.Match)
		if err != nil {
			return false, fmt.Sprintf("external match %q is not a valid regular expression: %v", step.Match, err)
		}
		if !pattern.MatchString(output) {
			return false, fmt.Sprintf("expected external output %q to match %q", output, step.Match)
		}
		return true, fmt.Sprintf("external output matches %q", step.Match)
	}
	return true, "external ok"
}

// refuseExternalSteps reports the first external step in cases.
//
// The caller decides whether to ask: authorization is not a parameter here,
// because a helper that takes the answer and returns nil for one value of it is
// the caller's `if` moved somewhere a reader has to go looking for it.
//
// It runs over every case before the first is executed, so authorization is
// decided for the whole run rather than discovered when execution happens to
// reach the step -- a suite whose last case runs a program must not provision a
// database, apply a schema, and then refuse.
func refuseExternalSteps(cases []Case) error {
	for caseIndex := range cases {
		steps := cases[caseIndex].Steps
		for stepIndex := range steps {
			if steps[stepIndex].External == nil {
				continue
			}
			return fmt.Errorf(
				"%w: test case %q, step %d runs %v; set %s to authorize it",
				ErrExternalNotAuthorized,
				cases[caseIndex].Name,
				stepIndex+1,
				steps[stepIndex].External.Program,
				AllowExternalCommandsEnvVar,
			)
		}
	}
	return nil
}

// AllowExternalCommandsEnvVar names the environment variable the native test
// commands read to authorize external steps. The refusal names it so an
// operator reading a report knows what to set.
const AllowExternalCommandsEnvVar = "PTAH_ALLOW_EXTERNAL_TEST_COMMAND"
