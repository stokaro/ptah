package processcapture_test

import (
	"context"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/processcapture"
)

const processCaptureHelperFlag = "--process-capture-helper"

func TestMain(m *testing.M) {
	if len(os.Args) >= 3 && os.Args[1] == processCaptureHelperFlag {
		os.Exit(runProcessCaptureHelper(os.Args[2:]))
	}
	os.Exit(m.Run())
}

func runProcessCaptureHelper(args []string) int {
	if len(args) == 0 {
		return 2
	}
	switch args[0] {
	case "echo":
		_, _ = os.Stdout.WriteString(args[1])
	case "fail":
		_, _ = os.Stderr.WriteString(args[1])
		return 7
	case "sleep":
		time.Sleep(5 * time.Second)
	default:
		return 2
	}
	return 0
}

func TestRun_ExecutesLiteralArgvWithoutShell(t *testing.T) {
	c := qt.New(t)
	literal := "value; touch should-not-run\n"

	result, err := processcapture.Run(context.Background(), processcapture.Command{
		Args: helperCommand("echo", literal),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Stdout), qt.Equals, literal)
	c.Assert(result.Stderr, qt.Equals, "")
}

func TestRun_ReportsBoundedStdout(t *testing.T) {
	c := qt.New(t)

	_, err := processcapture.Run(context.Background(), processcapture.Command{
		Args:      helperCommand("echo", "abcdef"),
		MaxStdout: 4,
	})

	var failure *processcapture.Failure
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.Kind, qt.Equals, processcapture.FailureOutputLimit)
}

func TestRun_RetainsBoundedStderrOnExit(t *testing.T) {
	c := qt.New(t)

	_, err := processcapture.Run(context.Background(), processcapture.Command{
		Args:      helperCommand("fail", "abcdefgh"),
		MaxStderr: 4,
	})

	var failure *processcapture.Failure
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.Kind, qt.Equals, processcapture.FailureStartOrExit)
	c.Assert(failure.Stderr, qt.Equals, "efgh")
}

func TestRun_ClassifiesCallerCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	c.Cleanup(cancel)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := processcapture.Run(ctx, processcapture.Command{
		Args: helperCommand("sleep"),
	})

	var failure *processcapture.Failure
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.Kind, qt.Equals, processcapture.FailureCanceled)
	c.Assert(err, qt.ErrorIs, context.Canceled)
}

func TestRun_ClassifiesCommandTimeout(t *testing.T) {
	c := qt.New(t)

	_, err := processcapture.Run(context.Background(), processcapture.Command{
		Args:    helperCommand("sleep"),
		Timeout: 50 * time.Millisecond,
	})

	var failure *processcapture.Failure
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.Kind, qt.Equals, processcapture.FailureTimedOut)
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}

func helperCommand(mode string, args ...string) []string {
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return append([]string{executable, processCaptureHelperFlag, mode}, args...)
}
