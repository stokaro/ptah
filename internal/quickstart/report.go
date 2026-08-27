package quickstart

import (
	"fmt"
	"strings"
)

// FormatFailure renders one failed assertion for someone who has not opened
// this package: the page, the line, the command as the page publishes it, the
// line that was missing, and the stream it was looked for in.
func FormatFailure(shell Shell, failure Failure) string {
	var out strings.Builder
	fmt.Fprintf(&out, "FAIL %s:%d (%s step %d)\n", failure.Page, failure.Line, shell, failure.Step)
	out.WriteString("  the page publishes this command:\n")
	out.WriteString(indent(failure.Command, "    | "))
	if failure.Stream == "" {
		fmt.Fprintf(&out, "  %s\n", failure.Problem)
	} else {
		fmt.Fprintf(&out, "  %s, on %s\n", failure.Problem, failure.Stream)
	}
	if failure.Missing != "" {
		fmt.Fprintf(&out, "  the missing line is:\n    | %s\n", failure.Missing)
	}
	if len(failure.Expected) > 0 {
		out.WriteString("  the page shows:\n")
		out.WriteString(indent(strings.Join(failure.Expected, "\n"), "    | "))
	}
	out.WriteString("  the command wrote:\n")
	out.WriteString(indent(failure.Got, "    | "))
	return out.String()
}

// FormatResult renders one program's outcome, with every failure under it.
func FormatResult(result *Result) string {
	var out strings.Builder
	verdict := "OK"
	if !result.OK() {
		verdict = "FAILED"
	}
	fmt.Fprintf(&out, "%s %s (%s): %d step(s), %d assertion(s), shell exit %d\n",
		verdict, result.Page, result.Shell, result.Steps, result.Asserted, result.ExitCode)
	for _, failure := range result.Failures {
		out.WriteString(FormatFailure(result.Shell, failure))
	}
	if !result.OK() && len(result.Failures) == 0 {
		fmt.Fprintf(&out, "  the shell exited %d with every published output block matched; "+
			"a step the page shows no output for failed\n", result.ExitCode)
	}
	return out.String()
}

func indent(text, prefix string) string {
	if strings.TrimSpace(text) == "" {
		return prefix + "(nothing)\n"
	}
	var out strings.Builder
	for line := range strings.SplitSeq(strings.TrimRight(text, "\n"), "\n") {
		out.WriteString(prefix + line + "\n")
	}
	return out.String()
}
