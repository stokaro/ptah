package quickstart

import (
	"fmt"
	"strings"
)

const (
	// heredocTerminator ends a Bash here-document holding a file the page tells
	// the reader to write.
	heredocTerminator = "PTAH_QUICKSTART_FILE"
	// hereStringTerminator ends the PowerShell equivalent. It must start at
	// column one, which is why the generated script never indents.
	hereStringTerminator = "'@"
)

// Sentinel is the line the generated script prints on both streams once step
// number has finished.
//
// It is what lets one shell process carry the whole page -- so `cd` and the
// rest of the shell state behave as they do for a reader -- while each step's
// two streams are still told apart afterwards.
func Sentinel(number int) string {
	return fmt.Sprintf("__ptah_quickstart_step_%d__", number)
}

// RenderScript turns one program into one script for its shell.
//
// File writes are emitted where the page puts them, so a file the page edits
// halfway through is rewritten at that point rather than before the run.
func RenderScript(program *Program) (string, error) {
	switch program.Shell {
	case Bash:
		return renderBash(program)
	case PowerShell:
		return renderPowerShell(program)
	default:
		return "", fmt.Errorf("unknown shell %q", program.Shell)
	}
}

func renderBash(program *Program) (string, error) {
	var out strings.Builder
	// set -euo pipefail is what makes a failing step end the run. Without it a
	// step that fails is followed by every later step, and the page's own
	// checksum and validation commands stop being gates.
	out.WriteString("set -euo pipefail\n")
	for _, action := range program.Actions {
		if action.Kind == ActionFile {
			if err := bashFile(&out, action); err != nil {
				return "", err
			}
			continue
		}
		fmt.Fprintf(&out, "\n%s\n", strings.TrimRight(action.Body, "\n"))
		fmt.Fprintf(&out, "printf '%%s\\n' '%s'\n", Sentinel(action.Number))
		fmt.Fprintf(&out, "printf '%%s\\n' '%s' >&2\n", Sentinel(action.Number))
	}
	return out.String(), nil
}

func bashFile(out *strings.Builder, action Action) error {
	if hasLine(action.Body, heredocTerminator) {
		return fmt.Errorf("file %s contains the here-document terminator %s on a line of its own", action.Path, heredocTerminator)
	}
	fmt.Fprintf(out, "\nmkdir -p \"$(dirname -- '%s')\"\n", action.Path)
	fmt.Fprintf(out, "cat >'%s' <<'%s'\n", action.Path, heredocTerminator)
	fmt.Fprintf(out, "%s\n%s\n", strings.TrimRight(action.Body, "\n"), heredocTerminator)
	return nil
}

func renderPowerShell(program *Program) (string, error) {
	var out strings.Builder
	// $ErrorActionPreference governs cmdlet errors only. A native binary that
	// exits non-zero does not stop a PowerShell script, so every step checks
	// $LASTEXITCODE explicitly -- the same rule the documented install snippet
	// follows.
	out.WriteString("$ErrorActionPreference = 'Stop'\n")
	// PowerShell 7.3 added a preference that turns a native command's non-zero
	// exit into a terminating error, and Windows PowerShell 5.1 has no such
	// thing. Turning it off gives both versions the same behavior, decided by
	// the $LASTEXITCODE check below rather than by which PowerShell the runner
	// found. Assigning a name 5.1 does not know is harmless there.
	out.WriteString("$PSNativeCommandUseErrorActionPreference = $false\n")
	for _, action := range program.Actions {
		if action.Kind == ActionFile {
			if err := powerShellFile(&out, action); err != nil {
				return "", err
			}
			continue
		}
		fmt.Fprintf(&out, "\n%s\n", strings.TrimRight(action.Body, "\n"))
		out.WriteString("if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) { exit $LASTEXITCODE }\n")
		fmt.Fprintf(&out, "Write-Output '%s'\n", Sentinel(action.Number))
		fmt.Fprintf(&out, "[Console]::Error.WriteLine('%s')\n", Sentinel(action.Number))
	}
	return out.String(), nil
}

func powerShellFile(out *strings.Builder, action Action) error {
	if hasLine(action.Body, hereStringTerminator) {
		return fmt.Errorf("file %s contains the here-string terminator %s on a line of its own", action.Path, hereStringTerminator)
	}
	// WriteAllText rather than Set-Content: Windows PowerShell 5.1 writes a
	// byte-order mark with -Encoding utf8, and a schema file that opens with one
	// is not the file the page told the reader to write.
	fmt.Fprintf(out, "\n$ptahQuickstartPath = Join-Path (Get-Location).Path '%s'\n", action.Path)
	out.WriteString("$ptahQuickstartDir = Split-Path -Parent $ptahQuickstartPath\n")
	out.WriteString("if ($ptahQuickstartDir -and -not (Test-Path -LiteralPath $ptahQuickstartDir)) " +
		"{ New-Item -ItemType Directory -Force -Path $ptahQuickstartDir | Out-Null }\n")
	fmt.Fprintf(out, "[System.IO.File]::WriteAllText($ptahQuickstartPath, @'\n%s\n%s + \"`n\")\n",
		strings.TrimRight(action.Body, "\n"), hereStringTerminator)
	return nil
}

func hasLine(body, want string) bool {
	for line := range strings.SplitSeq(body, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

// splitOnSentinels cuts one captured stream into one chunk per step.
//
// A chunk is reported as seen only when its sentinel arrived, so a run that
// stopped at step 4 is distinguishable from one where step 5 printed nothing.
// What the interrupted step managed to write is kept in its own chunk anyway:
// that text is the whole diagnosis, and dropping it would leave a report saying
// only that something failed.
func splitOnSentinels(text string, steps int) (chunks []string, seen []bool) {
	chunks = make([]string, steps)
	seen = make([]bool, steps)
	current := 0
	var buffer []string
	for line := range strings.SplitSeq(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		if current < steps && line == Sentinel(current+1) {
			chunks[current] = strings.Join(buffer, "\n")
			seen[current] = true
			buffer = nil
			current++
			continue
		}
		buffer = append(buffer, line)
	}
	if current < steps {
		chunks[current] = strings.Join(buffer, "\n")
	}
	return chunks, seen
}
