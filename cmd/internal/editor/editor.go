// Package editor resolves and launches the operator-configured text editor for
// commands that open migration or plan files for interactive editing. The
// resolution order matches the native `ptah migrations edit` command: an
// explicit editor command wins, then $VISUAL, then $EDITOR.
package editor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/google/shlex"
	"github.com/mattn/go-isatty"

	"go.5x5.cz/ptah/internal/envbool"
)

// ErrNoEditor reports that no editor command could be resolved. Callers may
// decorate it with the command-specific flags that would supply one.
var ErrNoEditor = errors.New("no editor configured: set $EDITOR or $VISUAL")

// ErrNotInteractive reports that an editor session was requested on a stream
// that is not a terminal, so the editor would have nothing to read from.
var ErrNotInteractive = errors.New("standard input is not a terminal, so an editor session cannot be opened")

// AllowNonInteractiveEnvVar names the environment variable that declares the
// configured editor to be non-interactive (a script such as `sed -i`), which
// makes an editor session legitimate without a terminal.
//
// It is an environment variable rather than a flag on purpose: the Atlas
// surface registers no flag for it, and the compat surface must not grow flags
// Atlas does not have. The same reasoning applies on the native side, where the
// variable keeps one spelling across both binaries.
const AllowNonInteractiveEnvVar = "PTAH_ALLOW_NONINTERACTIVE_EDIT"

// Resolve returns the editor command [Open] would launch: an explicit
// editorCmd, then $VISUAL, then $EDITOR. It reports [ErrNoEditor] when none
// resolves, so a caller can refuse before doing expensive work it would have to
// throw away.
func Resolve(editorCmd string) (string, error) {
	if editorCmd == "" {
		editorCmd = firstNonEmpty(os.Getenv("VISUAL"), os.Getenv("EDITOR"))
	}
	if strings.TrimSpace(editorCmd) == "" {
		return "", ErrNoEditor
	}
	return editorCmd, nil
}

// RequireInteractive reports whether an editor session may be opened against
// in, which is the command's standard input.
//
// A terminal is required because an interactive editor started without one
// does not fail — it blocks, and a pipeline that waits forever is worse than
// one that stops. The refusal is therefore deterministic: no terminal and no
// declared non-interactive editor means the command stops before launching
// anything.
//
// The escape hatch is [AllowNonInteractiveEnvVar], for the scripted-editor
// workflow ($EDITOR set to something that edits and exits on its own).
func RequireInteractive(in io.Reader) error {
	allowed, err := nonInteractiveAllowed()
	if err != nil {
		return err
	}
	if allowed || isTerminal(in) {
		return nil
	}
	return fmt.Errorf("%w; set %s=1 when the configured editor edits non-interactively", ErrNotInteractive, AllowNonInteractiveEnvVar)
}

// allowNonInteractive is the declaration of the variable, made once, in the
// package that owns it. See [go.5x5.cz/ptah/internal/envbool]. An explicitly
// empty value is refused here too, which is stokaro/ptah#1334's one change to
// this reader.
// It is [go.5x5.cz/ptah/internal/envbool.Retained]: permitting a scripted
// editor in a non-interactive process adds no editor or migration capability
// the pinned binary lacks, so strict compatibility keeps it reachable.
var allowNonInteractive = envbool.New(AllowNonInteractiveEnvVar, false, envbool.Retained)

// nonInteractiveAllowed reads the opt-out. An unparsable value is an error
// rather than a silent false: an operator who believes the gate is lifted must
// not get a refusal from a typo, and one who believes it is closed must not get
// an editor launched by one.
func nonInteractiveAllowed() (bool, error) {
	return allowNonInteractive.Resolve()
}

// isTerminal reports whether in is an open terminal device. A stream that is
// not an *os.File at all (a test buffer, a captured pipe) is never one, which
// is also what makes the refusal reachable in a test.
//
// The check is an ioctl, not a `ModeCharDevice` bit test, and the difference
// decides whether the guard works at all: /dev/null IS a character device, and
// it is exactly what CI hands a process as standard input. A mode test
// therefore calls the most common non-interactive environment interactive and
// lets an editor launch into it.
func isTerminal(in io.Reader) bool {
	file, ok := in.(*os.File)
	if !ok || file == nil {
		return false
	}
	fd := file.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

// Open launches the resolved editor on the given file paths, wired to the
// current terminal for interactive editing. An empty editorCmd falls back to
// $VISUAL, then $EDITOR; empty paths are skipped. Shell-style quoting is parsed
// without invoking a shell, so commands such as `code --wait` and quoted
// executable paths work without exposing the edited paths to shell expansion.
func Open(ctx context.Context, editorCmd string, paths ...string) error {
	editorCmd, err := Resolve(editorCmd)
	if err != nil {
		return err
	}
	fields, err := shlex.Split(editorCmd)
	if err != nil {
		return fmt.Errorf("parse editor command %q: %w", editorCmd, err)
	}
	if len(fields) == 0 || strings.TrimSpace(fields[0]) == "" {
		return fmt.Errorf("parse editor command %q: command has no executable", editorCmd)
	}
	args := append(append([]string{}, fields[1:]...), nonEmpty(paths)...)
	c := exec.CommandContext(ctx, fields[0], args...) // #nosec -- the editor is operator-provided and runs directly without a shell
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := c.Run(); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return fmt.Errorf("editor %q failed: %w", editorCmd, err)
	}
	return nil
}

func nonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
