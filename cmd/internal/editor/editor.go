// Package editor resolves and launches the operator-configured text editor for
// commands that open migration or plan files for interactive editing. The
// resolution order matches the native `ptah migrations edit` command: an
// explicit editor command wins, then $VISUAL, then $EDITOR.
package editor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/google/shlex"
)

// ErrNoEditor reports that no editor command could be resolved. Callers may
// decorate it with the command-specific flags that would supply one.
var ErrNoEditor = errors.New("no editor configured: set $EDITOR or $VISUAL")

// Open launches the resolved editor on the given file paths, wired to the
// current terminal for interactive editing. An empty editorCmd falls back to
// $VISUAL, then $EDITOR; empty paths are skipped. Shell-style quoting is parsed
// without invoking a shell, so commands such as `code --wait` and quoted
// executable paths work without exposing the edited paths to shell expansion.
func Open(ctx context.Context, editorCmd string, paths ...string) error {
	if editorCmd == "" {
		editorCmd = firstNonEmpty(os.Getenv("VISUAL"), os.Getenv("EDITOR"))
	}
	if strings.TrimSpace(editorCmd) == "" {
		return ErrNoEditor
	}
	fields, err := shlex.Split(editorCmd)
	if err != nil {
		return fmt.Errorf("parse editor command %q: %w", editorCmd, err)
	}
	if len(fields) == 0 || strings.TrimSpace(fields[0]) == "" {
		return fmt.Errorf("parse editor command %q: command has no executable", editorCmd)
	}
	args := append(append([]string{}, fields[1:]...), nonEmpty(paths)...)
	c := exec.CommandContext(ctx, fields[0], args...) //nolint:gosec // the editor is operator-provided and runs directly without a shell
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
