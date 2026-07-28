// Package editor resolves and launches the operator-configured text editor for
// commands that open migration or plan files for interactive editing. The
// resolution order matches the native `ptah migrations edit` command: an
// explicit editor command wins, then $VISUAL, then $EDITOR.
package editor

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// ErrNoEditor reports that no editor command could be resolved. Callers may
// decorate it with the command-specific flags that would supply one.
var ErrNoEditor = errors.New("no editor configured: set $EDITOR or $VISUAL")

// Open launches the resolved editor on the given file paths, wired to the
// current terminal for interactive editing. An empty editorCmd falls back to
// $VISUAL, then $EDITOR; empty paths are skipped. The editor command may carry
// arguments of its own (for example "code --wait"), like git's core.editor.
func Open(editorCmd string, paths ...string) error {
	if editorCmd == "" {
		editorCmd = firstNonEmpty(os.Getenv("VISUAL"), os.Getenv("EDITOR"))
	}
	if strings.TrimSpace(editorCmd) == "" {
		return ErrNoEditor
	}
	fields := strings.Fields(editorCmd)
	args := append(append([]string{}, fields[1:]...), nonEmpty(paths)...)
	c := exec.Command(fields[0], args...) //nolint:gosec // the editor is operator-provided, like git's core.editor
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := c.Run(); err != nil {
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
