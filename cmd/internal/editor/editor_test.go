package editor_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/editor"
)

// writeAppendScript writes an executable shell script that appends a marker
// line to every file passed to it, so tests can observe an "editing" session
// without spawning an interactive editor.
func writeAppendScript(t *testing.T, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "editor.sh")
	script := "#!/bin/sh\nfor f in \"$@\"; do\n  printf '%s\\n' \"" + marker + "\" >> \"$f\"\ndone\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { //nolint:gosec // test helper script must be executable
		t.Fatal(err)
	}
	return path
}

func TestOpen_NoEditorConfigured(t *testing.T) {
	c := qt.New(t)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")

	err := editor.Open("")

	c.Assert(err, qt.ErrorIs, editor.ErrNoEditor)
}

func TestOpen_ExplicitEditorRunsOnPaths(t *testing.T) {
	c := qt.New(t)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")
	script := writeAppendScript(t, "-- explicit")
	target := filepath.Join(t.TempDir(), "file.sql")
	c.Assert(os.WriteFile(target, []byte("SELECT 1;\n"), 0o600), qt.IsNil)

	err := editor.Open(script, target, "")

	c.Assert(err, qt.IsNil)
	content, readErr := os.ReadFile(target)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- explicit")
}

func TestOpen_VisualWinsOverEditor(t *testing.T) {
	c := qt.New(t)
	t.Setenv("VISUAL", writeAppendScript(t, "-- visual"))
	t.Setenv("EDITOR", writeAppendScript(t, "-- editor"))
	target := filepath.Join(t.TempDir(), "file.sql")
	c.Assert(os.WriteFile(target, nil, 0o600), qt.IsNil)

	err := editor.Open("", target)

	c.Assert(err, qt.IsNil)
	content, readErr := os.ReadFile(target)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- visual")
	c.Assert(string(content), qt.Not(qt.Contains), "-- editor")
}

func TestOpen_EditorFallsBackFromEmptyVisual(t *testing.T) {
	c := qt.New(t)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", writeAppendScript(t, "-- editor"))
	target := filepath.Join(t.TempDir(), "file.sql")
	c.Assert(os.WriteFile(target, nil, 0o600), qt.IsNil)

	err := editor.Open("", target)

	c.Assert(err, qt.IsNil)
	content, readErr := os.ReadFile(target)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- editor")
}

func TestOpen_FailingEditorReportsCommand(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "boom.sh")
	c.Assert(os.WriteFile(path, []byte("#!/bin/sh\nexit 42\n"), 0o700), qt.IsNil) //nolint:gosec // test helper script must be executable

	err := editor.Open(path, filepath.Join(t.TempDir(), "file.sql"))

	c.Assert(err, qt.ErrorMatches, `editor .* failed: .*`)
}
