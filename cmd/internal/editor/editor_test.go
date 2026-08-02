package editor_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/editor"
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

	err := editor.Open(context.Background(), "")

	c.Assert(err, qt.ErrorIs, editor.ErrNoEditor)
}

func TestOpen_ExplicitEditorRunsOnPaths(t *testing.T) {
	c := qt.New(t)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")
	script := writeAppendScript(t, "-- explicit")
	target := filepath.Join(t.TempDir(), "file.sql")
	c.Assert(os.WriteFile(target, []byte("SELECT 1;\n"), 0o600), qt.IsNil)

	err := editor.Open(context.Background(), script, target, "")

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

	err := editor.Open(context.Background(), "", target)

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

	err := editor.Open(context.Background(), "", target)

	c.Assert(err, qt.IsNil)
	content, readErr := os.ReadFile(target)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- editor")
}

func TestOpen_FailingEditorReportsCommand(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "boom.sh")
	c.Assert(os.WriteFile(path, []byte("#!/bin/sh\nexit 42\n"), 0o700), qt.IsNil) //nolint:gosec // test helper script must be executable

	err := editor.Open(context.Background(), path, filepath.Join(t.TempDir(), "file.sql"))

	c.Assert(err, qt.ErrorMatches, `editor .* failed: .*`)
}

func TestOpen_ParsesQuotedExecutablePathAndEmptyArgument(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(t.TempDir(), "editor with spaces")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	script := filepath.Join(dir, "capture args.sh")
	c.Assert(os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' \"$#\" > \"$3\"\nprintf '<%s>\\n' \"$1\" \"$2\" >> \"$3\"\n"), 0o700), qt.IsNil) //nolint:gosec // test helper script must be executable
	target := filepath.Join(t.TempDir(), "args.txt")

	err := editor.Open(context.Background(), `"`+script+`" -a ''`, target)

	c.Assert(err, qt.IsNil)
	content, readErr := os.ReadFile(target)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Equals, "3\n<-a>\n<>\n")
}

func TestOpen_RejectsMalformedQuotedCommand(t *testing.T) {
	c := qt.New(t)

	err := editor.Open(context.Background(), `"unterminated`, "ignored.sql")

	c.Assert(err, qt.ErrorMatches, `parse editor command .*: EOF found when expecting closing quote`)
}

func TestOpen_RejectsCommandWithoutExecutable(t *testing.T) {
	c := qt.New(t)

	err := editor.Open(context.Background(), "# comment only", "ignored.sql")

	c.Assert(err, qt.ErrorMatches, `parse editor command .*: command has no executable`)
}

func TestOpen_StopsEditorWhenContextExpires(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "blocking-editor.sh")
	c.Assert(os.WriteFile(path, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700), qt.IsNil) //nolint:gosec // test helper script must be executable
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := editor.Open(ctx, path, filepath.Join(t.TempDir(), "file.sql"))

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}
