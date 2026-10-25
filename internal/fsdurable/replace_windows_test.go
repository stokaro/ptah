package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"golang.org/x/sys/windows"

	"ptah.run/internal/fsdurable"
)

// replacing writes the destination and a staged file beside it, and returns
// their paths.
func replacing(c *qt.C) (staged, destination string) {
	dir := c.TempDir()
	staged, destination = filepath.Join(dir, "staged"), filepath.Join(dir, "destination")
	c.Assert(os.WriteFile(destination, []byte("old"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(staged, []byte("new"), 0o600), qt.IsNil)
	return staged, destination
}

// A reader of the destination, such as a concurrent install reading its
// record, holds it only briefly; the replace waits it out.
func TestReplaceFile_WaitsOutAReader(t *testing.T) {
	c := qt.New(t)
	staged, destination := replacing(c)
	reader, err := os.Open(destination)
	c.Assert(err, qt.IsNil)
	// The premise: while the reader holds it, Windows refuses the move.
	c.Assert(os.Rename(staged, destination), qt.ErrorIs, windows.ERROR_ACCESS_DENIED)
	closed := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		closed <- reader.Close()
	}()

	err = fsdurable.ReplaceFile(staged, destination)

	c.Assert(err, qt.IsNil)
	c.Assert(<-closed, qt.IsNil)
	data, err := os.ReadFile(destination)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, "new")
}

// A destination held for longer than the window is refused with the error
// Windows gave, rather than waited for without end.
func TestReplaceFile_GivesUpAfterTheWindow(t *testing.T) {
	c := qt.New(t)
	staged, destination := replacing(c)
	reader, err := os.Open(destination)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = reader.Close() })
	started := time.Now()

	err = fsdurable.ReplaceFile(staged, destination)

	c.Assert(err, qt.ErrorIs, windows.ERROR_ACCESS_DENIED)
	c.Assert(time.Since(started) >= fsdurable.ReplaceWindow, qt.IsTrue)
}

// Any other error is returned at once.
func TestReplaceFile_ReturnsOtherErrorsAtOnce(t *testing.T) {
	c := qt.New(t)
	_, destination := replacing(c)
	started := time.Now()

	err := fsdurable.ReplaceFile(filepath.Join(filepath.Dir(destination), "missing"), destination)

	c.Assert(err, qt.ErrorIs, windows.ERROR_FILE_NOT_FOUND)
	c.Assert(time.Since(started) < fsdurable.ReplaceWindow, qt.IsTrue)
}
