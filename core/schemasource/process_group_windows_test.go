//go:build windows

package schemasource_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemasource"
)

func TestRun_TimeoutKillsDescendantProcess(t *testing.T) {
	c := qt.New(t)
	startedFile := filepath.Join(t.TempDir(), "started")
	survivorFile := filepath.Join(t.TempDir(), "survived")

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env: append(
			helperEnv("orphan"),
			"SCHEMASOURCE_STARTED_FILE="+startedFile,
			"SCHEMASOURCE_SURVIVOR_FILE="+survivorFile,
		),
		Timeout: 200 * time.Millisecond,
	})
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)

	time.Sleep(time.Second)
	_, err = os.Stat(survivorFile)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestRun_SuccessfulCommandKillsDetachedDescendant(t *testing.T) {
	c := qt.New(t)
	startedFile := filepath.Join(t.TempDir(), "started")
	survivorFile := filepath.Join(t.TempDir(), "survived")

	db, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env: append(
			helperEnv("orphan-detached-sql"),
			"SCHEMASOURCE_STARTED_FILE="+startedFile,
			"SCHEMASOURCE_SURVIVOR_FILE="+survivorFile,
		),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)

	time.Sleep(time.Second)
	_, err = os.Stat(survivorFile)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
