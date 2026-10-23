//go:build !windows

package providerbundle_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/providerbundle"
	"ptah.run/internal/providercatalog"
)

// A staged executable keeps its execute bit. Windows has no such bit: Go reports
// 0o666 or 0o444 there, so this assertion lives behind a build tag instead of
// reducing to a tautology on that platform.
func TestStage_ExecutableBitIsKept(t *testing.T) {
	c := qt.New(t)
	fixture := newRelease(linux)
	staged, err := providerbundle.Stage(c.Context(), providerbundle.Options{
		Out: c.TempDir(), BaseURL: serve(c, fixture.archive), Pins: fixture.lookup,
		Platforms: []providercatalog.Platform{linux},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(staged, qt.HasLen, 1)
	info := must.Must(os.Stat(staged[0].Executable))
	c.Assert(info.Mode().Perm()&0o111, qt.Not(qt.Equals), os.FileMode(0))
}
