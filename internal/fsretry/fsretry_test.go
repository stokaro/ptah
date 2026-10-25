package fsretry_test

import (
	"errors"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/fsretry"
)

var errHeld = errors.New("held by another handle")

// attempts is a replacement that fails with errs in turn, then succeeds, and
// counts how often it ran.
type attempts struct {
	errs []error
	runs int
}

func (a *attempts) replace() error {
	a.runs++
	if a.runs <= len(a.errs) {
		return a.errs[a.runs-1]
	}
	return nil
}

func held(err error) bool { return errors.Is(err, errHeld) }

// A replacement refused while another handle holds the file is retried until
// the handle lets go.
func TestReplace_RetriesWhileTheFileIsHeld(t *testing.T) {
	c := qt.New(t)
	replacement := &attempts{errs: []error{errHeld, errHeld}}

	err := fsretry.Replace(replacement.replace, held, time.Second)

	c.Assert(err, qt.IsNil)
	c.Assert(replacement.runs, qt.Equals, 3)
}

// Any other failure is reported at once.
func TestReplace_ReportsAnotherFailureAtOnce(t *testing.T) {
	c := qt.New(t)
	other := errors.New("no such directory")
	replacement := &attempts{errs: []error{other}}

	err := fsretry.Replace(replacement.replace, held, time.Second)

	c.Assert(err, qt.ErrorIs, other)
	c.Assert(replacement.runs, qt.Equals, 1)
}

// A file held past the window is reported with the refusal.
func TestReplace_GivesUpAfterTheWindow(t *testing.T) {
	c := qt.New(t)
	replacement := &attempts{errs: []error{errHeld, errHeld, errHeld, errHeld, errHeld, errHeld, errHeld, errHeld}}

	err := fsretry.Replace(replacement.replace, held, 5*time.Millisecond)

	c.Assert(err, qt.ErrorIs, errHeld)
	c.Assert(replacement.runs < len(replacement.errs)+1, qt.IsTrue)
}
