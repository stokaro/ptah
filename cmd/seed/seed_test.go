package seed_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/seed"
)

func TestNewSeedCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := seed.NewSeedCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "seed")
	c.Assert(cmd.Short, qt.Contains, "seed")
}
