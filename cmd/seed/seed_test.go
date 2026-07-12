package seed

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestNewSeedCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := NewSeedCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "seed")
	c.Assert(cmd.Short, qt.Contains, "seed")
}

func TestRunSeedRequiresDatabaseURL(t *testing.T) {
	c := qt.New(t)

	err := runSeed(t.Context(), runOptions{env: "test", seedsDir: "seeds"})

	c.Assert(err, qt.ErrorMatches, `database URL is required`)
}

func TestRunSeedValidatesProtectedEnvBeforeConnecting(t *testing.T) {
	c := qt.New(t)

	err := runSeed(t.Context(), runOptions{
		dbURL:    "postgres://localhost/db",
		seedsDir: "seeds",
		env:      "prod",
	})

	c.Assert(err, qt.ErrorMatches, `refusing to seed protected environment "prod" without --allow-prod`)
}
