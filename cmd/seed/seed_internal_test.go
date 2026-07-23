package seed

// White-box testing required: runSeed validates pre-connection seed options
// that are not fully observable through NewSeedCommand without attempting a
// database connection.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

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
