package dbcli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/migration/migrator"
)

const operationID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCheckRunAttribution_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		actor     string
		origin    string
		reference string
	}{
		{name: "nothing"},
		{name: "a reference alone", reference: operationID},
		{name: "an actor alone", actor: "alice@example.com"},
		{name: "an actor and its origin", actor: "alice@example.com", origin: "admission", reference: operationID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := dbcli.CheckRunAttribution(test.actor, test.origin, test.reference)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestCheckRunAttribution_FailurePath(t *testing.T) {
	t.Run("a reference the log would not store as given", func(t *testing.T) {
		c := qt.New(t)

		err := dbcli.CheckRunAttribution("", "", "run 7")

		c.Assert(err, qt.ErrorIs, migrator.ErrInvalidRunAttribution)
		c.Assert(err, qt.ErrorMatches, `--run-reference: invalid run attribution: .*`)
	})
	t.Run("an origin that is not one word", func(t *testing.T) {
		c := qt.New(t)

		err := dbcli.CheckRunAttribution("alice@example.com", "Admission", "")

		c.Assert(err, qt.ErrorIs, migrator.ErrInvalidRunAttribution)
		c.Assert(err, qt.ErrorMatches, `--actor-origin: invalid run attribution: .*`)
	})
	t.Run("an origin with no actor to describe", func(t *testing.T) {
		c := qt.New(t)

		err := dbcli.CheckRunAttribution("  ", "admission", "")

		c.Assert(err, qt.ErrorMatches, `--actor-origin says where the --actor name came from, and no --actor was given`)
	})
}
