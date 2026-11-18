//go:build integration

package migrator_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// repeatableLogAttempt is what the log says of one attempt, read back from the
// server, without the times and the run identifier.
type repeatableLogAttempt struct {
	Version    int64
	Repeatable string
	Checksum   string
	Outcome    migrator.MigrationLogState
	Failed     bool
}

// repeatableDigest is the digest the record and the log keep for body,
// computed here from the bytes rather than taken from the code under test.
func repeatableDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "h1:" + base64.StdEncoding.EncodeToString(sum[:])
}

// log reads the operation log of the target.
func (r *repeatableEngineTarget) log(c *qt.C) []repeatableLogAttempt {
	c.Helper()
	attempts, err := r.migrator(c, repeatableEngineMigrations(nil)).MigrationLog(context.Background(), 0)
	c.Assert(err, qt.IsNil)
	read := make([]repeatableLogAttempt, 0, len(attempts))
	for _, attempt := range attempts {
		read = append(read, repeatableLogAttempt{
			Version:    attempt.Start.Version,
			Repeatable: attempt.Start.Repeatable,
			Checksum:   attempt.Start.Checksum,
			Outcome:    attempt.Outcome.State,
			Failed:     attempt.Outcome.Error != "",
		})
	}
	return read
}

// The operation log keeps every attempt of a repeatable migration under its
// identity, the failed one with its error and the retry that applied, beside
// the versioned migration the run applied first.
func TestRepeatableMigrationLogOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__partial.sql": repeatableEnginesFailing})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.ErrorAs, new(*migrator.RepeatableMigrationError))
			writeRepeatable(fsys, "R__partial.sql", engine.fixed)
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

			read := r.log(c)

			c.Assert(read, qt.HasLen, 3)
			c.Assert(read[:2], qt.DeepEquals, []repeatableLogAttempt{
				{Repeatable: "partial", Checksum: repeatableDigest(engine.fixed), Outcome: migrator.MigrationLogApplied},
				{
					Repeatable: "partial", Checksum: repeatableDigest(repeatableEnginesFailing),
					Outcome: migrator.MigrationLogFailed, Failed: true,
				},
			})
			c.Assert(read[2].Version, qt.Equals, int64(1))
			c.Assert(read[2].Repeatable, qt.Equals, "")
			c.Assert(read[2].Outcome, qt.Equals, migrator.MigrationLogApplied)
		})
	}
}
