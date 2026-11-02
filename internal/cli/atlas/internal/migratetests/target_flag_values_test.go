package migratetests_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// TestCompatCommand_ATargetFlagTakesTheValueAfterIt pins that a flag only the
// native target declares takes the word after it as its value on a verb that
// reads Atlas positionals, as it does when written with "=". The checkpoint is
// named by the positional and its dialect is the one --dialect names, rather
// than "sqlite" naming the checkpoint and --dialect taking the directory flag
// the verb forwards.
func TestCompatCommand_ATargetFlagTakesTheValueAfterIt(t *testing.T) {
	c := qt.New(t)
	migrationsDir, devURL := checkpointFlagFixture(c)

	_, _, err := atlastest.RunCompatStreams(c, "migrate", "checkpoint", "--dialect", "sqlite", "named",
		"--dir", "file://"+migrationsDir, "--dev-url", devURL)

	c.Assert(err, qt.IsNil)
	written, err := filepath.Glob(filepath.Join(migrationsDir, "*_named.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
}

// TestCompatCommand_ATargetFlagReadsAlikeWithAndWithoutEquals runs each verb
// that reads Atlas positionals with a target-only flag written both ways and
// requires one outcome: a value written after the flag is the flag's, never a
// positional the verb refuses.
func TestCompatCommand_ATargetFlagReadsAlikeWithAndWithoutEquals(t *testing.T) {
	tests := []struct {
		name    string
		spaced  []string
		inlined []string
	}{
		{name: "migrate test", spaced: []string{"migrate", "test", "--db-url", "x", "--dir", "file://migrations"},
			inlined: []string{"migrate", "test", "--db-url=x", "--dir", "file://migrations"}},
		{name: "schema test", spaced: []string{"schema", "test", "--db-url", "x"},
			inlined: []string{"schema", "test", "--db-url=x"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Chdir(c.TempDir())

			_, _, spacedErr := atlastest.RunCompatStreams(c, test.spaced...)
			_, _, inlinedErr := atlastest.RunCompatStreams(c, test.inlined...)

			c.Assert(atlastest.ErrMessageOrEmpty(spacedErr), qt.Equals, atlastest.ErrMessageOrEmpty(inlinedErr))
			c.Assert(atlastest.ErrMessageOrEmpty(spacedErr), qt.Not(qt.Matches), `.*unexpected positional arguments.*`)
		})
	}
}
