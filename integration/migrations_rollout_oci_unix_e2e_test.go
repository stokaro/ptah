//go:build integration && unix

package integration_test

// Unix only: the pre-migration hook that moves the tag during the rollout is a
// POSIX shell script.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/migrationrollout"
)

// tagMover is a pre-migration hook that holds a run open for a second and, the
// first time it runs, pushes a different release to the tag the rollout was
// started from.
const tagMover = `set -eu
marker="$1.moved"
if [ ! -e "$marker" ]; then
  "$2" migrations push "$3" --migrations-dir "$4" --version "v$(date +%s)-moved" --plain-http
  : > "$marker"
fi
sleep 1
`

// TestMigrationsRolloutBindsAnOCITagToItsDigestE2E holds a rollout started from
// a movable tag to the digest the tag named when it started.
//
// The canary's pre-migration hook pushes a three-migration release to the same
// tag while the rollout is running. Every target, the canary included, must end
// with the two migrations of the release the run bound, which it can only do if
// each target pulled the digest rather than the tag. A resume naming the tag
// is then refused, because the tag now names other content; a resume naming the
// recorded digest is the same release and runs nothing.
func TestMigrationsRolloutBindsAnOCITagToItsDigestE2E(t *testing.T) {
	registry := requiredOCIRegistry(t)
	c := qt.New(t)
	ws := newRolloutWorkspace(c)
	binary := clirun.Build(c, clirun.Ptah)
	reference := fmt.Sprintf("oci://%s/ptah/rollout-tag-%d:release", registry, time.Now().UnixNano())

	pushed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: ws.dir},
		"migrations", "push", reference, "--migrations-dir", ws.migrations, "--version", "v1", "--plain-http")
	c.Assert(pushed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", pushed.Stderr))

	moved := filepath.Join(ws.dir, "moved-release")
	c.Assert(os.CopyFS(moved, os.DirFS(ws.migrations)), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(moved, "0000000003_late.up.sql"),
		[]byte("CREATE TABLE late (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(moved, "0000000003_late.down.sql"), []byte("DROP TABLE late;\n"), 0o600), qt.IsNil)
	hook := filepath.Join(ws.dir, "move-tag.sh")
	c.Assert(os.WriteFile(hook, []byte(tagMover), 0o600), qt.IsNil)

	ws.writeDefinition(c, ws.targets("canary", "t1", "t2")+`stages:
  - {name: canary, canary: true, targets: [canary]}
  - {name: wave, depends_on: [canary], targets: [t1, t2], concurrency: 2}
`)
	rollout := func(c *qt.C, env []string, verb, release string, extra ...string) clirun.Result {
		c.Helper()
		args := append([]string{
			"migrations", "rollout", verb,
			"--definition", ws.definition,
			"--migrations-dir", release,
			"--state-file", ws.state,
			"--plain-http",
		}, extra...)
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: ws.dir, Env: append(os.Environ(), env...)}, args...)
	}

	first := rollout(c, []string{fmt.Sprintf("PTAH_PRE_UP_HOOK=sh '%s' '%s' '%s' '%s' '%s'",
		hook, filepath.Join(ws.dir, "tag"), binary, reference, moved)}, "run", reference)

	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	_, err := os.Stat(filepath.Join(ws.dir, "tag.moved"))
	c.Assert(err, qt.IsNil, qt.Commentf("the hook did not move the tag during the rollout"))
	state := ws.readState(c)
	c.Assert(state.Release.Kind, qt.Equals, migrationrollout.ReleaseOCI)
	c.Assert(state.Release.Source, qt.Equals, reference)
	c.Assert(state.Release.Reference, qt.Equals,
		strings.TrimSuffix(reference, ":release")+"@"+state.Release.Digest)
	for _, name := range []string{"canary", "t1", "t2"} {
		c.Assert(applicationTables(c, ws.database(name)), qt.DeepEquals, []string{"accounts", "ledgers"}, qt.Commentf("target %s", name))
	}

	byTag := rollout(c, nil, "resume", reference)

	c.Assert(byTag.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", byTag.Stdout, byTag.Stderr))
	c.Assert(byTag.Stderr, qt.Contains, "the migration release is not the one this rollout is bound to")
	c.Assert(ranTargets(byTag.Stderr), qt.HasLen, 0)

	byDigest := rollout(c, nil, "resume", state.Release.Reference)

	c.Assert(byDigest.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", byDigest.Stdout, byDigest.Stderr))
	c.Assert(ranTargets(byDigest.Stderr), qt.HasLen, 0)
	c.Assert(ws.readState(c).Invocations, qt.Equals, 2)
}
