package migrationrollout_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationrollout"
	"ptah.run/internal/sourcecontent"
)

func TestStateFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, canaryAndWavesDefinition, nil)
	state := newState(plan)
	path := filepath.Join(c.TempDir(), "rollout-state.json")

	c.Assert(migrationrollout.CreateState(path, state), qt.IsNil)
	state.Stages[0].Targets[0].Status = migrationrollout.TargetSucceeded
	c.Assert(migrationrollout.WriteState(path, state), qt.IsNil)
	read, err := migrationrollout.ReadState(path)

	c.Assert(err, qt.IsNil)
	c.Assert(read, qt.DeepEquals, state)
	c.Assert(read.Bind(plan, directoryRelease), qt.IsNil)
	entries, err := os.ReadDir(filepath.Dir(path))
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1, qt.Commentf("the replacement leaves no temporary file behind"))
}

func TestStateFile_FailurePath(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "unknown field", content: `{"contract_version": 1, "verdict": "ok"}`, wantErr: `read rollout state .*: json: unknown field "verdict"`},
		{name: "another contract version", content: `{"contract_version": 2}`, wantErr: `read rollout state .*: contract version 2 is not 1, the one this build writes`},
		{name: "two documents", content: `{"contract_version": 1} {}`, wantErr: `read rollout state .*: the file holds more than one document`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "state.json")
			c.Assert(os.WriteFile(path, []byte(tc.content), 0o600), qt.IsNil)

			state, err := migrationrollout.ReadState(path)

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(state, qt.IsNil)
		})
	}
}

// A new run never writes over the record of another.
func TestCreateState_RefusesAnExistingFile(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "state.json")
	c.Assert(os.WriteFile(path, []byte("{}"), 0o600), qt.IsNil)

	err := migrationrollout.CreateState(path, newState(mustPlan(c, canaryAndWavesDefinition, nil)))

	c.Assert(err, qt.ErrorIs, os.ErrExist)
	content, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Equals, "{}")
}

// A second invocation on one state refuses while the first holds the lock, and
// may take it once the first releases.
func TestLockState(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "state.json")

	unlock, err := migrationrollout.LockState(path)
	c.Assert(err, qt.IsNil)
	second, secondErr := migrationrollout.LockState(path)
	c.Assert(secondErr, qt.ErrorMatches, `another rollout holds .*state\.json\.lock \(pid \d+ on .* since .*\); if no rollout is running on this state, remove the file`)
	c.Assert(second, qt.IsNil)

	c.Assert(unlock(), qt.IsNil)
	c.Assert(unlock(), qt.IsNil)
	again, err := migrationrollout.LockState(path)
	c.Assert(err, qt.IsNil)
	c.Assert(again(), qt.IsNil)
}

func TestBind_FailurePath(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, canaryAndWavesDefinition, nil)
	state := newState(plan)
	changedPlan := mustPlan(c, replaceOnce(c, canaryAndWavesDefinition, "concurrency: 2", "concurrency: 3"), nil)
	changedContent := directoryRelease
	changedContent.Digest = "sha256:0000000000000000000000000000000000000000000000000000000000000002"
	anotherKind := migrationrollout.Release{
		Kind:      migrationrollout.ReleaseOCI,
		Source:    "oci://registry.example/app:release",
		Digest:    directoryRelease.Digest,
		Reference: "oci://registry.example/app@" + directoryRelease.Digest,
	}

	c.Assert(state.Bind(changedPlan, directoryRelease), qt.ErrorIs, migrationrollout.ErrPlanChanged)
	c.Assert(state.Bind(plan, changedContent), qt.ErrorIs, migrationrollout.ErrReleaseChanged)
	c.Assert(state.Bind(plan, changedContent), qt.ErrorMatches,
		`the migration release is not the one this rollout is bound to: the run is bound to sha256:0+1, `+
			`and migrations now holds sha256:0+2; start a new rollout with a new state file to roll out the new content`)
	c.Assert(state.Bind(plan, anotherKind), qt.ErrorIs, migrationrollout.ErrReleaseChanged)
}

// A tag that names the same digest is the same release.
func TestBind_SameDigestUnderAnotherSpelling(t *testing.T) {
	c := qt.New(t)
	recorded := migrationrollout.Release{
		Kind:      migrationrollout.ReleaseOCI,
		Source:    "oci://registry.example/app:release",
		Digest:    "sha256:abc",
		Reference: "oci://registry.example/app@sha256:abc",
	}
	pinned := recorded
	pinned.Source = "oci://registry.example/app@sha256:abc"

	c.Assert(pinned.Matches(recorded), qt.IsNil)
}

// The local release digest is the content digest Ptah Operator binds a plan
// to, over every file the captured directory holds.
func TestDirectoryDigest(t *testing.T) {
	c := qt.New(t)
	files := fstest.MapFS{
		"1_init.up.sql":   {Data: []byte("CREATE TABLE a (id INT);\n")},
		"1_init.down.sql": {Data: []byte("DROP TABLE a;\n")},
		"ptah.sum":        {Data: []byte("h1:abc\n")},
	}
	want, err := sourcecontent.Digest([]sourcecontent.File{
		{Path: "1_init.down.sql", Data: []byte("DROP TABLE a;\n")},
		{Path: "1_init.up.sql", Data: []byte("CREATE TABLE a (id INT);\n")},
		{Path: "ptah.sum", Data: []byte("h1:abc\n")},
	})
	c.Assert(err, qt.IsNil)

	got, err := migrationrollout.DirectoryDigest(files)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, want)
	files["2_more.up.sql"] = &fstest.MapFile{Data: []byte("SELECT 1;\n")}
	changed, err := migrationrollout.DirectoryDigest(files)
	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.Not(qt.Equals), want)
}
