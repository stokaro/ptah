//go:build integration

package providercli_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/testutils"
)

// notDescribedLine is the header line a document made from a read carries for
// the kind the fixture's engine declares and every read records.
const notDescribedLine = "// ptah:not-described fixture_stream reason=unsupported provenance=derived-from-target"

// notDescribedFixture is the CLI over a fixture database holding one table,
// items, served by the provider whose engine declares kind and whose every
// read records it as not described, in the form of FIXTURE_NOT_DESCRIBED. An
// empty kind declares none and lists no feature.
func notDescribedFixture(c *qt.C, kind string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "constraints", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_CONSTRAINT_MODE=text", "FIXTURE_NOT_DESCRIBED="+kind)
	f := newProviderCLI(c, spec)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	return f
}

// A read records the kind its engine declares, whatever the database holds,
// and a document made from the read says so: the header of `schema inspect`
// carries the line, and its JSON lists the record among what the read does not
// describe.
func TestProviderNotDescribedKindReachesTheDocument_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		format string
		want   string
	}{
		{name: "an HCL export", format: "hcl", want: `(?s).*` + notDescribedLine + `\n.*`},
		{name: "JSON", format: "json", want: `(?s).*"kind": "fixture_stream",\s+"name": "",\s+"reason": "unsupported",\s+"provenance": "derived-from-target".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := notDescribedFixture(c, "fixture_stream")

			stdout, stderr, code := f.run(c, c.Context(), "inspect", "--format", test.format)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Matches, test.want)
			c.Assert(stdout, qt.Contains, "items")
		})
	}
}

// A provider whose engine declares no kind records nothing, and the document
// names no such kind.
func TestProviderNotDescribedWithoutAKind_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := notDescribedFixture(c, "")

	stdout, stderr, code := f.run(c, c.Context(), "inspect", "--format", "hcl")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, `table "items"`)
	c.Assert(stdout, qt.Not(qt.Contains), "fixture_stream")
}

// The host reads the line back: an HCL export of the read is a schema the host
// takes, and a plan of it against the same database has nothing to change.
// A host that did not know the kind would refuse the header by name.
func TestProviderNotDescribedHeaderReadsBack_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := notDescribedFixture(c, "fixture_stream")
	exported, stderr, code := f.run(c, c.Context(), "inspect", "--format", "hcl")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(exported, qt.Contains, notDescribedLine)
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(exported), 0o600), qt.IsNil)

	stdout, stderr, code := f.run(c, c.Context(), "apply", "--schema-file", path, "--tx-mode", "none", "--dry-run")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout+stderr, qt.Contains, "Schema is synced, no changes to be made.")
}

// A drift check over an exact exported schema still has to say that the
// provider's change-stream-like kind was outside its comparison.
func TestProviderNotDescribedKindAppearsInDrift(t *testing.T) {
	c := qt.New(t)
	f := notDescribedFixture(c, "fixture_stream")
	exported := f.success(c, "inspect", "--format", "hcl")
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(exported), 0o600), qt.IsNil)

	stdout, stderr, code := f.run(c, c.Context(), "drift", "--schema-file", path, "--format", "json")
	var report struct {
		Drift       bool                      `json:"drift"`
		Failed      bool                      `json:"failed"`
		NotCompared []catalog.UnmodeledObject `json:"not_compared"`
	}
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	c.Assert(report.Drift, qt.IsFalse)
	c.Assert(report.Failed, qt.IsFalse)
	c.Assert(report.NotCompared, qt.DeepEquals, []catalog.UnmodeledObject{{
		Kind: "fixture stream", Reason: "the provider does not describe this object kind",
	}})
}

// A kind the engine declares belongs to it alone, so the host refuses one that
// is a common kind before it reads anything.
func TestProviderNotDescribedCommonKind_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := notDescribedFixture(c, "view")

	stdout, stderr, code := f.run(c, c.Context(), "inspect", "--format", "hcl")

	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", stdout))
	c.Assert(stderr, qt.Matches, `(?s).*invalid provider registration: coverage kind "view" is a common kind and cannot be an owner's.*`)
	c.Assert(stdout, qt.Not(qt.Contains), `table "items"`)
}
