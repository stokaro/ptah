package dbcapabilities_test

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/cmd/dbcapabilities"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/serverprofile"
)

// sqliteURL is a database URL for a file this test owns. SQLite is the one
// engine the command can be driven against without a server, and it is a real
// connection rather than a stub: the version string asserted below is what the
// compiled-in engine reports about itself.
func sqliteURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TB.(*testing.T).TempDir(), "capabilities.db")
}

func runCapabilities(c *qt.C, args ...string) (stdout, stderr string, err error) {
	c.Helper()

	cmd := dbcapabilities.NewCapabilitiesCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)

	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func TestCapabilitiesCommand_TextOutput_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c))

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Dialect:")
	c.Assert(stdout, qt.Contains, "sqlite")
	c.Assert(stdout, qt.Contains, "Capability preset:")
	c.Assert(stdout, qt.Contains, "SQLite3")
	c.Assert(stdout, qt.Contains, "Support level:")
	c.Assert(stdout, qt.Contains, string(capability.Certified))
	c.Assert(stdout, qt.Contains, "Behavior:")
	c.Assert(stdout, qt.Contains, "identifier_limit")
	c.Assert(stdout, qt.Contains, "foreign_key_reference")
	c.Assert(stdout, qt.Contains, "Capabilities:")
}

// TestCapabilitiesCommand_TextOutputNamesEveryCapability is the non-vacuity
// guard on the block above. "Capabilities:" appearing proves a heading was
// printed, not that anything was printed under it, and a renderer that emitted
// the heading and no rows would satisfy every assertion in the happy path.
func TestCapabilitiesCommand_TextOutputNamesEveryCapability(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c))

	c.Assert(err, qt.IsNil)
	for _, key := range capability.All() {
		c.Assert(stdout, qt.Contains, string(key))
	}
	// Absent keys are the ones an operator is diagnosing with, so the words
	// for both answers have to reach the output.
	c.Assert(stdout, qt.Contains, "supported")
	c.Assert(stdout, qt.Contains, "unsupported")
}

func TestCapabilitiesCommand_JSONOutput_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c), "--format", "json")

	c.Assert(err, qt.IsNil)

	var profile serverprofile.Profile
	c.Assert(json.Unmarshal([]byte(stdout), &profile), qt.IsNil)
	c.Assert(profile.Dialect, qt.Equals, "sqlite")
	c.Assert(profile.Preset.Name, qt.Equals, "SQLite3")
	c.Assert(profile.Certification.Level, qt.Equals, capability.Certified)
	c.Assert(profile.Certification.Line, qt.Equals, "3")
	c.Assert(profile.Certification.Reason, qt.Not(qt.Equals), "")
	c.Assert(profile.Server.Version, qt.Not(qt.Equals), "")
	c.Assert(profile.Capabilities, qt.HasLen, len(capability.All()))
	c.Assert(profile.Traits.ForeignKeyReference, qt.Equals, capability.ReferenceUnique)
	c.Assert(profile.Traits.EnumModeling, qt.Equals, capability.EnumUnsupported)
	// SQLite is the dialect Ptah models no identifier limit for, so this
	// round-trips the zero value rather than a number somebody chose.
	c.Assert(profile.Traits.Identifiers.Unlimited(), qt.IsTrue)
}

// TestCapabilitiesCommand_JSONOutputIsStable pins the property that makes the
// machine-readable form usable at all. capability.All ranges over a map and
// documents that its order is unspecified, so an unsorted rendering produced a
// different document on every invocation and any tool diffing it reported a
// change every time.
func TestCapabilitiesCommand_JSONOutputIsStable(t *testing.T) {
	c := qt.New(t)

	url := sqliteURL(c)
	first, _, err := runCapabilities(c, "--db-url", url, "--format", "json")
	c.Assert(err, qt.IsNil)
	second, _, err := runCapabilities(c, "--db-url", url, "--format", "json")
	c.Assert(err, qt.IsNil)

	c.Assert(second, qt.Equals, first)
}

// TestCapabilitiesCommand_PayloadAndNarrationAreSeparated keeps the JSON form
// pipeable. A progress line on stdout would be inside the document.
func TestCapabilitiesCommand_PayloadAndNarrationAreSeparated(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c), "--format", "json")

	c.Assert(err, qt.IsNil)
	c.Assert(json.Valid([]byte(stdout)), qt.IsTrue)
}

func TestCapabilitiesCommand_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing database url",
			args:    []string{},
			wantErr: "database URL is required",
		},
		{
			name:    "unknown format",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--format", "yaml"},
			wantErr: `invalid --format value "yaml": expected text or json`,
		},
		{
			name:    "empty format",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--format", ""},
			wantErr: `invalid --format value "": expected text or json`,
		},
		{
			name:    "unparsable connect timeout",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--connect-timeout", "soon"},
			wantErr: ".*soon.*",
		},
		{
			name:    "unknown flag",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--bogus-flag"},
			wantErr: "unknown flag: --bogus-flag",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := runCapabilities(c, test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestCapabilitiesCommand_ValidatesBeforeConnecting matters because the
// alternative reads as a database problem. A bad --format resolved after the
// dial would report a connection failure for a target the operator never
// meant to reach, and the address below is unreachable on purpose.
func TestCapabilitiesCommand_ValidatesBeforeConnecting(t *testing.T) {
	c := qt.New(t)

	_, _, err := runCapabilities(c,
		"--db-url", "postgres://ptah:ptah@127.0.0.1:1/ptah?sslmode=disable",
		"--format", "yaml")

	c.Assert(err, qt.ErrorMatches, `invalid --format value "yaml": expected text or json`)
}
