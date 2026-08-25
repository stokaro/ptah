package schema

// White-box testing required: the usage file reader is package-local and is not
// reachable through an exported API.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A missing flag and an empty file are different answers -- stokaro/ptah#1961.
//
// No file means the caller collected nothing, and ROL01 must report itself
// skipped. A file holding `[]` means the caller observed a window in which
// nothing was used, and every grant in it is then unused. Collapsing them would
// make the rule either permanently silent or permanently wrong, which is why
// nil and the empty slice are asserted apart rather than by length.
func TestReadRoleUsage_NoFileAndAnEmptyFileAreDifferentAnswers(t *testing.T) {
	c := qt.New(t)

	absent, err := readRoleUsage("")
	c.Assert(err, qt.IsNil)
	c.Assert(absent, qt.IsNil)

	path := filepath.Join(t.TempDir(), "usage.json")
	c.Assert(os.WriteFile(path, []byte("[]"), 0o600), qt.IsNil)

	empty, err := readRoleUsage(path)
	c.Assert(err, qt.IsNil)
	c.Assert(empty, qt.IsNotNil)
	c.Assert(empty, qt.HasLen, 0)
}

// Observations are read as written.
func TestReadRoleUsage_ReadsTheObservations(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "usage.json")
	c.Assert(os.WriteFile(path, []byte(
		`[{"role":"reporting","kind":"table","name":"orders"},`+
			`{"role":"analytics","kind":"schema","name":"public"}]`), 0o600), qt.IsNil)

	usage, err := readRoleUsage(path)

	c.Assert(err, qt.IsNil)
	c.Assert(usage, qt.HasLen, 2)
	c.Assert(usage[0].Role, qt.Equals, "reporting")
	c.Assert(usage[0].Kind, qt.Equals, "table")
	c.Assert(usage[0].Name, qt.Equals, "orders")
	c.Assert(usage[1].Name, qt.Equals, "public")
}

// A file that names no use is refused rather than read as an observation.
//
// An entry with no role or no name would silently match nothing and make every
// grant look unused, which is the direction that costs a privilege.
func TestReadRoleUsage_RefusesAnObservationThatNamesNoUse(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "no role", content: `[{"kind":"table","name":"orders"}]`},
		{name: "no name", content: `[{"role":"reporting","kind":"table"}]`},
		{name: "blank role", content: `[{"role":"  ","kind":"table","name":"orders"}]`},
		{name: "not JSON", content: `not json`},
		{name: "not a list", content: `{"role":"reporting"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), "usage.json")
			c.Assert(os.WriteFile(path, []byte(test.content), 0o600), qt.IsNil)

			_, err := readRoleUsage(path)

			c.Assert(err, qt.ErrorMatches, `read --role-usage:.*`)
		})
	}
}

// A named file that does not exist is an error, not an absent signal.
//
// Reading it as "no usage collected" would turn a typo in a CI script into a
// silently skipped rule.
func TestReadRoleUsage_AMissingFileIsAnError(t *testing.T) {
	c := qt.New(t)

	_, err := readRoleUsage(filepath.Join(t.TempDir(), "nope.json"))

	c.Assert(err, qt.ErrorMatches, `read --role-usage:.*`)
}
