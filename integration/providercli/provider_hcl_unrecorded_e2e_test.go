//go:build integration

package providercli_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// warningLines are the lines of a run's error stream that are warnings.
func warningLines(stderr string) []string {
	warnings := make([]string, 0)
	for line := range strings.SplitSeq(stderr, "\n") {
		if strings.HasPrefix(line, "warning: ") {
			warnings = append(warnings, line)
		}
	}
	return warnings
}

// An HCL export of a provider read reports a record of an object the read
// lists and does not describe, of a model no block of the provider writes:
// the document's captured header carries only the knowledge of the models its
// blocks write, so the record has no place in it, and the run warns with the
// object, its model and why, as it warns of a built-in owner's record HCL
// cannot carry. The document says nothing of the object. A read that records
// no such object is the control and warns of nothing. Without the provider's
// models marked as ones HCL records no limit of (hclext.Extension.Unrecorded),
// the record leaves the export without a word.
func TestProviderInspectHCL_ReportsARecordItCannotCarry_HappyPath(t *testing.T) {
	// The object is named by its database, which the path quotes again.
	const object, quoted = `example\.org/fixture/legacy \[database=".+database\.db"; namespace=\[\]\] audit`,
		`example\.org/fixture/legacy \[database=\\".+database\.db\\"; namespace=\[\]\] audit`
	for _, test := range []struct {
		name        string
		environment []string
		warnings    []string
	}{
		{name: "a read that records a legacy object", environment: []string{"FIXTURE_READ_UNREAD=1"},
			warnings: []string{`warning: features\["` + quoted + `"\]: feature object ` + object + ` of kind example\.org/fixture/legacy is not described ` +
				`\(a legacy object, which the fixture does not read\), and HCL cannot record that`}},
		{name: "a read that records none", warnings: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := readExportProvider(c, test.environment...)

			stdout, stderr, code := executeProviderCLI(c.Context(), f.arguments("inspect", "--format", "hcl"))

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Contains, "table \"records\" {")
			c.Assert(stdout, qt.Not(qt.Contains), "audit")
			c.Assert(stdout, qt.Not(qt.Contains), "example.org/fixture/legacy")
			warnings := warningLines(stderr)
			c.Assert(warnings, qt.HasLen, len(test.warnings), qt.Commentf("%s", stderr))
			for i, want := range test.warnings {
				c.Assert(warnings[i], qt.Matches, want)
			}
		})
	}
}

// A provider's model is one HCL records no limit of where no block of the
// provider writes it. A model a block of its own writes is not, and neither is
// one its block writes at a table's platform group: the captured header keeps
// each subject of it the read left incomplete.
func TestProviderHCLUnrecordedModels_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, command string
		environment   []string
		kind          schemaext.Kind
		unrecorded    bool
	}{
		{name: "a model no block writes", command: "hcl-claims", environment: []string{"FIXTURE_READ_UNREAD=1"}, kind: "example.org/fixture/legacy", unrecorded: true},
		{name: "a model of a platform group", command: "hcl-platform", kind: "example.org/fixture/retention", unrecorded: false},
		{name: "a model a block writes", command: "hcl-claims", kind: "example.org/fixture/retention", unrecorded: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, runtime := yamlProviderEnvironment(c, test.command, test.environment)

			c.Assert(runtime.HCL().Unrecorded(test.kind), qt.Equals, test.unrecorded)
		})
	}
}
