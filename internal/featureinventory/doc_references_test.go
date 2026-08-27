package featureinventory_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/featureinventory"
)

// TestDocCommandReferences_NoDocumentNamesACommandTheTreesLack is check 2.
//
// The gate is scripts/check-doc-command-references.sh.
func TestDocCommandReferences_NoDocumentNamesACommandTheTreesLack(t *testing.T) {
	c := qt.New(t)
	census := shippedCensus(c)
	references := documentedReferences(c)

	findings := featureinventory.CheckDocCommandReferences(census, references, featureinventory.DocCommandExemptions())
	c.Assert(messages(findings), qt.HasLen, 0)
}

// TestDocCommandReferences_ReadsAllThreeShapes is the control on the scan.
//
// A reading that quietly stopped producing references would leave the gate above
// comparing an empty set against the trees, and an empty set has nothing wrong
// with it. Each shape is asserted to have produced something, and one reference
// of each is named so a shape that started producing the wrong thing is a
// failure too.
func TestDocCommandReferences_ReadsAllThreeShapes(t *testing.T) {
	c := qt.New(t)
	references := documentedReferences(c)

	counts := make(map[string]int)
	for _, reference := range references {
		counts[reference.Source]++
	}

	c.Assert(counts["fenced code block"] > 400, qt.IsTrue, qt.Commentf("fenced blocks yielded %d references", counts["fenced code block"]))
	c.Assert(counts["heading"] > 20, qt.IsTrue, qt.Commentf("headings yielded %d references", counts["heading"]))
	c.Assert(counts["table row"] > 20, qt.IsTrue, qt.Commentf("table rows yielded %d references", counts["table row"]))
}

// TestDocCommandReferencesSelftest_NoticesAStaleReference plants the defect.
func TestDocCommandReferencesSelftest_NoticesAStaleReference(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "a verb the root does not have",
			body: "# Fixture\n\n```bash\nptah compare --db-url postgres://localhost/db\n```\n",
			want: "`compare` names no command of `ptah`",
		},
		{
			name: "a verb the group does not have",
			body: "# Fixture\n\n```bash\nptah schema generate --root-dir ./models\n```\n",
			want: "`generate` names no command of `ptah schema`",
		},
		{
			name: "reached through the second copy of the native binary",
			body: "# Fixture\n\n```bash\ngo run ./cmd write-db --db-url postgres://localhost/db\n```\n",
			want: "`write-db` names no command of `ptah`",
		},
		{
			name: "named in a heading",
			body: "# Fixture\n\n### `ptah-compat migrate resurrect`\n\nProse.\n",
			want: "`resurrect` names no command of `ptah-compat migrate`",
		},
		{
			name: "named in a table under a launcher heading",
			body: "# Fixture\n\n## The `ptah-compat migrate` group\n\n| Command | What |\n| --- | --- |\n| `ptah-compat migrate resurrect` | raises the dead |\n",
			want: "`resurrect` names no command of `ptah-compat migrate`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			census := fixtureCensus(c)
			references := scanFixture(c, test.body)
			findings := featureinventory.CheckDocCommandReferences(census, references, nil)
			c.Assert(messages(findings), qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Contains, test.want)
		})
	}
}

// TestDocCommandReferencesSelftest_LeavesTheShapesItMustNotRead is the other
// half, and it is the half that makes the gate usable.
//
// Every row here was a measured false positive while this scan was being built.
// A gate that reported them would be turned off within a week, and a gate that
// silenced them with a blanket allowlist would swallow the real regression along
// with them. Each is excluded by the structure of the reading instead.
func TestDocCommandReferencesSelftest_LeavesTheShapesItMustNotRead(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "a sentence saying the command does not exist",
			body: "# Fixture\n\nThere is no `ptah generate`: it answers `error: unknown command`.\n",
		},
		{
			name: "an ASCII diagram in a text block",
			body: "# Fixture\n\n```text\nptah                  !-> ptah-atlas-conformance\n```\n",
		},
		{
			name: "captured output in a text block",
			body: "# Fixture\n\n```text\nptah-compat                      SQL logic error: no such table\n```\n",
		},
		{
			name: "a trailing shell comment",
			body: "# Fixture\n\n```bash\nptah schema render   # render the desired schema\n```\n",
		},
		{
			name: "a positional argument after a leaf",
			body: "# Fixture\n\n```bash\nptah schema render ./models\n```\n",
		},
		{
			name: "a second program after a pipe",
			body: "# Fixture\n\n```bash\nptah schema render | grep CREATE\n```\n",
		},
		{
			name: "a longer launcher that is not the native binary",
			body: "# Fixture\n\n```bash\ngo run ./cmd/integration-test --scenarios=apply\n```\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			census := fixtureCensus(c)
			references := scanFixture(c, test.body)
			findings := featureinventory.CheckDocCommandReferences(census, references, nil)
			c.Assert(messages(findings), qt.HasLen, 0)
		})
	}
}

// TestDocCommandReferencesSelftest_AnExemptionForARealCommandIsItselfAFinding is
// what polices the exemption list.
//
// Without it the list is an allowlist, and an allowlist grows until it covers
// the defect the gate was built for. An entry whose command the tree registers
// reddens the gate that granted it.
func TestDocCommandReferencesSelftest_AnExemptionForARealCommandIsItselfAFinding(t *testing.T) {
	c := qt.New(t)
	census := fixtureCensus(c)

	exemptions := []featureinventory.Exemption{
		{File: "docs/fixture.md", Tree: featureinventory.TreeNative, Word: "schema", Reason: "a command that does exist"},
	}
	findings := featureinventory.CheckDocCommandReferences(census, nil, exemptions)
	c.Assert(messages(findings), qt.HasLen, 1)
	c.Assert(findings[0].Message, qt.Contains, "now registers it")
}

// TestDocCommandReferencesSelftest_TheOneExemptionStillNamesAMissingCommand runs
// that rule against the committed list rather than a fixture.
func TestDocCommandReferencesSelftest_TheOneExemptionStillNamesAMissingCommand(t *testing.T) {
	c := qt.New(t)
	census := shippedCensus(c)

	exemptions := featureinventory.DocCommandExemptions()
	c.Assert(len(exemptions) > 0, qt.IsTrue)
	findings := featureinventory.CheckDocCommandReferences(census, nil, exemptions)
	c.Assert(messages(findings), qt.HasLen, 0)

	for _, exemption := range exemptions {
		t.Run(exemption.File+" "+exemption.Word, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(exemption.Reason, qt.Not(qt.Equals), "")
		})
	}
}

// TestDocFlagReferences_NoDocumentedInvocationCarriesAFlagItsCommandLacks is
// check 3. The gate is scripts/check-doc-flag-references.sh.
func TestDocFlagReferences_NoDocumentedInvocationCarriesAFlagItsCommandLacks(t *testing.T) {
	c := qt.New(t)
	census := shippedCensus(c)
	references := documentedReferences(c)

	findings := featureinventory.CheckDocFlagReferences(census, references)
	c.Assert(messages(findings), qt.HasLen, 0)
}

// TestDocFlagReferences_ResolvesEnoughInvocationsToBeReading is the control.
//
// A flag check that resolved no invocations would inspect no flags and report
// nothing, which is what a clean run looks like.
func TestDocFlagReferences_ResolvesEnoughInvocationsToBeReading(t *testing.T) {
	c := qt.New(t)
	census := shippedCensus(c)
	references := documentedReferences(c)

	resolved, flags := 0, 0
	for _, reference := range references {
		resolved += countResolved(census, reference)
		flags += countFlags(reference)
	}
	c.Assert(resolved > 400, qt.IsTrue, qt.Commentf("%d documented invocations resolved to a command", resolved))
	c.Assert(flags > 500, qt.IsTrue, qt.Commentf("%d flag mentions were inspected", flags))
}

// TestDocFlagReferencesSelftest_NoticesAFlagThatDoesNotExist plants the defect.
func TestDocFlagReferencesSelftest_NoticesAFlagThatDoesNotExist(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "a flag no command has",
			body: "# Fixture\n\n```bash\nptah schema render --skip-chxxxx\n```\n",
			want: "`--skip-chxxxx` is not a flag of `ptah schema render`",
		},
		{
			name: "a flag with a value",
			body: "# Fixture\n\n```bash\nptah schema render --root-dirs=./models\n```\n",
			want: "`--root-dirs` is not a flag of `ptah schema render`",
		},
		{
			name: "a flag that belongs to another command",
			body: "# Fixture\n\n```bash\nptah-compat migrate apply --root-dir ./models\n```\n",
			want: "`--root-dir` is not a flag of `ptah-compat migrate apply`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			census := fixtureCensus(c)
			references := scanFixture(c, test.body)
			findings := featureinventory.CheckDocFlagReferences(census, references)
			c.Assert(messages(findings), qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Contains, test.want)
		})
	}
}

// TestDocFlagReferencesSelftest_LeavesTheFlagsItMustNotRead is the false-positive
// half.
//
// The first four rows are measured: `--changeset` and `--rollback` are Liquibase
// syntax, `--restrict-fk-on-non-standard-key` is a MySQL server option, and the
// `--migrations` at atlas/adoption.md is a deliberate control saying that flag
// does not exist. None of them needs an allowlist entry, because none of them is
// in a resolved invocation of a Ptah command -- which is also why an allowlist
// would have been the wrong instrument: it would have had to name flags, and a
// named flag stops being checked everywhere.
func TestDocFlagReferencesSelftest_LeavesTheFlagsItMustNotRead(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "a flag named in prose in order to deny it",
			body: "# Fixture\n\nThere is no `--migrations` flag in that command.\n",
		},
		{
			name: "another program's flags after a pipe",
			body: "# Fixture\n\n```bash\nptah schema render | docker run --rm -i alpine\n```\n",
		},
		{
			name: "another program entirely",
			body: "# Fixture\n\n```bash\ngo run ./cmd/integration-test --scenarios=apply --databases=postgres\n```\n",
		},
		{
			name: "liquibase directives in a sql block",
			body: "# Fixture\n\n```sql\n--liquibase formatted sql\n--changeset app:1\n--rollback DROP TABLE t;\n```\n",
		},
		{
			name: "a hidden flag, which exists and is therefore accepted",
			body: "# Fixture\n\n```bash\nptah schema render --hidden-flag x\n```\n",
		},
		{
			name: "the end-of-flags marker",
			body: "# Fixture\n\n```bash\nptah schema render -- --not-a-flag\n```\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			census := fixtureCensus(c)
			references := scanFixture(c, test.body)
			findings := featureinventory.CheckDocFlagReferences(census, references)
			c.Assert(messages(findings), qt.HasLen, 0)
		})
	}
}

// scanFixture writes one document into a throwaway directory and scans it with
// the real launchers, so a self-test drives the same reading the gate does.
func scanFixture(c *qt.C, body string) []featureinventory.Reference {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "fixture.md"), []byte(body), 0o600), qt.IsNil)

	programs, err := featureinventory.Programs(repoRoot(c))
	c.Assert(err, qt.IsNil)
	references, err := featureinventory.ScanDocument(dir, "fixture.md", featureinventory.Launchers(programs))
	c.Assert(err, qt.IsNil)
	return references
}

// countResolved and countFlags keep the conditionals out of the test bodies.

func countResolved(census *featureinventory.Census, reference featureinventory.Reference) int {
	if reference.Launcher.Tree == "" || reference.Source != "fenced code block" {
		return 0
	}
	if _, _, refused := census.Resolve(reference.Launcher.Tree, reference.Words); refused {
		return 0
	}
	return 1
}

func countFlags(reference featureinventory.Reference) int {
	count := 0
	for _, word := range reference.Words {
		if _, ok := featureinventory.FlagNameOf(word); ok {
			count++
		}
	}
	return count
}
