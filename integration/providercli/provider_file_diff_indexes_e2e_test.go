//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
	"ptah.run/internal/testutils"
)

// recordsWithIndex declares records with index records_value on value, of the
// type given; the column-semantics fixture prepares an index of no type as
// kind "global".
func recordsWithIndex(indexType string) string {
	return "tables:\n  records:\n    columns:\n      id: {type: BIGINT, primary: true, not_null: true}\n      value: {type: BIGINT}\n" +
		"    indexes:\n      records_value: {fields: [value], type: \"" + indexType + "\"}\n"
}

// fileDiff runs `schema diff` of two documents through the fixture program
// started with environment and selected for engine fixture under aliases,
// offline, with the fixture database as the dev database that names the
// engine, and returns what it wrote and its exit code.
func fileDiff(c *qt.C, program string, aliases, environment []string, extension, from, to string) (string, int) {
	c.Helper()
	spec := fixtureReleaseSpec(c, program, testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, environment...)
	f := newProviderCLI(c, spec)
	changeManifest(c, f, func(config *providerconfig.Configuration) { config.Providers[0].Engines[0].Aliases = aliases })
	dir := filepath.Dir(f.schemaPath)
	fromPath, toPath := filepath.Join(dir, "from"+extension), filepath.Join(dir, "to"+extension)
	c.Assert(os.WriteFile(fromPath, []byte(from), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(to), 0o600), qt.IsNil)
	out, diagnostics, code := executeProviderCLI(c.Context(),
		[]string{"schema", "diff", "--from", fromPath, "--to", toPath, "--dev-url", f.address, "--provider-config", f.manifestPath})
	return out + diagnostics, code
}

// columnAliases names the engine of the column-semantics fixture as the
// program declares it.
var columnAliases = []string{"fixture-columns"}

// textMode has the constraints fixture write its plans as SQL text.
var textMode = []string{"FIXTURE_CONSTRAINT_MODE=text"}

// A document compared with itself through a provider plans nothing. The
// comparison reads each declared index of the --to side as the provider's
// table preparation states it, and the --from side, a document converted to
// stand for a database, is described through the same preparation: its kind
// (column-semantics prepares an index of no type as "global"), the NULLS
// order of each key (the constraints fixture states NULLS FIRST on an
// ascending key as SQLite's default, empty) and its predicate (spelled
// `(score > '0')`). Described as the source wrote it, every such index was
// dropped and added again.
func TestProviderFileDiffOfADocumentWithItself_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, program        string
		aliases, environment []string
		extension, document  string
	}{
		{"index kind", "column-semantics", columnAliases, nil, ".yaml", recordsWithIndex("")},
		{"NULLS order", "constraints", nil, textMode, ".hcl", declaredScoreIndex("nulls_first = true")},
		{"predicate", "constraints", nil, textMode, ".yaml", declaredPositiveScores("score > 0")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			out, code := fileDiff(c, test.program, test.aliases, test.environment, test.extension, test.document, test.document)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced, no changes to be made.")
		})
	}
}

// A change of an index's kind or predicate between the two documents is still
// a change: both sides are prepared the same way, so the comparison finds
// what the provider states differently. The column-semantics fixture plans no
// index change, and the host refuses the replacement by name; the constraints
// fixture rebuilds the index with the new predicate in its spelling.
func TestProviderFileDiffOfAChangedIndex(t *testing.T) {
	for _, test := range []struct {
		name, program        string
		aliases, environment []string
		extension, from, to  string
		code                 int
		want                 string
	}{
		{"index kind", "column-semantics", columnAliases, nil, ".yaml", recordsWithIndex(""), recordsWithIndex("async"),
			2, "the experimental provider contract does not support SchemaDiff.IndexesAdded"},
		{"predicate", "constraints", nil, textMode, ".yaml", declaredPositiveScores("score > 0"), declaredPositiveScores("score > 1"),
			0, "DROP INDEX \"items_positive\";\nCREATE INDEX \"items_positive\" ON \"items\" (\"score\") WHERE (score > '1');\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			out, code := fileDiff(c, test.program, test.aliases, test.environment, test.extension, test.from, test.to)

			c.Assert(code, qt.Equals, test.code, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.want)
		})
	}
}
