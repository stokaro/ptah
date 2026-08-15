//go:build integration

package render_test

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// atlasFeatureRefusal is the substring that separates the two refusals this
// front had to keep apart. "postgres: sequences are not supported by this
// version" is the binary declining to model a construct, which no spelling of
// ours can lift and which the compatibility surface therefore omits. `There is
// no variable named "bigint"` was Ptah's own rendering, fixed in #1255 rather
// than suppressed -- so a row that starts failing for THAT reason must not keep
// counting as a feature gap.
const atlasFeatureRefusal = "are not supported by this version"

// atlasToleratedBlockTypes are top-level block types Ptah's inspect renders
// that the pinned binary does NOT refuse, and which the compatibility surface
// therefore keeps.
//
// They are the control in the opposite direction. Without them this run could
// pass with a list that suppressed everything, or with one grown by guesswork:
// each name here is asserted to be readable, so a block type that STARTS being
// refused turns the job red and says "this now belongs in the list" instead of
// shipping unreadable output.
//
// `wibble` is the sharpest of them. It is not a construct at all, and the
// binary accepts it exactly as it accepts `role` or `view` -- which is what
// makes exit 0 on those names evidence of tolerance rather than of support.
var atlasToleratedBlockTypes = map[string][]string{
	platform.Postgres: {
		"role",
		"function",
		"view",
		"materialized",
		"trigger",
		"permission",
		"wibble",
	},
	platform.SQLite: {
		// The three PostgreSQL refuses. They are listed here rather than in
		// atlasRefusedBlockTypes because the refusal is the PostgreSQL driver's
		// and not the file format's, which is why that map is keyed by dialect
		// at all. If SQLite ever starts refusing them, this run says so.
		"extension",
		"sequence",
		"policy",
		"wibble",
	},
}

// TestOracleAtlasRefusedBlockTypesMatchTheBinary re-measures every entry of
// atlasRefusedBlockTypes against the pinned community binary.
//
// A list nothing re-measures rots silently, and this one rots in a direction
// that costs the user: a block type a later build starts modeling would go on
// being withheld from the compatibility output forever, describing less of the
// database than the reader could have used. So each entry is asserted in the
// direction that would make the suppression stale -- the binary must still
// refuse it -- and the refusal must still be the FEATURE refusal, because a row
// that began failing over one of Ptah's own spellings is not this list's
// business.
//
// Every probe starts from a base asserted to exit 0 first. An earlier attempt
// at this measurement built an invalid base, and every row inherited its error;
// the base subtest is what makes that impossible to repeat.
//
// The probes deliberately do NOT run in parallel. Each dialect's probes share
// one dev database and the binary materializes the file there, so concurrent
// probes collide inside the server with errors that read like verdicts about
// the block under test.
func TestOracleAtlasRefusedBlockTypesMatchTheBinary(t *testing.T) {
	oracle := requireTypeOracle(t)

	c := qt.New(t)
	dialects := atlashclrender.AtlasRefusedBlockDialects()
	c.Assert(dialects, qt.DeepEquals, []string{platform.Postgres},
		qt.Commentf("every measured refusal dialect must remain in the oracle matrix"))
	c.Assert(
		atlashclrender.AtlasRefusedBlockTypes(platform.Postgres),
		qt.DeepEquals,
		[]string{"extension", "policy", "sequence"},
		qt.Commentf("every measured PostgreSQL refusal must remain in the oracle matrix"),
	)

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			devURL := requireDevURL(t, dialect)

			refused := atlashclrender.AtlasRefusedBlockTypes(dialect)
			c.Assert(refused, qt.Not(qt.HasLen), 0,
				qt.Commentf("a dialect in the map with no entries would measure nothing"))

			t.Run("base", func(t *testing.T) {
				c := qt.New(t)

				out, code := runBlockOracle(c, oracle, devURL, dialect, "")
				c.Assert(code, qt.Equals, 0,
					qt.Commentf("the base this run adds one block to is not accepted, so no row below means anything: %s", out))
			})

			for _, block := range refused {
				t.Run("refused/"+block, func(t *testing.T) {
					c := qt.New(t)

					out, code := runBlockOracle(c, oracle, devURL, dialect, block)
					c.Assert(code, qt.Not(qt.Equals), 0,
						qt.Commentf("the binary now reads a %s block on %s; suppressing it withholds a construct the reader could use: %s",
							block, dialect, out))
					c.Assert(out, qt.Contains, atlasFeatureRefusal,
						qt.Commentf("the %s block on %s is refused for a reason that is not a feature gap, so it is not this list's to suppress: %s",
							block, dialect, out))
				})
			}
		})
	}
}

// TestOracleToleratesTheBlockTypesPtahStillRenders measures the other
// direction: everything the compatibility surface still emits stays readable.
//
// This is what keeps the omission surgical. The binary drops a top-level block
// whose name it does not model and carries on, so these blocks cost the reader
// nothing, and omitting them would cost a description for no compatibility
// gain. If one of them starts being refused, this run goes red rather than
// ptah-compat quietly emitting a file its counterpart cannot read.
func TestOracleToleratesTheBlockTypesPtahStillRenders(t *testing.T) {
	oracle := requireTypeOracle(t)

	c := qt.New(t)
	c.Assert(atlasToleratedBlockTypes, qt.DeepEquals, map[string][]string{
		platform.Postgres: {
			"role", "function", "view", "materialized", "trigger", "permission", "wibble",
		},
		platform.SQLite: {"extension", "sequence", "policy", "wibble"},
	}, qt.Commentf("every tolerance control must remain in the oracle matrix"))

	for _, dialect := range slices.Sorted(maps.Keys(atlasToleratedBlockTypes)) {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			devURL := requireDevURL(t, dialect)

			tolerated := atlasToleratedBlockTypes[dialect]
			c.Assert(tolerated, qt.Not(qt.HasLen), 0,
				qt.Commentf("a dialect with no controls controls nothing"))

			for _, block := range tolerated {
				t.Run("tolerated/"+block, func(t *testing.T) {
					c := qt.New(t)

					c.Assert(atlashclrender.AtlasRefusesBlock(dialect, block), qt.IsFalse,
						qt.Commentf("%q is a control: it must stay out of the suppression list for that list to have a boundary", block))

					out, code := runBlockOracle(c, oracle, devURL, dialect, block)
					c.Assert(code, qt.Equals, 0,
						qt.Commentf("the binary now refuses a %s block on %s, so ptah-compat is emitting a file it cannot read: %s",
							block, dialect, out))
				})
			}
		})
	}
}

// runBlockOracle asks the pinned binary to read a file holding one extra
// top-level block, and returns its combined output and exit status.
//
// The block is written bare -- a label and an empty body -- on purpose. A block
// carrying the attributes Ptah writes would be refused over one of them first,
// which is a different verdict about a different defect; stripping it to the
// label is what leaves a refusal of the block TYPE and nothing else.
//
// The column type is a sql() wrap because that is the one spelling
// TestOracleAcceptsTheWrapItFallsBackTo already measures as readable on every
// dialect here, so the base cannot fail for a reason this run is not about.
func runBlockOracle(c *qt.C, oracle, devURL, dialect, block string) (string, int) {
	c.Helper()

	schema := schemaNameByDialect[dialect]
	c.Assert(schema, qt.Not(qt.Equals), "",
		qt.Commentf("dialect %q has no schema name; add one before adding the dialect", dialect))

	probe, ok := blockProbeSources[block]
	c.Assert(ok, qt.IsTrue,
		qt.Commentf("block %q has no probe spelling; add one before measuring it", block))

	source := fmt.Sprintf(`schema %q {
}
table "probe" {
  schema = schema.%s
  column "c" {
    type = sql("integer")
  }
}
%s`, schema, schema, probe)

	path := filepath.Join(c.TempDir(), "blocks.hcl")
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

	// #nosec -- operator-provided oracle path, and path is a test temp dir
	cmd := exec.Command(oracle, "schema", "inspect", "-u", "file://"+path, "--dev-url", devURL)
	// The error is the exit status, which is the measurement; a process that
	// never started leaves ProcessState nil and fails the assertion instead.
	out, _ := cmd.CombinedOutput() //nolint:errcheck // exit status is read from ProcessState below
	c.Assert(cmd.ProcessState, qt.IsNotNil, qt.Commentf("the oracle did not run: %s", out))
	return string(out), cmd.ProcessState.ExitCode()
}

// blockProbeSources spells each probed block the way Ptah's inspect renders it.
// The empty key is the base probe, which adds nothing to the base.
//
// The spellings are written out rather than composed from the block name so
// that adding a block type to atlasRefusedBlockTypes or to
// atlasToleratedBlockTypes without deciding how it is spelled fails the run
// instead of measuring a form Ptah never emits. `permission` is why that
// matters: it is the one block Ptah renders with no label at all.
var blockProbeSources = map[string]string{
	"":             "",
	"extension":    "extension \"probe_block\" {\n}\n",
	"function":     "function \"probe_block\" {\n}\n",
	"materialized": "materialized \"probe_block\" {\n}\n",
	"permission":   "permission {\n}\n",
	"policy":       "policy \"probe_block\" {\n}\n",
	"role":         "role \"probe_block\" {\n}\n",
	"sequence":     "sequence \"probe_block\" {\n}\n",
	"trigger":      "trigger \"probe_block\" {\n}\n",
	"view":         "view \"probe_block\" {\n}\n",
	"wibble":       "wibble \"probe_block\" {\n}\n",
}
