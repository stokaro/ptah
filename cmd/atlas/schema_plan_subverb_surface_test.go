package atlas_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/atlas"
)

const atlasV13SchemaPlanHelpDir = "testdata/atlas-v1.3.0-schema-plan-help"

type atlasV13SchemaPlanHelpProvenance struct {
	AtlasVersion    string            `json:"atlas_version"`
	AtlasEdition    string            `json:"atlas_edition"`
	Platform        string            `json:"platform"`
	CapturedOn      string            `json:"captured_on"`
	ReleaseURL      string            `json:"release_url"`
	BinarySHA256    string            `json:"binary_sha256"`
	Environment     string            `json:"environment"`
	Captures        map[string]string `json:"captures"`
	ArtifactSHA256  map[string]string `json:"artifact_sha256"`
	RuntimeArtifact *string           `json:"runtime_artifact"`
	Limitations     []string          `json:"limitations"`
}

func readAtlasV13SchemaPlanHelpProvenance(c *qt.C) atlasV13SchemaPlanHelpProvenance {
	c.Helper()
	document, err := os.ReadFile(filepath.Join(atlasV13SchemaPlanHelpDir, "provenance.json"))
	c.Assert(err, qt.IsNil)
	var provenance atlasV13SchemaPlanHelpProvenance
	c.Assert(json.Unmarshal(document, &provenance), qt.IsNil)
	return provenance
}

func fileSHA256(c *qt.C, path string) string {
	c.Helper()
	document, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return fmt.Sprintf("%x", sha256.Sum256(document))
}

// Evidence for everything this file pins.
//
// Atlas CE v1.3.0, the pinned oracle binary, has NO usable `schema plan`
// sub-verbs. Reconfirmed 2026-08-02 in a scratch directory:
//
//	atlas schema plan new                  -> Abort: 'atlas schema plan' is not supported...  exit 1
//	atlas schema plan lint                 -> Abort: 'atlas schema plan' ...                  exit 1
//	atlas schema plan validate             -> Abort: 'atlas schema plan' ...                  exit 1
//	atlas schema plan test                 -> Abort: 'atlas schema plan' ...                  exit 1
//	atlas schema plan frobnicate-nonsense  -> Abort: 'atlas schema plan' ...                  exit 1   NONSENSE CONTROL
//
// The abort names `atlas schema plan`, never the token after it, and the
// nonsense control is byte-identical to the four real sub-verb names, so CE
// does not know any of them: the positional is swallowed by the community
// gate. The control that shows an unknown command CAN look different one level
// up is `atlas schema frobnicate`, which prints the `schema` group help at
// exit 0.
//
// CE also registers zero own flags on this path — flag parsing runs before the
// gate, so the set is measurable even on a gated verb:
//
//	atlas schema plan new --file x                -> Error: unknown flag: --file            exit 1
//	atlas schema plan lint -f x                   -> Error: unknown shorthand flag: 'f'     exit 1
//	atlas schema plan test --run x                -> Error: unknown flag: --run             exit 1
//	atlas schema plan validate --from a --to b    -> Error: unknown flag: --from            exit 1
//	atlas schema plan new --frobnicate-nonsense x -> Error: unknown flag: --frobnicate...   exit 1   NONSENSE CONTROL
//
// So none of this is a CE parity gap: there is no input on this path where CE
// succeeds and this tree fails. The flag sets asserted below were captured
// from the standard Atlas v1.3.0 binary's own help on 2026-08-02 and agree with
// the published Atlas CLI reference. The sub-verbs' runtime behavior is not
// established; successful Ptah executions therefore remain silent and keep
// that provenance in source and compatibility documentation.

func TestAtlasSchemaPlanV13HelpOracleProvenance(t *testing.T) {
	c := qt.New(t)
	provenance := readAtlasV13SchemaPlanHelpProvenance(c)

	c.Assert(provenance.AtlasVersion, qt.Equals, "v1.3.0")
	c.Assert(provenance.AtlasEdition, qt.Equals, "standard")
	c.Assert(provenance.Platform, qt.Equals, "darwin/arm64")
	c.Assert(provenance.CapturedOn, qt.Equals, "2026-08-02")
	c.Assert(provenance.ReleaseURL, qt.Equals, "https://github.com/ariga/atlas/releases/tag/v1.3.0")
	c.Assert(provenance.BinarySHA256, qt.Equals, "47aaf7c295c7569c7eecfcbc53f02de862846ce1fbef16f1bd8ae98b03c3c68f")
	c.Assert(provenance.Environment, qt.Equals,
		"empty temporary HOME; no external identifiers of any kind")
	c.Assert(provenance.Captures, qt.DeepEquals, map[string]string{
		"new.txt":      "HOME=<empty-temp-home> atlas schema plan new --help",
		"validate.txt": "HOME=<empty-temp-home> atlas schema plan validate --help",
	})
	c.Assert(provenance.RuntimeArtifact, qt.IsNil)
	c.Assert(provenance.Limitations, qt.HasLen, 2)

	tests := []struct {
		name string
		hash string
	}{
		{name: "new.txt", hash: "4ec146e7e8660cc85a137547c5c02ee7a818e621ed39d34f156a708b2b1b663f"},
		{name: "validate.txt", hash: "d636db61bea0187ff29dd79ea608b18162cb768719f1081d51433fd32eab9103"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provenance.ArtifactSHA256[test.name], qt.Equals, test.hash)
			c.Assert(fileSHA256(c, filepath.Join(atlasV13SchemaPlanHelpDir, test.name)), qt.Equals, test.hash)
		})
	}
}

// atlasSchemaPlanNewFlags is the local flag set Atlas registers on
// `atlas schema plan new`, verbatim from the published reference minus the
// global -c/--config, --env and --var, which the compat tree registers on the
// `schema` group, and minus --help, which Cobra adds.
var atlasSchemaPlanNewFlags = []string{
	"auto-approve",
	"dev-url",
	"edit",
	"exclude",
	"format",
	"from",
	"include",
	"lock-timeout",
	"name",
	"name-format",
	"output",
	"repo",
	"schema",
	"to",
}

// atlasSchemaPlanValidateFlags is the same for `atlas schema plan validate`.
var atlasSchemaPlanValidateFlags = []string{
	"auto-approve",
	"dev-url",
	"exclude",
	"file",
	"format",
	"from",
	"include",
	"lock-timeout",
	"repo",
	"schema",
	"to",
}

// atlasSchemaPlanLintFlags is the same for `atlas schema plan lint`, whose
// published entry registers the identical set: the shared transition flags plus
// -f/--file, and no --url.
//
// It is spelled out rather than aliased to atlasSchemaPlanValidateFlags. The
// two sets being equal today is a fact about the published reference, not a
// rule, and an alias would make a future divergence in one of them silently
// rewrite the assertion for the other.
var atlasSchemaPlanLintFlags = []string{
	"auto-approve",
	"dev-url",
	"exclude",
	"file",
	"format",
	"from",
	"include",
	"lock-timeout",
	"repo",
	"schema",
	"to",
}

// TestAtlasSchemaPlanVerbFlagSetsMatchAtlas pins each implemented sub-verb's
// registered flag set against the captured Atlas surface.
//
// This is the anti-drift gate the shared registration helper cannot provide by
// itself: sharing registration keeps the sub-verbs equal to each other, and
// only an external list keeps them equal to Atlas. In particular it fails if a
// sub-verb ever acquires a flag Atlas does not register there — `new` gaining
// --save or --dry-run is the exact mistake the delegation to the parent's run
// function makes easy.
func TestAtlasSchemaPlanVerbFlagSetsMatchAtlas(t *testing.T) {
	tests := []struct {
		name string
		path []string
		want []string
	}{
		{name: "new", path: []string{"schema", "plan", "new"}, want: atlasSchemaPlanNewFlags},
		{name: "validate", path: []string{"schema", "plan", "validate"}, want: atlasSchemaPlanValidateFlags},
		{name: "lint", path: []string{"schema", "plan", "lint"}, want: atlasSchemaPlanLintFlags},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := findAtlasCommand(c, test.path)

			c.Assert(localFlagNames(cmd), qt.DeepEquals, test.want)
		})
	}
}

// TestAtlasSchemaPlanVerbShorthandsMatchAtlas pins the two shorthands the
// capture shows on these sub-verbs. A long name can match while the shorthand
// silently does not, and `-f` is the one an Atlas pipeline actually types.
func TestAtlasSchemaPlanVerbShorthandsMatchAtlas(t *testing.T) {
	tests := []struct {
		name      string
		path      []string
		flag      string
		shorthand string
	}{
		{name: "new_output", path: []string{"schema", "plan", "new"}, flag: "output", shorthand: "o"},
		{name: "new_schema", path: []string{"schema", "plan", "new"}, flag: "schema", shorthand: "s"},
		{name: "validate_file", path: []string{"schema", "plan", "validate"}, flag: "file", shorthand: "f"},
		{name: "validate_schema", path: []string{"schema", "plan", "validate"}, flag: "schema", shorthand: "s"},
		{name: "lint_file", path: []string{"schema", "plan", "lint"}, flag: "file", shorthand: "f"},
		{name: "lint_schema", path: []string{"schema", "plan", "lint"}, flag: "schema", shorthand: "s"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := findAtlasCommand(c, test.path)

			flag := cmd.Flags().Lookup(test.flag)

			c.Assert(flag, qt.IsNotNil)
			c.Assert(flag.Shorthand, qt.Equals, test.shorthand)
		})
	}
}

// TestAtlasSchemaPlanNewRejectsParentOnlyFlags proves the negative half of the
// flag-set claim through the CLI rather than through reflection: the flags
// Atlas registers on `schema plan` but NOT on `schema plan new` must be
// unknown there. Without this, forcing --save on inside `new` could just as
// well have been done by registering --save and defaulting it to true, which
// would accept `--save=false` and quietly turn the verb into a no-op.
func TestAtlasSchemaPlanNewRejectsParentOnlyFlags(t *testing.T) {
	for _, flag := range []string{"--save", "--dry-run", "--push", "--pending", "--skip-lint", "--directive"} {
		t.Run(flag, func(t *testing.T) {
			c := qt.New(t)
			dir := chdirToScratchC(c)
			fixture := newPlanFixture(c, "parentonly",
				`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
				"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")

			out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
				append(fixture.args(), flag, "x")...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "unknown flag: "+flag)
			assertNoPlanFileWritten(c, dir)
		})
	}
}

// TestAtlasSchemaPlanRegistrySubverbsStayStubs pins which sub-verbs did NOT
// move. `test` is local by its Atlas flag set, so its presence among the stubs
// is a deliberate deferral, not an oversight; the reason is on
// unsupportedCommandTests.
func TestAtlasSchemaPlanRegistrySubverbsStayStubs(t *testing.T) {
	c := qt.New(t)
	root := atlas.NewCompatCommand("atlas")

	plan := findAtlasCommand(c, []string{"schema", "plan"})
	var names []string
	for _, child := range plan.Commands() {
		names = append(names, child.Name())
	}
	slices.Sort(names)

	c.Assert(names, qt.DeepEquals, []string{
		"approve", "lint", "list", "new", "pull", "push", "rm", "test", "validate",
	})
	c.Assert(root, qt.IsNotNil)
}

// runSchemaPlanSubverb executes `schema plan <verb>` with args and returns the
// merged output plus the execution error, matching runSchemaPlan's contract
// one level down.
func runSchemaPlanSubverb(root *cobra.Command, verb string, args ...string) (string, error) {
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append([]string{"schema", "plan", verb}, args...))
	err := root.Execute()
	return out.String(), err
}

// runSchemaPlanSubverbStreams is runSchemaPlanSubverb with the streams kept
// apart so tests can pin stdout and stderr independently.
func runSchemaPlanSubverbStreams(
	root *cobra.Command,
	verb string,
	args ...string,
) (stdout, stderr string, err error) {
	var out, errBuf bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errBuf)
	root.SetArgs(append([]string{"schema", "plan", verb}, args...))
	err = root.Execute()
	return out.String(), errBuf.String(), err
}

// chdirToScratchC is chdirToScratch for a quicktest subtest context.
func chdirToScratchC(c *qt.C) string {
	c.Helper()
	dir := c.TB.TempDir()
	c.TB.Chdir(dir)
	return dir
}

// findAtlasCommand walks the compat tree to the command named by path.
func findAtlasCommand(c *qt.C, path []string) *cobra.Command {
	c.Helper()
	cmd, _, err := atlas.NewCompatCommand("atlas").Find(path)
	c.Assert(err, qt.IsNil)
	c.Assert(cmd.Name(), qt.Equals, path[len(path)-1])
	return cmd
}

// localFlagNames returns the command's own sorted flag names, excluding the
// Cobra-generated --help and the inherited project flags.
func localFlagNames(cmd *cobra.Command) []string {
	var names []string
	cmd.LocalNonPersistentFlags().VisitAll(func(flag *pflag.Flag) {
		if flag.Name == "help" {
			return
		}
		names = append(names, flag.Name)
	})
	slices.Sort(names)
	return names
}
