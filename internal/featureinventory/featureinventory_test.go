package featureinventory_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentsurface"
	"ptah.run/internal/featureinventory"
)

// modulePath is the module the ledger fixtures below belong to. It is this
// repository's own, because the ledger fixtures are copied from its ledger; the
// parameter itself is exercised by TestLedgerPackages_ForeignModule.
const modulePath = "ptah.run"

// runnableExample is a fixture page that publishes something to run. An Example
// carrying no shells is refused, so the fixtures cannot use a bare page path.
func runnableExample(page string) featureinventory.Example {
	return featureinventory.Example{
		Page:   page,
		Shells: []featureinventory.ExampleShell{{Shell: "bash", Steps: 6, Expectations: 4}},
	}
}

// The self-test is the gate's proof that it can still fail, so it runs here as
// well as from scripts/check-feature-inventory.sh: `go test ./...` is what a
// contributor runs before the gate exists in their head.
func TestSelfTest_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(featureinventory.SelfTest(), qt.HasLen, 0)
}

// stableSection prefixes a fixture with the heading that classifies its items
// as the stable embedder API, so a case says what it is about rather than
// repeating the heading in every literal.
func stableSection(body string) string {
	return "## Stable Embedder API\n\n" + body
}

func TestParseLedger_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		ledger string
		want   []string
	}{
		{
			name:   "a list item is a listing",
			ledger: stableSection("- `ptah.run/core/renderer`\n"),
			want:   []string{"ptah.run/core/renderer"},
		},
		{
			name:   "trailing prose after the closing backtick is not part of the path",
			ledger: stableSection("- `ptah.run/dbschema` -- reads a live database\n"),
			want:   []string{"ptah.run/dbschema"},
		},
		{
			name:   "the same package listed twice is one package",
			ledger: stableSection("- `ptah.run/catalog`\n- `ptah.run/catalog`\n"),
			want:   []string{"ptah.run/catalog"},
		},
		{
			name:   "the result is sorted regardless of file order",
			ledger: stableSection("- `ptah.run/dbschema`\n- `ptah.run/catalog`\n"),
			want:   []string{"ptah.run/catalog", "ptah.run/dbschema"},
		},
		{
			name: "a shell comment inside a fenced block is not a heading",
			ledger: stableSection("- `ptah.run/catalog`\n\n" +
				"```bash\n# Stable Embedder API\n## not a heading either\n```\n\n" +
				"- `ptah.run/dbschema`\n"),
			want: []string{"ptah.run/catalog", "ptah.run/dbschema"},
		},
		{
			name: "a deeper heading explains an entry rather than reclassifying it",
			ledger: stableSection("- `ptah.run/docs`\n" +
				"\n### `ptah.run/docs`\n\nOne variable.\n\n- `ptah.run/catalog`\n"),
			want: []string{"ptah.run/catalog", "ptah.run/docs"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ledger, err := featureinventory.ParseLedger([]byte(test.ledger), modulePath)
			c.Assert(err, qt.IsNil)
			c.Assert(ledger.Stable, qt.DeepEquals, test.want)
			c.Assert(ledger.DocumentationOnly, qt.HasLen, 0)
			c.Assert(ledger.Boundary(), qt.DeepEquals, test.want)
		})
	}
}

// The two categories answer two different questions, so a package in one must
// never appear in the other. Boundary is the union because the importability
// gate asks whether a public import path may exist at all; every compatibility
// check reads Stable, and a documentation-only package reaching it would
// acquire a guarantee nobody wrote.
func TestParseLedger_Categories(t *testing.T) {
	c := qt.New(t)

	ledger, err := featureinventory.ParseLedger([]byte(
		"## Stable Embedder API\n\n- `ptah.run/catalog`\n\n"+
			"## Documentation-Only Packages\n\n- `ptah.run/examples/models`\n"), modulePath)

	c.Assert(err, qt.IsNil)
	c.Assert(ledger.Stable, qt.DeepEquals, []string{"ptah.run/catalog"})
	c.Assert(ledger.DocumentationOnly, qt.DeepEquals, []string{"ptah.run/examples/models"})
	c.Assert(ledger.Boundary(), qt.DeepEquals, []string{"ptah.run/catalog", "ptah.run/examples/models"})
}

// A documentation-only package is not a feature: it carries no guarantee, so it
// must not become a row a page can claim ownership of.
func TestDerive_DocumentationOnlyPackagesAreNotFeatureRows(t *testing.T) {
	c := qt.New(t)

	doc, problems := featureinventory.Derive(featureinventory.Sources{
		ModulePath:   modulePath,
		NativeLeaves: []agentsurface.Leaf{{Name: "schema apply"}},
		Ledger: []byte("## Stable Embedder API\n\n- `ptah.run/catalog`\n\n" +
			"## Documentation-Only Packages\n\n- `ptah.run/examples/models`\n"),
		Release:  []byte("builds:\n  - id: ptah\n    binary: ptah\n"),
		Examples: []featureinventory.Example{runnableExample("docs/site/src/content/docs/q.mdx")},
	})

	c.Assert(featureinventory.RulesOf(problems), qt.HasLen, 0)
	c.Assert(surfacesByKind(doc)[featureinventory.KindPublicPackage], qt.DeepEquals, []string{"ptah.run/catalog"})
}

// surfacesByKind groups a derived register's surfaces by the kind that
// produced them, in row order.
func surfacesByKind(doc *featureinventory.Document) map[featureinventory.Kind][]string {
	surfaces := make(map[featureinventory.Kind][]string)
	for _, row := range doc.Rows {
		surfaces[row.Kind] = append(surfaces[row.Kind], row.Surface)
	}
	return surfaces
}

// A backticked path in a paragraph is a mention, not a listing. This is the
// distinction stokaro/ptah#2246 put a gate fixture on, and the recognition now
// exists once -- every public-API gate reads the ledger through this function,
// directly or through `list-public-api-packages.sh` -- so they can no longer
// drift into different answers.
func TestParseLedger_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		ledger string
	}{
		{name: "a prose paragraph", ledger: stableSection("A paragraph mentioning `ptah.run/gateselftest` is not a listing.\n")},
		{name: "an indented list item", ledger: stableSection("  - `ptah.run/nested`\n")},
		{name: "a bullet with no backticks", ledger: stableSection("- ptah.run/bare\n")},
		{name: "a package outside this module", ledger: stableSection("- `github.com/spf13/cobra`\n")},
		{name: "the module path with nothing after it", ledger: stableSection("- `ptah.run`\n")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ledger, err := featureinventory.ParseLedger([]byte(test.ledger), modulePath)
			c.Assert(err, qt.IsNil)
			c.Assert(ledger.Boundary(), qt.HasLen, 0)
		})
	}
}

// A listing under a heading that classifies nothing is an error rather than a
// skip. Skipping would take the package off the compatibility surface and out
// of the importability boundary at once, in the permissive direction: the
// released-baseline check would stop comparing it, and the gate would stop
// requiring it to be listed, both without a word.
func TestParseLedger_UnclassifiedListing(t *testing.T) {
	tests := []struct {
		name   string
		ledger string
	}{
		{
			name:   "no heading at all",
			ledger: "- `ptah.run/core/renderer`\n",
		},
		{
			name:   "a heading that classifies nothing",
			ledger: "## Provisional Surface\n\n- `ptah.run/core/renderer`\n",
		},
		{
			name: "after a classifying section has ended",
			ledger: "## Stable Embedder API\n\n- `ptah.run/catalog`\n\n" +
				"## Compatibility Guard\n\n- `ptah.run/core/renderer`\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := featureinventory.ParseLedger([]byte(test.ledger), modulePath)
			c.Assert(err, qt.ErrorMatches,
				`ptah\.run/core/renderer is listed under no classifying heading; move it under .*`)
		})
	}
}

func TestReleaseBinaries_HappyPath(t *testing.T) {
	c := qt.New(t)

	binaries := featureinventory.ReleaseBinaries([]byte(
		"builds:\n" +
			"  - id: ptah\n    main: ./cmd/ptah\n    binary: ptah\n" +
			"  - id: ptah-ls\n    main: ./cmd/ptah-ls\n    binary: ptah-ls\n"))

	c.Assert(binaries, qt.DeepEquals, []string{"ptah", "ptah-ls"})
}

// A program is what the release configuration builds, and nothing else. A
// `go list` scan for main packages answers which programs are technically
// installable, which is not a statement that any of them is supported -- the
// inference that closed the attempt this replaces.
func TestReleaseBinaries_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		release string
	}{
		{name: "no builds block", release: "version: 2\nproject_name: ptah\n"},
		{name: "a build declaring no binary", release: "builds:\n  - id: ptah\n    main: ./cmd/ptah\n"},
		{name: "not yaml at all", release: "\tthis is not: [yaml\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(featureinventory.ReleaseBinaries([]byte(test.release)), qt.HasLen, 0)
		})
	}
}

func TestOwns_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		page string
		want []string
	}{
		{
			name: "a block sequence",
			page: "---\ntitle: Apply\nowns:\n  - cli-ptah-schema-apply\n  - cli-ptah-schema-plan\n---\n\nBody.\n",
			want: []string{"cli-ptah-schema-apply", "cli-ptah-schema-plan"},
		},
		{
			name: "a flow sequence means the same thing here as it does to the site",
			page: "---\ntitle: Apply\nowns: [cli-ptah-schema-apply]\n---\n",
			want: []string{"cli-ptah-schema-apply"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owns, err := featureinventory.Owns([]byte(test.page))
			c.Assert(err, qt.IsNil)
			c.Assert(owns, qt.DeepEquals, test.want)
		})
	}
}

func TestOwns_ClaimlessPages(t *testing.T) {
	tests := []struct {
		name string
		page string
	}{
		{name: "no frontmatter", page: "# A page\n\nBody.\n"},
		{name: "frontmatter with no owns key", page: "---\ntitle: Apply\n---\n"},
		{name: "an unterminated frontmatter block", page: "---\ntitle: Apply\nowns:\n  - cli-ptah-schema-apply\n"},
		{name: "owns written in the body rather than the frontmatter", page: "---\ntitle: Apply\n---\n\nowns:\n  - cli-ptah-schema-apply\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owns, err := featureinventory.Owns([]byte(test.page))
			c.Assert(err, qt.IsNil)
			c.Assert(owns, qt.HasLen, 0)
		})
	}
}

// Ownership is joined by string equality on a derived identifier and by nothing
// else. The check that closed the previous attempt computed identifying tokens
// from a feature and accepted a substring hit for any of them, so a page passed
// without documenting the feature it was credited with.
func TestDerive_HappyPath(t *testing.T) {
	c := qt.New(t)

	page := "docs/site/src/content/docs/direct/apply.md"
	doc, problems := featureinventory.Derive(featureinventory.Sources{
		ModulePath: modulePath,
		// `schema apply-plan` derives an identifier that CONTAINS the claimed
		// one. Under the substring join that closed the previous attempt it
		// would be credited to the same page; here it stays unclaimed, because
		// the join is equality and there is no search step.
		NativeLeaves: []agentsurface.Leaf{{Name: "schema apply"}, {Name: "schema apply-plan"}},
		Ledger:       []byte(stableSection("- `ptah.run/core/renderer`\n")),
		Release:      []byte("builds:\n  - binary: ptah\n"),
		Pages:        []featureinventory.PageClaim{{Path: page, Owns: []string{"cli-ptah-schema-apply"}}},
		Examples:     []featureinventory.Example{runnableExample("docs/site/src/content/docs/start/q.mdx")},
	})

	c.Assert(problems, qt.HasLen, 0)
	c.Assert(doc.Claimed, qt.Equals, 1)
	c.Assert(rowClaimant(c, doc, "cli-ptah-schema-apply"), qt.Equals, page)
	c.Assert(rowSurface(c, doc, "cli-ptah-schema-apply"), qt.Equals, "ptah schema apply")
	c.Assert(rowByID(c, doc, "cli-ptah-schema-apply-plan").ClaimedBy, qt.IsNil)
	c.Assert(unclaimed(doc), qt.DeepEquals, []string{"cli-ptah-schema-apply-plan", "gopkg-core-renderer"})
}

// The identifiers a page may claim are the ones the derivation produces, and a
// claim naming anything else is reported rather than ignored. Every rule has
// its own fixture in SelfTest; this is the one a reader meets first.
func TestDerive_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, problems := featureinventory.Derive(featureinventory.Sources{
		ModulePath:   modulePath,
		NativeLeaves: []agentsurface.Leaf{{Name: "schema apply"}},
		Ledger:       []byte(stableSection("- `ptah.run/core/renderer`\n")),
		Release:      []byte("builds:\n  - binary: ptah\n"),
		Pages:        []featureinventory.PageClaim{{Path: "a.md", Owns: []string{"cli-ptah-schema-aplly"}}},
		Examples:     []featureinventory.Example{runnableExample("q.mdx")},
	})

	c.Assert(featureinventory.RulesOf(problems), qt.DeepEquals, []string{featureinventory.RuleUnknownClaim})
	c.Assert(problems[0].Message, qt.Equals,
		`a.md claims "cli-ptah-schema-aplly" in its owns: frontmatter, and the derivation produces no such feature`)
}

func TestRender_HappyPath(t *testing.T) {
	c := qt.New(t)

	doc, _ := featureinventory.Derive(featureinventory.Sources{
		ModulePath:   modulePath,
		NativeLeaves: []agentsurface.Leaf{{Name: "db read"}},
		Ledger:       []byte(stableSection("- `ptah.run/core/renderer`\n")),
		Release:      []byte("builds:\n  - binary: ptah\n"),
		Examples:     []featureinventory.Example{runnableExample("q.mdx")},
	})

	first, err := featureinventory.Render(doc)
	c.Assert(err, qt.IsNil)
	second, err := featureinventory.Render(doc)
	c.Assert(err, qt.IsNil)

	c.Assert(string(second), qt.Equals, string(first))
	c.Assert(strings.HasSuffix(string(first), "\n"), qt.IsTrue)
	// An unclaimed row carries null rather than an empty string, so a reader
	// cannot mistake "nobody claims this" for "claimed at the repository root".
	c.Assert(strings.Count(string(first), `"claimed_by": null`), qt.Equals, len(doc.Rows))
}

// rowClaimant is the claiming page of one row, and fails the test when no such
// row exists rather than answering for a row that is not there.
func rowClaimant(c *qt.C, doc *featureinventory.Document, id string) string {
	row := rowByID(c, doc, id)
	c.Assert(row.ClaimedBy, qt.IsNotNil)
	return *row.ClaimedBy
}

func rowSurface(c *qt.C, doc *featureinventory.Document, id string) string {
	return rowByID(c, doc, id).Surface
}

func rowByID(c *qt.C, doc *featureinventory.Document, id string) featureinventory.Row {
	c.Helper()
	for _, row := range doc.Rows {
		if row.ID == id {
			return row
		}
	}
	c.Fatalf("no row carries the identifier %q", id)
	return featureinventory.Row{}
}

// unclaimed is the identifiers no page claims, capped so the assertion above
// reads as a list rather than as a wall.
func unclaimed(doc *featureinventory.Document) []string {
	var ids []string
	for _, row := range doc.Rows {
		if row.ClaimedBy == nil && len(ids) < 2 {
			ids = append(ids, row.ID)
		}
	}
	return ids
}

// The module path is a parameter, not a constant. A fixture that only ever used
// this repository's module path would pass whether the parameter were read or
// ignored.
func TestParseLedger_ForeignModule(t *testing.T) {
	c := qt.New(t)

	source := []byte(stableSection("- `apiguardfixture/pkg`\n- `ptah.run/core/renderer`\n"))

	foreign, err := featureinventory.ParseLedger(source, "apiguardfixture")
	c.Assert(err, qt.IsNil)
	c.Assert(foreign.Stable, qt.DeepEquals, []string{"apiguardfixture/pkg"})

	own, err := featureinventory.ParseLedger(source, modulePath)
	c.Assert(err, qt.IsNil)
	c.Assert(own.Stable, qt.DeepEquals, []string{"ptah.run/core/renderer"})
}

// An empty module path recognizes nothing rather than every backticked list
// item. The mistake has to fail closed: a pattern built from an empty prefix
// would widen the set to third-party paths and to whatever else a bullet holds,
// and a wider allowlist reports fewer undocumented packages, not an error.
func TestParseLedger_NoModulePath(t *testing.T) {
	c := qt.New(t)

	source := []byte(stableSection("- `ptah.run/core/renderer`\n- `github.com/spf13/cobra`\n"))

	ledger, err := featureinventory.ParseLedger(source, "")
	c.Assert(err, qt.IsNil)
	c.Assert(ledger.Boundary(), qt.HasLen, 0)
}

func TestModulePathOf_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		goMod string
		want  string
	}{
		{name: "the first directive", goMod: "module ptah.run\n\ngo 1.26.5\n", want: "ptah.run"},
		{name: "a tab after the keyword", goMod: "module\tptah.run\n", want: "ptah.run"},
		{name: "a quoted path", goMod: "module \"ptah.run\"\n", want: "ptah.run"},
		{name: "a trailing comment", goMod: "module ptah.run // the published path\n", want: "ptah.run"},
		{name: "a comment line first", goMod: "// a header\nmodule apiguardfixture\n", want: "apiguardfixture"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(featureinventory.ModulePathOf([]byte(test.goMod)), qt.Equals, test.want)
		})
	}
}

func TestModulePathOf_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		goMod string
	}{
		{name: "no module directive", goMod: "go 1.26.5\n\nrequire (\n\tgithub.com/spf13/cobra v1.10.2\n)\n"},
		{name: "an indented line is not a directive", goMod: "\tmodule ptah.run\n"},
		{name: "the keyword with nothing after it", goMod: "module\n"},
		{name: "a word that starts with the keyword", goMod: "modulepath ptah.run\n"},
		{name: "an empty file", goMod: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(featureinventory.ModulePathOf([]byte(test.goMod)), qt.Equals, "")
		})
	}
}

// The derivation reads the module path off this repository's own manifest, so
// the value the ledger pattern is built from moves with the module rather than
// being restated. This is the control on that: the shape above is exercised
// against fixtures, and this asserts the real file still answers.
func TestModulePathOf_ReadsThisModule(t *testing.T) {
	c := qt.New(t)

	source, err := os.ReadFile("../../go.mod")
	c.Assert(err, qt.IsNil)

	c.Assert(featureinventory.ModulePathOf(source), qt.Equals, modulePath)
}

// A page listed under runnable_examples has to run something. The marking is
// deliberate -- a page writes `quickstart: true` -- but a deliberate marking is
// still a claim, and a page of prose carrying it would otherwise be published
// as a runnable example with the gate reporting success.
func TestDerive_ExampleRunsNothing(t *testing.T) {
	tests := []struct {
		name    string
		example featureinventory.Example
		wantErr string
	}{
		{
			name:    "no shell at all",
			example: featureinventory.Example{Page: "docs/site/src/content/docs/atlas/license-boundary.md"},
			wantErr: "docs/site/src/content/docs/atlas/license-boundary.md opts in to internal/quickstart " +
				"but publishes no steps for any shell; a page listed under runnable_examples has to run something",
		},
		{
			name: "a shell whose program has no step",
			example: featureinventory.Example{
				Page:   "docs/site/src/content/docs/start/quick-start.md",
				Shells: []featureinventory.ExampleShell{{Shell: "bash", Expectations: 4}},
			},
			wantErr: "docs/site/src/content/docs/start/quick-start.md publishes a bash program with no steps; " +
				"a page listed under runnable_examples has to run something",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, problems := featureinventory.Derive(featureinventory.Sources{
				ModulePath:   modulePath,
				NativeLeaves: []agentsurface.Leaf{{Name: "db read"}},
				Ledger:       []byte(stableSection("- `ptah.run/core/renderer`\n")),
				Release:      []byte("builds:\n  - binary: ptah\n"),
				Examples:     []featureinventory.Example{test.example},
			})

			c.Assert(featureinventory.RulesOf(problems), qt.DeepEquals,
				[]string{featureinventory.RuleExampleRunsNothing})
			c.Assert(problems[0].Message, qt.Equals, test.wantErr)
		})
	}
}

// The coverage floor is a source constant, so `--write` cannot move it and an
// edited line of the artifact cannot lower it. A floor of zero would gate
// nothing while reporting the same success, which is the shape every other
// floor in this package exists to refuse.
func TestClaimedFloor_IsAnAnchor(t *testing.T) {
	c := qt.New(t)

	c.Assert(featureinventory.ClaimedFloor > 0, qt.IsTrue)
}
