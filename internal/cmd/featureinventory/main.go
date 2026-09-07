// Command featureinventory derives Ptah's feature register from the product's
// own declarations, and checks the committed artifact against that derivation.
//
// It walks the command tree in process and reads the ledger, the release
// configuration and the quick-start opt-ins as data, so every value in
// docs/feature-inventory.json is a measurement of this tree rather than
// something an author typed and a gate had to guess at (stokaro/ptah#2402).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ptah.run/cmd/root"
	"ptah.run/internal/agentsurface"
	"ptah.run/internal/featureinventory"
	"ptah.run/internal/quickstart"
)

// artifactPath is the committed register. It is `.json` deliberately: docs.go
// embeds `*.md`, `adr/*.md` and the site content, so a Markdown file here would
// ship inside every binary and answer `search_docs`, which is what happened to
// the attempt this replaces. docs/docs_test.go asserts the absence directly.
const artifactPath = "docs/feature-inventory.json"

func main() {
	write := flag.Bool("write", false, "rewrite "+artifactPath+" from the declarations")
	selftest := flag.Bool("selftest", false, "break each rule against fixtures and require the derivation to notice")
	listLedger := flag.Bool("list-ledger", false, "print the stable-embedder ledger's package list, one per line")
	listBoundary := flag.Bool("list-boundary", false, "print every classified package -- stable plus documentation-only -- one per line")
	moduleRoot := flag.String("root", ".", "the module `directory` to read go.mod and docs/public_api.md from")
	format := flag.String("format", "json", "check against `json`, or print a human table with md")
	flag.Parse()

	if err := run(options{
		write: *write, selftest: *selftest, listLedger: *listLedger, listBoundary: *listBoundary,
		root: *moduleRoot, format: *format,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "featureinventory: "+err.Error())
		os.Exit(1)
	}
}

// options are the invocation's modes. They travel as a struct rather than as
// five parameters so the mode is read off a named field at each branch.
type options struct {
	write        bool
	selftest     bool
	listLedger   bool
	listBoundary bool
	root         string
	format       string
}

func run(opts options) error {
	if opts.selftest {
		return runSelfTest()
	}
	if opts.listBoundary {
		return runListPackages(opts.root, ledgerBoundary)
	}
	if opts.listLedger {
		return runListPackages(opts.root, ledgerStable)
	}

	source, err := gather()
	if err != nil {
		return err
	}
	committed, readErr := os.ReadFile(artifactPath)
	if readErr != nil && !os.IsNotExist(readErr) {
		return readErr
	}

	doc, problems := featureinventory.Derive(source)
	if opts.format == "md" {
		fmt.Print(markdown(doc))
		return nil
	}
	if opts.write {
		return writeArtifact(doc, problems)
	}

	rendered, renderErr := featureinventory.Render(doc)
	if renderErr != nil {
		return renderErr
	}
	problems = featureinventory.Compare(committed, rendered, problems)
	if len(problems) > 0 {
		for _, problem := range problems {
			fmt.Fprintln(os.Stderr, "featureinventory: "+problem.String())
		}
		return fmt.Errorf("%d rule(s) refused the tree", len(problems))
	}
	fmt.Printf("featureinventory: %s matches the declarations (%d rows, %d claimed, floor %d)\n",
		artifactPath, len(doc.Rows), doc.Claimed, doc.ClaimedFloor)
	return nil
}

// writeArtifact rewrites the register.
//
// No rule is resolved by writing, the coverage floor included. Every rule here
// is somebody's mistake -- a claim naming nothing, two pages claiming one
// feature, a marked page that runs nothing, a kind that stopped deriving,
// coverage that fell -- and rewriting the file around any of them would record
// the mistake as the new truth. The floor used to be the exception: `--write`
// read it out of the artifact and wrote a new one back, so a lowered floor and
// a false claim both survived a regeneration and the gate reported success.
// featureinventory.ClaimedFloor is a source constant now, and raising it is a
// reviewed edit rather than a side effect of this command.
func writeArtifact(doc *featureinventory.Document, problems []featureinventory.Problem) error {
	for _, problem := range problems {
		fmt.Fprintln(os.Stderr, "featureinventory: "+problem.String())
	}
	if len(problems) > 0 {
		return fmt.Errorf("refusing to write an inventory that %d rule(s) refuse", len(problems))
	}
	rendered, err := featureinventory.Render(doc)
	if err != nil {
		return err
	}
	if err := os.WriteFile(artifactPath, rendered, 0o644); err != nil { // #nosec G306 -- a committed register is world-readable by design.
		return err
	}
	fmt.Printf("featureinventory: wrote %s (%d rows, %d claimed, floor %d)\n",
		artifactPath, len(doc.Rows), doc.Claimed, doc.ClaimedFloor)
	return nil
}

func runSelfTest() error {
	failures := featureinventory.SelfTest()
	for _, failure := range failures {
		fmt.Fprintln(os.Stderr, "featureinventory: "+failure)
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d self-test case(s) did not notice their own broken rule", len(failures))
	}
	fmt.Println("featureinventory: OK (every rule fired on its own broken fixture, and none fired on the clean one)")
	return nil
}

// ledgerSelection is which of the ledger's two categories a listing prints.
type ledgerSelection int

const (
	// ledgerStable is the compatibility surface: the released-baseline check,
	// the exported-doc-comment check and the site's stable-packages table all
	// ask this question and must not see a documentation-only package.
	ledgerStable ledgerSelection = iota
	// ledgerBoundary is every classified package. Only the importability gate
	// asks this one: whether a public import path is allowed to exist at all
	// is a different question from whether it carries a guarantee.
	ledgerBoundary
)

// runListPackages is how every gate that needs a ledger package set reads it,
// so the recognition of a listed package exists once rather than once per shell
// script. scripts/check-public-api.sh calls this directly;
// scripts/list-public-api-packages.sh forwards here for the docs-sync,
// exported-docs, and released-baseline checks.
//
// The module directory is a parameter so callers and unit fixtures can read a
// ledger relative to its own go.mod rather than assuming this repository.
//
// An unclassified listing is fatal for both selections. It is a package the
// document names and neither category claims, so printing the rest would answer
// a narrower question than the caller asked while looking like a complete
// answer.
func runListPackages(moduleRoot string, selection ledgerSelection) error {
	modulePath, err := readModulePath(moduleRoot)
	if err != nil {
		return err
	}
	source, err := os.ReadFile(filepath.Join(moduleRoot, ledgerPath)) // #nosec G304 -- the caller names its own module directory.
	if err != nil {
		return err
	}
	ledger, err := featureinventory.ParseLedger(source, modulePath)
	if err != nil {
		return fmt.Errorf("%s: %w", ledgerPath, err)
	}

	packages := ledger.Stable
	if selection == ledgerBoundary {
		packages = ledger.Boundary()
	}
	// The floor is on the stable set for both selections. A boundary listing
	// is allowed to consist of stable packages alone, but a ledger whose
	// stable category came up empty is the vacuous answer this refuses -- and
	// it would report every embedder package as undocumented rather than as
	// missing.
	if len(ledger.Stable) == 0 {
		return fmt.Errorf("%s lists no %s packages; refusing to report a vacuous ledger", ledgerPath, modulePath)
	}
	for _, path := range packages {
		fmt.Println(path)
	}
	return nil
}

// ledgerPath is the stable-embedder ledger, relative to a module directory.
const ledgerPath = "docs/public_api.md"

// readModulePath reads the module path out of a directory's go.mod.
//
// Derived rather than written down: featureinventory.LedgerPackages builds its
// pattern from this value, so a literal that stopped matching would produce an
// empty ledger rather than an error naming the cause.
func readModulePath(moduleRoot string) (string, error) {
	source, err := os.ReadFile(filepath.Join(moduleRoot, "go.mod")) // #nosec G304 -- the caller names its own module directory.
	if err != nil {
		return "", err
	}
	modulePath := featureinventory.ModulePathOf(source)
	if modulePath == "" {
		return "", fmt.Errorf("%s declares no module path", filepath.Join(moduleRoot, "go.mod"))
	}
	return modulePath, nil
}

// gather reads the four declarations and the page claims.
func gather() (featureinventory.Sources, error) {
	modulePath, err := readModulePath(".")
	if err != nil {
		return featureinventory.Sources{}, err
	}
	ledger, err := os.ReadFile(ledgerPath)
	if err != nil {
		return featureinventory.Sources{}, err
	}
	release, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		return featureinventory.Sources{}, err
	}
	claims, err := featureinventory.ReadClaims(".")
	if err != nil {
		return featureinventory.Sources{}, err
	}
	examples, err := discoverExamples()
	if err != nil {
		return featureinventory.Sources{}, err
	}
	return featureinventory.Sources{
		ModulePath:   modulePath,
		NativeLeaves: agentsurface.Walk(root.NewRootCommand()),
		Ledger:       ledger,
		Release:      release,
		Pages:        claims,
		Examples:     examples,
		// The floor is the source constant, never a number read back out of
		// the artifact this run is about to check.
		ClaimedFloor: featureinventory.ClaimedFloor,
	}, nil
}

// discoverExamples reports the pages whose commands continuous integration
// runs, from internal/quickstart's own opt-in.
//
// The marking is that package's `quickstart: true` frontmatter key and not a
// second one of ours: a page opts in deliberately, .github/workflows/
// quickstart-acceptance.yml executes the steps on Linux and Windows and
// compares each one's real output with what the page publishes. Inventing a
// second marking would create two lists of runnable pages that can disagree,
// and the older one is the one that actually runs.
func discoverExamples() ([]featureinventory.Example, error) {
	pages, err := quickstart.Discover(featureinventory.DocumentationRoot)
	if err != nil {
		return nil, err
	}
	examples := make([]featureinventory.Example, 0, len(pages))
	for _, page := range pages {
		example := featureinventory.Example{Page: page.Path}
		for _, shell := range page.ShellsPresent() {
			program, _ := page.Program(shell)
			example.Shells = append(example.Shells, featureinventory.ExampleShell{
				Shell:        string(shell),
				Steps:        program.Steps(),
				Expectations: program.Expectations(),
			})
		}
		examples = append(examples, example)
	}
	return examples, nil
}

// markdown is the human view, printed on demand and never committed. A second
// committed copy would be a second thing to keep in step, and nobody reads a
// register of this size as prose.
func markdown(doc *featureinventory.Document) string {
	var out strings.Builder
	fmt.Fprintf(&out, "# Feature inventory (%d rows, %d claimed)\n\n| ID | Kind | Surface | Claimed by |\n| --- | --- | --- | --- |\n",
		len(doc.Rows), doc.Claimed)
	for _, row := range doc.Rows {
		claimedBy := "-"
		if row.ClaimedBy != nil {
			claimedBy = *row.ClaimedBy
		}
		fmt.Fprintf(&out, "| `%s` | %s | `%s` | %s |\n", row.ID, row.Kind, row.Surface, claimedBy)
	}
	return out.String()
}
