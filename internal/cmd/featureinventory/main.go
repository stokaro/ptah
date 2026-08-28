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
	"strings"

	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/agentsurface"
	"go.5x5.cz/ptah/internal/featureinventory"
	"go.5x5.cz/ptah/internal/quickstart"
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
	format := flag.String("format", "json", "check against `json`, or print a human table with md")
	flag.Parse()

	if err := run(options{write: *write, selftest: *selftest, listLedger: *listLedger, format: *format}); err != nil {
		fmt.Fprintln(os.Stderr, "featureinventory: "+err.Error())
		os.Exit(1)
	}
}

// options are the invocation's modes. They travel as a struct rather than as
// four parameters so the mode is read off a named field at each branch.
type options struct {
	write      bool
	selftest   bool
	listLedger bool
	format     string
}

func run(opts options) error {
	if opts.selftest {
		return runSelfTest()
	}
	if opts.listLedger {
		return runListLedger()
	}

	source, err := gather()
	if err != nil {
		return err
	}
	committed, readErr := os.ReadFile(artifactPath)
	if readErr != nil && !os.IsNotExist(readErr) {
		return readErr
	}
	source.OwnedFloor = featureinventory.CommittedFloor(committed)

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
	fmt.Printf("featureinventory: %s matches the declarations (%d rows, %d owned, floor %d)\n",
		artifactPath, len(doc.Rows), doc.Owned, doc.OwnedFloor)
	return nil
}

// writeArtifact rewrites the register and moves the coverage ratchet forward.
//
// Only the floor rule is allowed to be resolved by writing. Every other rule is
// somebody's mistake -- a claim naming nothing, two pages claiming one feature,
// a kind that stopped deriving -- and rewriting the file around it would record
// the mistake as the new truth.
func writeArtifact(doc *featureinventory.Document, problems []featureinventory.Problem) error {
	for _, problem := range problems {
		if problem.Rule == featureinventory.RuleOwnedBelowFloor {
			continue
		}
		fmt.Fprintln(os.Stderr, "featureinventory: "+problem.String())
		return fmt.Errorf("refusing to write an inventory that %d rule(s) refuse", len(problems))
	}
	if doc.Owned > doc.OwnedFloor {
		doc.OwnedFloor = doc.Owned
	}
	rendered, err := featureinventory.Render(doc)
	if err != nil {
		return err
	}
	if err := os.WriteFile(artifactPath, rendered, 0o644); err != nil { // #nosec G306 -- a committed register is world-readable by design.
		return err
	}
	fmt.Printf("featureinventory: wrote %s (%d rows, %d owned, floor %d)\n",
		artifactPath, len(doc.Rows), doc.Owned, doc.OwnedFloor)
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

// runListLedger is what scripts/check-public-api.sh reads the ledger through,
// so the recognition of a listed package exists once rather than twice.
func runListLedger() error {
	ledger, err := os.ReadFile("docs/public_api.md")
	if err != nil {
		return err
	}
	packages := featureinventory.LedgerPackages(ledger)
	if len(packages) == 0 {
		return fmt.Errorf("docs/public_api.md lists no %s packages; refusing to report a vacuous ledger", featureinventory.ModulePath)
	}
	for _, path := range packages {
		fmt.Println(path)
	}
	return nil
}

// gather reads the four declarations and the page claims.
func gather() (featureinventory.Sources, error) {
	ledger, err := os.ReadFile("docs/public_api.md")
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
		NativeLeaves: agentsurface.Walk(root.NewRootCommand()),
		Ledger:       ledger,
		Release:      release,
		Pages:        claims,
		Examples:     examples,
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
	fmt.Fprintf(&out, "# Feature inventory (%d rows, %d owned)\n\n| ID | Kind | Surface | Canonical page |\n| --- | --- | --- | --- |\n",
		len(doc.Rows), doc.Owned)
	for _, row := range doc.Rows {
		owner := "-"
		if row.Owner != nil {
			owner = *row.Owner
		}
		fmt.Fprintf(&out, "| `%s` | %s | `%s` | %s |\n", row.ID, row.Kind, row.Surface, owner)
	}
	return out.String()
}
