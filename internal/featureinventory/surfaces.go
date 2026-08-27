package featureinventory

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// FormatList is one enumerated set of format names a flag accepts, together
// with the declaration it was read out of.
//
// The names are the ones docs/feature-inventory.md publishes in its format-set
// mapping, so a row's `format:<list>/<value>` token and this census agree on
// which list is which. That document names fifteen sets; five of them are
// declared as a list in code and are the five read here. The other ten are
// per-command flag usages with no enumerating declaration to read, and a census
// that scraped them out of a usage string would agree with a usage string that
// had drifted from the switch beside it -- which is exactly the defect
// stokaro/ptah#2065 repaired.
//
// `import-from` is the one set with a declaration that cannot be read as the
// answer, and it is worth naming rather than quietly omitting. The six `Format`
// constants in internal/atlasmigrateimport are a SUPERSET of what
// `ptah migrations import --from` accepts: `atlas` parses, and then every import
// path refuses it with "cannot import a migration directory already in
// \"atlas\" format". Reading the constants reports a sixth accepted value that
// does not exist, and reading the flag's usage would be reading help. The repair
// is an enumerating slice beside the constants, which belongs in its own change;
// until then this census does not claim to cover that set.
type FormatList struct {
	// Name is the list identifier the inventory spells, as
	// `format:<Name>/<value>`.
	Name string
	// Source is the declaration the values came from, so a finding names a
	// place to look rather than a value.
	Source string
	Values []string
}

// Surfaces is everything the inventory has to account for that is not a command
// path.
type Surfaces struct {
	// Packages are the Go import paths the public-API ledger lists.
	Packages []string
	// Programs are every `main` package in the repository.
	Programs []Program
	// Formats are the enumerated format sets, sorted by name.
	Formats []FormatList
}

// formatSources declares where each enumerated format set is read from.
//
// Every entry names a package directory and a declaration inside it. Nothing
// here restates a value: a set that moved fails to resolve and reddens the gate,
// and a value added to a declaration appears in this census without an edit.
// That is the whole difference between this and the four-of-seven help string
// stokaro/ptah#2065 repaired -- the help was a copy, and a copy is what goes
// stale.
var formatSources = []struct {
	name    string
	pkgDir  string
	read    func(*goPackage) ([]string, error)
	sources string
}{
	{
		name:    "schema-file-ext",
		pkgDir:  "internal/schemaload",
		read:    func(p *goPackage) ([]string, error) { return p.SliceVar("supportedExtensions") },
		sources: "internal/schemaload supportedExtensions",
	},
	{
		name:    "export-to",
		pkgDir:  "cmd/schema",
		read:    func(p *goPackage) ([]string, error) { return p.SliceVar("exportTargetFormats") },
		sources: "cmd/schema exportTargetFormats",
	},
	{
		name:   "viz-format",
		pkgDir: "internal/schemaviz",
		read: func(p *goPackage) ([]string, error) {
			return p.ConstsNamed("FormatDOT", "FormatMermaid")
		},
		sources: "internal/schemaviz FormatDOT and FormatMermaid",
	},
	{
		name:    "dialect",
		pkgDir:  "core/platform",
		read:    func(p *goPackage) ([]string, error) { return p.UntypedStringConsts() },
		sources: "core/platform's canonical dialect constants",
	},
}

// Installable reports whether `go install` can reach this program from outside
// the module.
//
// The bound is cmd/, without an `internal/` or `testdata/` segment. `internal`
// is unreachable from another module by the language's own rule, and a testdata
// helper is a fixture rather than a program. What that leaves is the six the
// inventory's section 15 names, which is the set the audit's critic required a
// decision for: three released binaries, two contributor tools, and `cmd`
// itself -- three lines calling root.Execute, a fourth complete copy of the
// native CLI that no previous inventory, gate or release has ever mentioned.
//
// It deliberately leaves out the installable `main` packages elsewhere in the
// tree -- examples/annotation_parser, examples/extension_ignore and
// migration/generator/example resolve for `go install` too. They are named in
// the inventory's prose rather than as rows, and widening this predicate is the
// change that would make them rows.
func (p Program) Installable() bool {
	if !strings.HasPrefix(p.Dir, "cmd") {
		return false
	}
	for segment := range strings.SplitSeq(p.Dir, "/") {
		if segment == "internal" || segment == "testdata" {
			return false
		}
	}
	return true
}

// NewSurfaces reads every non-command surface from the code and the ledger.
func NewSurfaces(repoRoot string) (*Surfaces, error) {
	packages, err := LedgerPackages(repoRoot)
	if err != nil {
		return nil, err
	}
	programs, err := Programs(repoRoot)
	if err != nil {
		return nil, err
	}
	surfaces := &Surfaces{Packages: packages, Programs: programs}
	for _, source := range formatSources {
		pkg, err := loadGoPackage(repoRoot, source.pkgDir)
		if err != nil {
			return nil, err
		}
		values, err := source.read(pkg)
		if err != nil {
			return nil, err
		}
		surfaces.Formats = append(surfaces.Formats, FormatList{
			Name:   source.name,
			Source: source.sources,
			Values: sortedUnique(values),
		})
	}
	sort.Slice(surfaces.Formats, func(i, j int) bool { return surfaces.Formats[i].Name < surfaces.Formats[j].Name })
	if len(surfaces.Formats) != len(formatSources) {
		return nil, fmt.Errorf("featureinventory: %d format sets declared, %d read", len(formatSources), len(surfaces.Formats))
	}
	return surfaces, nil
}

// ledgerEntry matches a public-API ledger list item.
//
// List items only, which is the same reading scripts/check-public-api.sh does
// and for the same reason: a package whose only mention in the ledger is a prose
// paragraph is not listed, and stokaro/ptah#2246 records a fixture proving a
// scrape that accepted prose would pass.
var ledgerEntry = regexp.MustCompile("^- `(go\\.5x5\\.cz/ptah/[^`]+)`")

// LedgerPackages reads the stable Go packages out of docs/public_api.md.
func LedgerPackages(repoRoot string) ([]string, error) {
	body, err := os.ReadFile(filepath.Join(repoRoot, "docs", "public_api.md"))
	if err != nil {
		return nil, fmt.Errorf("featureinventory: reading the public API ledger: %w", err)
	}
	var packages []string
	for line := range strings.SplitSeq(string(body), "\n") {
		if match := ledgerEntry.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			packages = append(packages, match[1])
		}
	}
	if len(packages) == 0 {
		return nil, fmt.Errorf("featureinventory: docs/public_api.md listed no packages; refusing to report a public surface read from nothing")
	}
	return sortedUnique(packages), nil
}
