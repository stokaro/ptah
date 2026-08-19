// Command boundaries measures the architecture boundaries ADR 0001 declares and
// refuses the ones that grow.
//
// # Why it reads the type checker rather than the source text
//
// stokaro/ptah#1344 asks for executable invariants and rules out searching for
// spellings: "Do not grep for string concatenation, dialect comparisons, helper
// names, or specific type spellings. Those checks are bypassable and produce
// false positives."
//
// That is not a stylistic preference, and the cost was paid before this
// existed. Counting one of these rules by searching for a type spelling
// answered wrongly twice -- five files, then eight sites -- because a doc
// comment showing a caller how to build a schema looks exactly like a caller
// building one. The true figure was four. So every rule here is answered from
// the import graph and the type checker: an import edge is an edge the compiler
// resolved, and a construction site is a composite literal whose type the type
// checker says is the one named.
//
// # Shrink-only
//
// Two of the four rules are violated today. A gate demanding zero on arrival
// would be red from the commit that added it, and a red gate teaches a reader
// to skip it. So the recorded counts may fall and may never rise, which is the
// baseline discipline #1344 asks for and ADR 0001 stage 0 schedules.
//
// A fall also fails, which reads odd until the alternative is written down: a
// ceiling nobody lowers is not a ratchet, and leaving the old number recorded
// would let the debt return to it with the gate green the whole way. The
// refusal names the command that records the improvement.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// modulePath is the import path every package in this module shares.
const modulePath = "go.5x5.cz/ptah"

// baselineName is the checked-in record of what each rule currently permits.
const baselineName = "docs/architecture_boundaries.json"

// rule is one forbidden dependency direction from ADR 0001 section 3.2.
//
// The predicate is expressed over resolved packages rather than over names, so
// a rule cannot be satisfied by renaming a package, and cannot fire on a
// package whose name merely resembles a layer.
type rule struct {
	ID      string
	Summary string
	// violations answers the rule for one loaded package, returning a finding
	// per offence.
	violations func(pkg *packages.Package) []finding
}

// finding is one recorded offence: which package, and what it did.
type finding struct {
	Package string `json:"package"`
	Detail  string `json:"detail"`
	// Position is where the type checker found it. It is reported for a human
	// and deliberately NOT part of the baseline comparison, so moving a line
	// does not read as new debt.
	Position string `json:"position,omitempty"`
}

// ruleReport is a rule's measured state.
type ruleReport struct {
	Rule     string    `json:"rule"`
	Summary  string    `json:"summary"`
	Count    int       `json:"count"`
	Findings []finding `json:"findings"`
}

// baseline is the checked-in permitted state.
type baseline struct {
	// Comment is for whoever opens the file rather than for the program.
	Comment string         `json:"comment"`
	Rules   map[string]int `json:"rules"`
}

func main() {
	update := flag.Bool("update", false, "rewrite the baseline from the current tree")
	jsonOut := flag.Bool("json", false, "print the measured report as JSON")
	flag.Parse()

	root, err := repoRoot()
	fail(err)
	reports, err := measure(root)
	fail(err)

	if *jsonOut {
		out, err := json.MarshalIndent(reports, "", "  ")
		fail(err)
		fmt.Println(string(out))
		return
	}
	if *update {
		fail(writeBaseline(root, reports))
		fmt.Printf("boundaries: baseline updated (%s)\n", summarize(reports))
		return
	}
	fail(check(root, reports))
	fmt.Printf("boundaries: OK (%s)\n", summarize(reports))
}

func fail(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func summarize(reports []ruleReport) string {
	parts := make([]string, 0, len(reports))
	for _, report := range reports {
		parts = append(parts, fmt.Sprintf("%s=%d", report.Rule, report.Count))
	}
	return strings.Join(parts, " ")
}

// rules are the four forbidden directions, in the order ADR 0001 states them.
func rules() []rule {
	return []rule{
		{
			ID:      "model-imports-pipeline",
			Summary: "the canonical model (core/) must not import comparison, planning or conversion",
			violations: func(pkg *packages.Package) []finding {
				return importEdges(pkg, under("core/"), anyUnder(
					"internal/planner/", "migration/schemadiff", "internal/convert/"))
			},
		},
		{
			ID:         "pipeline-builds-source-description",
			Summary:    "a planner or comparator must not construct a source schema description",
			violations: constructsSourceDescription,
		},
		{
			ID:      "pipeline-imports-execution",
			Summary: "planning must not import versioned execution",
			violations: func(pkg *packages.Package) []finding {
				return importEdges(pkg,
					anyUnder("internal/planner/", "migration/planner", "migration/schemadiff"),
					under("migration/migrator"))
			},
		},
		{
			ID:      "renderer-imports-comparator",
			Summary: "a renderer must not import a comparator",
			violations: func(pkg *packages.Package) []finding {
				return importEdges(pkg, contains("renderer"), contains("schemadiff"))
			},
		},
	}
}

// under matches a package by import-path prefix, relative to the module.
func under(prefix string) func(string) bool {
	return func(rel string) bool { return strings.HasPrefix(rel, prefix) }
}

func contains(fragment string) func(string) bool {
	return func(rel string) bool { return strings.Contains(rel, fragment) }
}

func anyUnder(prefixes ...string) func(string) bool {
	return func(rel string) bool {
		return slices.ContainsFunc(prefixes, func(prefix string) bool {
			return strings.HasPrefix(rel, prefix)
		})
	}
}

// importEdges reports each import of pkg that crosses a forbidden direction.
//
// It reads pkg.Imports, which the loader filled from the compiler's own
// resolution, so an edge here is an edge that exists.
func importEdges(pkg *packages.Package, from, to func(string) bool) []finding {
	self := relative(pkg.PkgPath)
	if self == "" || !from(self) {
		return nil
	}
	found := make([]finding, 0)
	for path := range pkg.Imports {
		target := relative(path)
		if target == "" || !to(target) {
			continue
		}
		found = append(found, finding{Package: self, Detail: "imports " + target})
	}
	return found
}

// constructsSourceDescription reports composite literals of a source-schema
// description type inside a planning or comparison package.
//
// The type is resolved by the type checker, so a doc comment showing a caller
// how to build one is not a finding, and a local alias of the same type is.
func constructsSourceDescription(pkg *packages.Package) []finding {
	self := relative(pkg.PkgPath)
	if self == "" || !anyUnder("internal/planner/", "migration/planner", "migration/schemadiff")(self) {
		return nil
	}
	found := make([]finding, 0)
	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			typ := pkg.TypesInfo.TypeOf(literal)
			if typ == nil || !isSourceDescription(typ) {
				return true
			}
			found = append(found, finding{
				Package:  self,
				Detail:   "constructs " + shortType(typ),
				Position: pkg.Fset.Position(literal.Pos()).String(),
			})
			return true
		})
	}
	return found
}

// sourceDescriptionTypes are the types that describe a schema as a SOURCE
// wrote it, which a pipeline stage should receive rather than build.
var sourceDescriptionTypes = []string{
	modulePath + "/core/goschema.Database",
	modulePath + "/dbschema/types.DBSchema",
}

func isSourceDescription(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	if !ok {
		return false
	}
	object := named.Obj()
	if object == nil || object.Pkg() == nil {
		return false
	}
	return slices.Contains(sourceDescriptionTypes, object.Pkg().Path()+"."+object.Name())
}

func shortType(typ types.Type) string {
	named, ok := typ.(*types.Named)
	if !ok {
		return typ.String()
	}
	return relative(named.Obj().Pkg().Path()) + "." + named.Obj().Name()
}

// relative strips the module prefix, and reports the empty string for a package
// outside this module so a dependency can never be counted as local debt.
func relative(path string) string {
	if path == modulePath {
		return "."
	}
	if !strings.HasPrefix(path, modulePath+"/") {
		return ""
	}
	return strings.TrimPrefix(path, modulePath+"/")
}

// measure loads every package in the module and answers each rule.
func measure(root string) ([]ruleReport, error) {
	config := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir: root,
		// Tests are excluded on purpose. A test may legitimately build a source
		// description to feed the stage under test; the rule is about what the
		// production path does.
		Tests: false,
	}
	loaded, err := packages.Load(config, "./...")
	if err != nil {
		return nil, fmt.Errorf("load packages: %w", err)
	}
	if packages.PrintErrors(loaded) > 0 {
		return nil, fmt.Errorf("packages failed to load")
	}

	reports := make([]ruleReport, 0, len(rules()))
	for _, r := range rules() {
		found := make([]finding, 0)
		for _, pkg := range loaded {
			found = append(found, r.violations(pkg)...)
		}
		sort.Slice(found, func(i, j int) bool {
			if found[i].Package != found[j].Package {
				return found[i].Package < found[j].Package
			}
			return found[i].Detail < found[j].Detail
		})
		reports = append(reports, ruleReport{
			Rule: r.ID, Summary: r.Summary, Count: len(found), Findings: found,
		})
	}
	return reports, nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above the working directory")
		}
		dir = parent
	}
}

func readBaseline(root string) (baseline, error) {
	data, err := os.ReadFile(filepath.Join(root, baselineName))
	if err != nil {
		return baseline{}, fmt.Errorf("read %s: %w", baselineName, err)
	}
	var recorded baseline
	if err := json.Unmarshal(data, &recorded); err != nil {
		return baseline{}, fmt.Errorf("parse %s: %w", baselineName, err)
	}
	return recorded, nil
}

func writeBaseline(root string, reports []ruleReport) error {
	recorded := baseline{
		Comment: "Recorded architecture-boundary debt (stokaro/ptah#1344, ADR 0001 section 8 stage 0). " +
			"A count may fall and may never rise. Regenerate with: go run ./internal/cmd/boundaries -update",
		Rules: make(map[string]int),
	}
	for _, report := range reports {
		recorded.Rules[report.Rule] = report.Count
	}
	data, err := json.MarshalIndent(recorded, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, baselineName), append(data, '\n'), 0o600)
}

// check refuses a rule that grew, and a baseline that no longer describes the
// rule set.
func check(root string, reports []ruleReport) error {
	recorded, err := readBaseline(root)
	if err != nil {
		return err
	}
	problems := make([]string, 0)
	for _, report := range reports {
		permitted, known := recorded.Rules[report.Rule]
		if !known {
			problems = append(problems, fmt.Sprintf(
				"rule %q has no recorded baseline; add it with -update after deciding it is acceptable",
				report.Rule))
			continue
		}
		if report.Count > permitted {
			problems = append(problems, fmt.Sprintf(
				"rule %q grew from %d to %d -- %s\n%s",
				report.Rule, permitted, report.Count, report.Summary, indent(report.Findings)))
			continue
		}
		// A shrink is the good outcome and still fails, because a ceiling that
		// never tightens is not a ratchet: leaving the higher number recorded
		// would let the debt come back to it unnoticed. The fix is one command
		// and the message says which.
		if report.Count < permitted {
			problems = append(problems, fmt.Sprintf(
				"rule %q fell from %d to %d -- record it so it cannot come back: "+
					"go run ./internal/cmd/boundaries -update",
				report.Rule, permitted, report.Count))
		}
	}
	for name := range recorded.Rules {
		if !slices.ContainsFunc(reports, func(r ruleReport) bool { return r.Rule == name }) {
			problems = append(problems, fmt.Sprintf(
				"baseline records rule %q, which no longer exists; remove it with -update", name))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("boundaries: %s\n\na count may fall and may never rise (stokaro/ptah#1344)",
		strings.Join(problems, "\n"))
}

func indent(found []finding) string {
	lines := make([]string, 0, len(found))
	for _, item := range found {
		lines = append(lines, "    "+item.Package+" "+item.Detail)
	}
	return strings.Join(lines, "\n")
}
