package featureinventory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v3"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/internal/agentsurface"
)

// ClaimedFloor is the number of claimed rows the gate refuses to fall below.
//
// It is a constant here, and not a number the artifact carries, because a
// ratchet read out of the file it guards is not a ratchet. Reading it from the
// artifact and writing it back made `claimed_floor` the one field the byte
// comparison could not police -- editing that line lowered the floor and the
// gate reported success, so a coverage regression could be laundered through
// `--write`, and a page claiming a feature it does not document raised the
// floor to lock the false claim in as coverage.
//
// Moving it forward is therefore a reviewed source edit, which is what the
// number is: a statement that this many features had a page on the day somebody
// looked. internal/quickstart.DefaultFloors is the same shape for the same
// reason.
const ClaimedFloor = 79

// Kind is a row's provenance: which declaration the row was derived from.
//
// A kind is not a category somebody chose for a feature. It names the source,
// so a reader can go to that source and get the same answer.
type Kind string

const (
	// KindNativeVerb is one runnable leaf of the native `ptah` command tree.
	KindNativeVerb Kind = "native-verb"
	// KindPublicPackage is one import path the stable-embedder ledger lists.
	KindPublicPackage Kind = "public-package"
	// KindProgram is one binary the release configuration builds.
	KindProgram Kind = "program"
	// KindDialect is one dialect name the renderer accepts, normalized.
	KindDialect Kind = "dialect"
)

// kindOrder is the order the kinds are emitted in, so the artifact is stable
// under a change to the constant block above.
var kindOrder = []Kind{KindNativeVerb, KindPublicPackage, KindProgram, KindDialect}

// Row is one feature. Four fields, because there is no fifth thing a machine
// can check exactly.
type Row struct {
	// ID is derived from Surface. It exists because a gate, an issue and an
	// `owns:` entry each need one anchor-safe token, and an import path is not
	// one.
	ID string `json:"id"`
	// Kind is which declaration produced the row.
	Kind Kind `json:"kind"`
	// Surface is the declaration's own spelling of the feature.
	Surface string `json:"surface"`
	// ClaimedBy is the repository path of the page whose frontmatter claims this
	// ID, and null where no page claims it. It is never authored here: it is
	// the path of the file the claim was read from.
	//
	// The field is named for what the gate checks. It checks that the claim
	// resolves to a derived feature, that no second page claims the same one,
	// and that the value is the claiming file's own path -- all by string
	// equality. It does not check that the page explains the feature, because
	// nothing can: that would need a search over prose, which is the comparison
	// this register exists to have none of. "Canonical page" reads as a
	// stronger promise than any of those, so the column does not use the word.
	ClaimedBy *string `json:"claimed_by"`
}

// Example is one page whose commands continuous integration executes, with the
// counts internal/quickstart reports for it. It attributes nothing to a
// feature: see notice entry 5.
type Example struct {
	// Page is the page's repository path.
	Page string `json:"page"`
	// Shells are the per-shell counts, in internal/quickstart's shell order.
	Shells []ExampleShell `json:"shells"`
}

// ExampleShell is one shell's measured counts for one page.
type ExampleShell struct {
	Shell        string `json:"shell"`
	Steps        int    `json:"steps"`
	Expectations int    `json:"expectations"`
}

// Document is the committed artifact.
type Document struct {
	// Notice is what this file does not claim, carried in the artifact so
	// nobody reads it as more than it is.
	Notice []string `json:"notice"`
	// Generator is the command that rewrites the file.
	Generator string `json:"generator"`
	// ClaimedFloor is the claimed-row count the gate refuses to fall below. It
	// is rendered from the [ClaimedFloor] constant, so the byte comparison
	// polices this line like every other derived value.
	ClaimedFloor int `json:"claimed_floor"`
	// Claimed is the number of rows a page claims, as measured.
	Claimed int `json:"claimed"`
	// Rows are the features, by kind then by ID.
	Rows []Row `json:"rows"`
	// RunnableExamples are the pages whose commands are executed, from
	// quickstart.Discover.
	RunnableExamples []Example `json:"runnable_examples"`
}

// Notice is the artifact's own statement of its limits.
//
// It is generated rather than written into the file, so it cannot be edited
// away from what the code does without the gate noticing.
func Notice() []string {
	return []string{
		"Generated. Every row is derived from a declaration the product already maintains; nothing here is authored.",
		"1. Not that a page explains its feature. `owns:` is a page's own claim, and claimed_by is the file it was read from. " +
			"The gate proves the claim resolves to a derived feature and that no second page makes it. It cannot read, so " +
			"no column here says canonical.",
		"2. Not that every feature has a page. A row nobody claims carries claimed_by null. claimed_floor is rendered " +
			"from a constant in internal/featureinventory, not carried forward from this file: a ratchet read out of the " +
			"file it guards would let one edited line lower it.",
		"3. Not that the surface list is complete. It covers four kinds. Schema formats, migration file formats, OCI media types, " +
			"MCP tools, GitHub Action inputs and PTAH_* variables are absent, each because no canonical enumeration exists to " +
			"compare against. Absent means not enumerable today, never does not exist.",
		"4. Not stability, maturity, support level or Atlas CE classification. No column carries any of them.",
		"5. Not that any example exercises any feature. runnable_examples is page-scoped: it says these pages carry " +
			"commands continuous integration runs and whose output it checks, and attributes nothing to a row. A page " +
			"that publishes no step is refused rather than listed.",
		"6. Not that a dialect row is a promise. A dialect being accepted is not a promise that every construct renders on it; docs/capabilities.md and `ptah db capabilities` answer that.",
		"7. Not that the walk reaches every spelling the binary answers to. `__complete` and `__completeNoDesc` come from cobra's unexported initCompleteCmd and no walk can reach them; internal/agentsurface/walk.go says so.",
		"8. Not the compatibility tree. AGENTS.md holds cmd/ptah-compat/main.go as the only non-test file outside cmd/atlas " +
			"that may import cmd/atlas, so no tool here can walk that tree. Adding the kind is a maintainer decision about " +
			"that rule, not a discovery.",
		"9. Not a verification date. There is no last-verified column: the file is regenerated on every continuous-integration run, and a stale one is a red gate.",
	}
}

// GeneratorCommand is the one command that changes the artifact.
const GeneratorCommand = "scripts/check-feature-inventory.sh --write"

// Sources are the declarations a document is derived from.
//
// The command tree arrives as walked leaves rather than as a root command, so
// this package imports no command tree and the rules can be driven against
// fixtures.
type Sources struct {
	// ModulePath is the module the ledger's packages belong to, read from
	// go.mod by the caller. An empty value derives no packages rather than
	// every backticked list item, so the mistake surfaces as empty-kind.
	ModulePath string
	// NativeLeaves is agentsurface.Walk of the native root command.
	NativeLeaves []agentsurface.Leaf
	// Ledger is the bytes of docs/public_api.md.
	Ledger []byte
	// Release is the bytes of .goreleaser.yaml.
	Release []byte
	// Pages are the documentation pages and the IDs each one claims.
	Pages []PageClaim
	// Examples are the executed pages, from quickstart.Discover.
	Examples []Example
	// ClaimedFloor is the claimed-row count below which the derivation refuses.
	// The generator passes the [ClaimedFloor] constant; a fixture passes its
	// own.
	ClaimedFloor int
}

// Derive builds the document, and returns every rule violation it found.
//
// A violation is reported rather than silently absorbed into a different
// artifact: an `owns:` entry naming an identifier the derivation does not
// produce is a mistake somebody has to see, not a row to drop.
func Derive(src Sources) (*Document, []Problem) {
	rows, problems := deriveRows(src)
	claimed, claimProblems := applyClaims(rows, src.Pages)
	problems = append(problems, claimProblems...)
	problems = append(problems, exampleProblems(src.Examples)...)

	doc := &Document{
		Notice:           Notice(),
		Generator:        GeneratorCommand,
		ClaimedFloor:     src.ClaimedFloor,
		Claimed:          claimed,
		Rows:             rows,
		RunnableExamples: src.Examples,
	}
	if claimed < src.ClaimedFloor {
		problems = append(problems, Problem{RuleClaimedBelowFloor, fmt.Sprintf(
			"%d row(s) are claimed by a page, below the floor of %d that internal/featureinventory.ClaimedFloor holds; a page stopped claiming a feature",
			claimed, src.ClaimedFloor)})
	}
	if len(src.Examples) == 0 {
		problems = append(problems, Problem{RuleNoExamples,
			"no page opts in to internal/quickstart; refusing to report an empty runnable-example set"})
	}
	sortProblems(problems)
	return doc, problems
}

// exampleProblems refuses a marked page that publishes nothing to run.
//
// The marking is deliberate -- a page writes `quickstart: true` -- but a
// deliberate marking is a claim, and this is the exact check available on it:
// the page's extracted programs either carry steps or they do not. Without it a
// page of prose could be marked and published under `runnable_examples` while
// executing nothing, and the gate would report success, which is the shape of
// every false green this register was rebuilt to remove.
func exampleProblems(examples []Example) []Problem {
	var problems []Problem
	for _, example := range examples {
		if len(example.Shells) == 0 {
			problems = append(problems, Problem{RuleExampleRunsNothing, fmt.Sprintf(
				"%s opts in to internal/quickstart but publishes no steps for any shell; a page listed under runnable_examples has to run something",
				example.Page)})
			continue
		}
		for _, shell := range example.Shells {
			if shell.Steps > 0 {
				continue
			}
			problems = append(problems, Problem{RuleExampleRunsNothing, fmt.Sprintf(
				"%s publishes a %s program with no steps; a page listed under runnable_examples has to run something",
				example.Page, shell.Shell)})
		}
	}
	return problems
}

// deriveRows produces the rows of every kind and refuses a kind that came up
// empty. A kind that derives nothing reports the same success as a kind that
// derived everything, which is the failure this floor exists to prevent.
func deriveRows(src Sources) ([]Row, []Problem) {
	byKind := map[Kind][]Row{
		KindNativeVerb:    verbRows(src.NativeLeaves),
		KindPublicPackage: packageRows(LedgerPackages(src.Ledger, src.ModulePath), src.ModulePath),
		KindProgram:       programRows(ReleaseBinaries(src.Release)),
		KindDialect:       dialectRows(),
	}

	var rows []Row
	var problems []Problem
	for _, kind := range kindOrder {
		if len(byKind[kind]) == 0 {
			problems = append(problems, Problem{RuleEmptyKind, fmt.Sprintf(
				"the %s source derived no rows; refusing to report a vacuous inventory", kind)})
		}
		rows = append(rows, byKind[kind]...)
	}

	seen := make(map[string]string, len(rows))
	for _, row := range rows {
		if first, clash := seen[row.ID]; clash {
			problems = append(problems, Problem{RuleIdentifierCollision, fmt.Sprintf(
				"identifier %q is derived from both %q and %q; the derivation is not injective",
				row.ID, first, row.Surface)})
			continue
		}
		seen[row.ID] = row.Surface
	}
	return rows, problems
}

func verbRows(leaves []agentsurface.Leaf) []Row {
	rows := make([]Row, 0, len(leaves))
	for _, leaf := range leaves {
		rows = append(rows, Row{
			ID:      "cli-ptah-" + slug(leaf.Name),
			Kind:    KindNativeVerb,
			Surface: "ptah " + leaf.Name,
		})
	}
	return sorted(rows)
}

func packageRows(paths []string, modulePath string) []Row {
	rows := make([]Row, 0, len(paths))
	for _, path := range paths {
		rows = append(rows, Row{
			ID:      "gopkg-" + slug(strings.TrimPrefix(path, modulePath+"/")),
			Kind:    KindPublicPackage,
			Surface: path,
		})
	}
	return sorted(rows)
}

func programRows(binaries []string) []Row {
	rows := make([]Row, 0, len(binaries))
	for _, binary := range binaries {
		rows = append(rows, Row{
			ID:      "program-" + slug(binary),
			Kind:    KindProgram,
			Surface: binary,
		})
	}
	return sorted(rows)
}

// dialectRows is the normalized image of the renderer's own list under the
// platform's own normalizer: a canonical product API applied to a canonical
// product API, computed in process. Rows for the aliases the first returns
// would be noise, and reconstructing the set from Go source would be the AST
// extraction this file exists to avoid.
func dialectRows() []Row {
	seen := make(map[string]bool)
	var rows []Row
	for _, spelling := range renderer.SupportedDialects() {
		name := platform.NormalizeDialect(spelling)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		rows = append(rows, Row{ID: "dialect-" + slug(name), Kind: KindDialect, Surface: name})
	}
	return sorted(rows)
}

func sorted(rows []Row) []Row {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// ReleaseBinaries returns the binaries the release configuration builds.
//
// The product declaring what it ships is the only list that is wrong when a
// binary ships unlisted -- the release would not contain it. A `go list` scan
// for main packages answers a different question: which programs are
// technically installable, which is not a statement that any of them is
// supported.
func ReleaseBinaries(source []byte) []string {
	var config struct {
		Builds []struct {
			Binary string `yaml:"binary"`
		} `yaml:"builds"`
	}
	if err := yaml.Unmarshal(source, &config); err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var binaries []string
	for _, build := range config.Builds {
		if build.Binary == "" || seen[build.Binary] {
			continue
		}
		seen[build.Binary] = true
		binaries = append(binaries, build.Binary)
	}
	sort.Strings(binaries)
	return binaries
}

var slugRun = regexp.MustCompile(`[^a-z0-9]+`)

// slug folds a surface into an anchor-safe token. The derivation's injectivity
// is not assumed: deriveRows compares the produced identifiers and reports a
// collision rather than letting two features share a row.
func slug(surface string) string {
	return strings.Trim(slugRun.ReplaceAllString(strings.ToLower(surface), "-"), "-")
}

// Render writes the document as the committed artifact: two-space JSON with a
// trailing newline, which is what `git diff` and every editor produce.
func Render(doc *Document) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(doc); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
