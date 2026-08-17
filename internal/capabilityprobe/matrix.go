package capabilityprobe

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// CICell is one matrix cell in the form continuous integration consumes it.
//
// It is DERIVED from Cells and never edited alongside it. The tiered pipeline
// of stokaro/ptah#1341 fans out over these entries, so a second hand-written
// list of release lines would be exactly the drift the issue exists to
// prevent: a line added to cells.go and forgotten in a workflow reads as a
// line nobody has to measure.
type CICell struct {
	// ID names the cell in a job name, an artifact name and a file name. It is
	// the dialect and the line with the dots flattened.
	ID string `json:"id"`

	Dialect string `json:"dialect"`
	Line    string `json:"line"`
	Label   string `json:"label,omitempty"`

	// Preset is the capability preset the line claims, empty when Ptah has
	// none for it yet.
	Preset string `json:"preset"`

	// Support is what Ptah promises about the line, in the vocabulary
	// [capability.SupportLevel] defines. It travels with the cell so that a
	// job, an artifact and the generated documentation all say which promise
	// the run was measuring: a certified line going red is a broken
	// commitment, a best-effort line going red is news about a line nobody
	// claimed, and a consumer that cannot tell them apart has to treat both
	// as the louder one.
	//
	// Like every field here it is DERIVED from the declaring cell and never
	// edited beside it. Writing "certified" a second time is how the level
	// stops meaning what cells.go's census enforces, which is that continuous
	// integration actually exercises the line.
	Support capability.SupportLevel `json:"support_level"`

	Refinement string `json:"refinement"`

	// Image reproduces the line, empty when no container does.
	Image string `json:"image,omitempty"`

	// TagPinsLine reports whether Image's tag names the release LINE, so the
	// registry resolves it to that line's newest patch, rather than freezing
	// one patch. ResolveNewestPatch means the matrix driver performs that
	// resolution when the registry publishes no floating line tag.
	//
	// This is the scoping rule of stokaro/ptah#1341 made checkable per cell
	// rather than asserted in prose: "mariadb:10.11" satisfies it and
	// "cockroachdb/cockroach:v26.2.5" does not.
	TagPinsLine bool `json:"tag_pins_line"`

	ResolveNewestPatch bool `json:"resolve_newest_patch,omitempty"`

	// DockerRun is the full argument list for `docker run`, after the caller's
	// own --detach and --name. It is one flat list rather than separate env,
	// port and command fields because the container command follows the image
	// and a consumer assembling three lists in the right order is a consumer
	// that can assemble them in the wrong one.
	DockerRun []string `json:"docker_run,omitempty"`

	// URL addresses the started server.
	URL string `json:"url,omitempty"`

	// SuiteDatabase is the --databases value the integration runner knows this
	// dialect by. SuiteURLEnv and SuiteURL configure the scenario connection;
	// the optional cleanup pair configures the privileged cleanup connection.
	// All are empty when the runner has no target for the dialect.
	SuiteDatabase      string `json:"suite_database,omitempty"`
	SuiteURLEnv        string `json:"suite_url_env,omitempty"`
	SuiteURL           string `json:"suite_url,omitempty"`
	SuiteCleanupURLEnv string `json:"suite_cleanup_url_env,omitempty"`
	SuiteCleanupURL    string `json:"suite_cleanup_url,omitempty"`

	// Runnable reports whether a tier can execute this cell at all.
	Runnable bool `json:"runnable"`

	// Skip says why it cannot, and is empty exactly when Runnable is true. A
	// cell that cannot run is reported with this reason rather than dropped:
	// an absent cell reads as a passing one.
	Skip string `json:"skip,omitempty"`

	// SuiteSkip says why the integration runner has no target for this cell,
	// and is empty exactly when the Suite fields are populated.
	//
	// A dialect can be capability-measured before it can be suite-tested, and
	// the two were one decision until Spanner needed them apart: its rows are
	// measurable against a live endpoint today, while the integration suite
	// has no Spanner target at all. Requiring both would leave the dialect
	// unmeasured for as long as the second half takes -- and the first half is
	// what keeps a preset honest in the meantime (stokaro/ptah#942).
	//
	// It is a stated reason rather than an empty field for the same reason
	// Skip is: a cell that quietly runs half of what its neighbors run reads
	// as a cell that ran.
	SuiteSkip string `json:"suite_skip,omitempty"`

	// Note carries the declaring cell's own note.
	Note string `json:"note,omitempty"`
}

// Matrix is the CI view of the whole capability matrix: what runs, what does
// not, and the census that proves the two halves account for every declared
// line.
type Matrix struct {
	// Declared is len(Cells) in cells.go.
	Declared int `json:"declared"`
	// Cells are the lines a tier fans out over, in declaration order.
	Cells []CICell `json:"cells"`
	// Skipped are the declared lines no tier can execute, each carrying its
	// reason.
	Skipped []CICell `json:"skipped"`
}

// Validate refuses a matrix that cannot honestly gate anything.
//
// The empty fan-out is the case worth naming. A strategy built from an empty
// list produces no jobs, GitHub reports the workflow as successful, and a
// pipeline that measured nothing is indistinguishable from one that measured
// everything and found nothing wrong. The census identity is the same refusal
// one step further in: every declared line is either run or skipped for a
// stated reason, so a line cannot leave the pipeline by being forgotten.
func (m Matrix) Validate() error {
	var problems []error
	if m.Declared == 0 {
		problems = append(problems, errors.New(
			"the capability matrix declares no release lines; a pipeline with no cells must not report success"))
	}
	if len(m.Cells) == 0 {
		problems = append(problems, errors.New(
			"no declared release line is runnable, so the fan-out would produce zero jobs and pass by examining nothing"))
	}
	if got := len(m.Cells) + len(m.Skipped); got != m.Declared {
		problems = append(problems, fmt.Errorf(
			"the census does not add up: %d declared, %d runnable, %d skipped", m.Declared, len(m.Cells), len(m.Skipped)))
	}
	problems = append(problems, m.cellProblems()...)
	return errors.Join(problems...)
}

func (m Matrix) cellProblems() []error {
	var problems []error
	seen := map[string]bool{}
	for _, cell := range append(slices.Clone(m.Cells), m.Skipped...) {
		if seen[cell.ID] {
			problems = append(problems, fmt.Errorf("two cells share the id %q, so their jobs and artifacts would collide", cell.ID))
		}
		seen[cell.ID] = true
		problems = append(problems, cell.problems()...)
	}
	return problems
}

func (c CICell) problems() []error {
	if !c.Runnable {
		if c.Skip == "" {
			return []error{fmt.Errorf("cell %s is not runnable and says no reason why", c.ID)}
		}
		return nil
	}
	var problems []error
	if c.URL == "" {
		problems = append(problems, fmt.Errorf("cell %s is runnable with no URL to probe", c.ID))
	}
	if len(c.DockerRun) == 0 {
		problems = append(problems, fmt.Errorf("cell %s is runnable with no way to start a server", c.ID))
	}
	suiteDeclared := c.SuiteDatabase != "" || c.SuiteURLEnv != "" || c.SuiteURL != ""
	switch {
	case suiteDeclared && (c.SuiteDatabase == "" || c.SuiteURLEnv == "" || c.SuiteURL == ""):
		problems = append(problems, fmt.Errorf("cell %s is runnable with an incomplete integration-runner target", c.ID))
	case suiteDeclared && c.SuiteSkip != "":
		problems = append(problems, fmt.Errorf(
			"cell %s declares an integration-runner target and the reason %q for having none", c.ID, c.SuiteSkip))
	case !suiteDeclared && c.SuiteSkip == "":
		problems = append(problems, fmt.Errorf(
			"cell %s is runnable with no integration-runner target and says no reason why", c.ID))
	}
	if (c.SuiteCleanupURLEnv == "") != (c.SuiteCleanupURL == "") {
		problems = append(problems, fmt.Errorf("cell %s has only half of its integration cleanup connection", c.ID))
	}
	if c.ResolveNewestPatch && !c.TagPinsLine {
		problems = append(problems, fmt.Errorf(
			"cell %s asks to resolve a newest patch but its image selector does not name release line %s", c.ID, c.Line))
	}
	if c.Skip != "" {
		problems = append(problems, fmt.Errorf("cell %s is runnable and carries the skip reason %q", c.ID, c.Skip))
	}
	return problems
}

// PresetGaps returns one error per declared release line that names no
// capability preset.
//
// This is one half of "the matrix is checked against the presets." A new
// release line must carry measured preset evidence before the pull-request
// fan-out can pass; silent saturation onto an older line is not evidence.
func PresetGaps() []error {
	var gaps []error
	for _, cell := range Cells {
		if cell.Measured() {
			continue
		}
		gaps = append(gaps, fmt.Errorf(
			"release line %s names no capability preset: %s", cell.String(), cell.Note))
	}
	return gaps
}

// IDs returns the runnable cell ids, which is the set a tier must produce a
// result for.
func (m Matrix) IDs() []string {
	ids := make([]string, 0, len(m.Cells))
	for _, cell := range m.Cells {
		ids = append(ids, cell.ID)
	}
	return ids
}

// Find returns the runnable cell with an id.
func (m Matrix) Find(id string) (CICell, bool) {
	for _, cell := range m.Cells {
		if cell.ID == id {
			return cell, true
		}
	}
	return CICell{}, false
}

// launcher is how one dialect's server is started and addressed. It is keyed
// by dialect rather than by line because the recipe does not vary by release:
// what varies is the image tag, and that comes from the cell.
type launcher struct {
	// flags precede the image on the docker run command line.
	flags []string
	// command follows it.
	command []string
	// url addresses the container from the runner's own host namespace.
	url string
	// suiteDatabase and the URL fields wire the same server into the integration
	// runner for tier 3. Empty when the runner has no target for the dialect,
	// which suiteSkip then has to say.
	suiteDatabase      string
	suiteSkip          string
	suiteURLEnv        string
	suiteURL           string
	suiteCleanupURLEnv string
	suiteCleanupURL    string
}

// launchers covers exactly the dialects whose servers this repository already
// starts, with the recipes taken from docker-compose.yaml and
// .github/workflows/go-integration-tests.yml so that a cell probes the same
// shape of server the rest of CI does.
//
// The credentials are the throwaway ones those two files already use, in the
// clear for the same reason: they are the password the container is created
// with, three lines above the URL that uses it, on a server that exists for
// the length of one job and listens only on the runner's own loopback.
//
// #nosec G101 -- these are the container's own throwaway credentials, created on the line above and thrown away with the job
var launchers = map[string]launcher{
	platform.Postgres: {
		flags: []string{
			"--publish", "5432:5432",
			"--env", "POSTGRES_DB=ptah_test",
			"--env", "POSTGRES_USER=ptah_user",
			"--env", "POSTGRES_PASSWORD=ptah_password",
		},
		url:           "postgres://ptah_user:ptah_password@127.0.0.1:5432/ptah_test?sslmode=disable",
		suiteDatabase: "postgres",
		suiteURLEnv:   "POSTGRES_URL",
	},
	platform.MySQL: {
		flags: []string{
			"--publish", "3306:3306",
			"--env", "MYSQL_DATABASE=ptah_test",
			"--env", "MYSQL_USER=ptah_user",
			"--env", "MYSQL_PASSWORD=ptah_password",
			"--env", "MYSQL_ROOT_PASSWORD=root_password",
			"--tmpfs", "/var/lib/mysql:rw,noexec,nosuid,size=1024m",
		},
		url:                "mysql://root:root_password@tcp(127.0.0.1:3306)/ptah_test",
		suiteDatabase:      "mysql",
		suiteURLEnv:        "MYSQL_URL",
		suiteURL:           "mysql://ptah_user:ptah_password@tcp(127.0.0.1:3306)/ptah_test",
		suiteCleanupURLEnv: "MYSQL_CLEANUP_URL",
		suiteCleanupURL:    "mysql://root:root_password@tcp(127.0.0.1:3306)/ptah_test",
	},
	platform.MariaDB: {
		flags: []string{
			"--publish", "3306:3306",
			"--env", "MARIADB_DATABASE=ptah_test",
			"--env", "MARIADB_USER=ptah_user",
			"--env", "MARIADB_PASSWORD=ptah_password",
			"--env", "MARIADB_ROOT_PASSWORD=root_password",
			"--tmpfs", "/var/lib/mysql:rw,noexec,nosuid,size=1024m",
		},
		url:                "mariadb://root:root_password@tcp(127.0.0.1:3306)/ptah_test",
		suiteDatabase:      "mariadb",
		suiteURLEnv:        "MARIADB_URL",
		suiteURL:           "mariadb://ptah_user:ptah_password@tcp(127.0.0.1:3306)/ptah_test",
		suiteCleanupURLEnv: "MARIADB_CLEANUP_URL",
		suiteCleanupURL:    "mariadb://root:root_password@tcp(127.0.0.1:3306)/ptah_test",
	},
	platform.ClickHouse: {
		// The native protocol port, not 8123: the probe connects with the Go
		// driver the rest of Ptah uses, and CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT
		// is what lets the RBAC experiments create a role at all. Both are taken
		// from docker-compose.yaml so the two ways of starting a ClickHouse for
		// Ptah cannot describe different servers (stokaro/ptah#916).
		flags: []string{
			"--publish", "9000:9000",
			"--env", "CLICKHOUSE_DB=ptah_test",
			"--env", "CLICKHOUSE_USER=ptah_user",
			"--env", "CLICKHOUSE_PASSWORD=ptah_password",
			"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		},
		url: "clickhouse://ptah_user:ptah_password@127.0.0.1:9000/ptah_test",
		// Probe only, deliberately. The integration suite already runs against
		// ClickHouse through docker-compose, on the one line that file pins;
		// naming a suite target here would additionally run it on 24.10, 25.8
		// and 26.3, which is a coverage decision about the suite rather than
		// about the capability model this cell exists to measure.
		suiteSkip: "the integration runner reaches ClickHouse through docker-compose on the pinned line; " +
			"this cell adds capability-probe coverage for the other declared lines and does not move " +
			"the suite onto them (stokaro/ptah#916)",
	},
	platform.CockroachDB: {
		flags:         []string{"--publish", "26257:26257"},
		command:       []string{"start-single-node", "--insecure", "--advertise-addr=localhost:26257"},
		url:           "cockroachdb://root@127.0.0.1:26257/defaultdb?sslmode=disable",
		suiteDatabase: "cockroachdb",
		suiteURLEnv:   "COCKROACHDB_URL",
	},
	platform.Spanner: {
		flags: []string{"--publish", "5432:5432"},
		url:   "spanner://127.0.0.1:5432/ptah_test?sslmode=disable",
		suiteSkip: "the integration runner has no Spanner target: Ptah's index read is " +
			"pg_index/pg_class/pg_am/pg_get_indexdef throughout and Spanner exposes indexes " +
			"through information_schema, so the suite would fail on reading a schema rather " +
			"than on anything it set out to test (stokaro/ptah#942)",
	},
	platform.YugabyteDB: {
		flags: []string{"--publish", "5433:5433"},
		command: []string{
			"bash", "-lc",
			"ip=$(hostname -i); ip=${ip%% *}; exec bin/yugabyted start --daemon=false --advertise_address=$ip --ui=false",
		},
		url:           "yugabytedb://yugabyte@127.0.0.1:5433/yugabyte?sslmode=disable",
		suiteDatabase: "yugabytedb",
		suiteURLEnv:   "YUGABYTEDB_URL",
	},
}

// CIMatrix derives the pipeline's fan-out from the declared matrix.
//
// Runnability is derived, never listed. A line runs when a container
// reproduces it AND the probe has a statement table for its dialect, and both
// halves are read from the same code the probe itself uses — planFor and the
// cell's own Image — so a ClickHouse plan added tomorrow turns four skipped
// cells into four running ones with no workflow edit at all.
func CIMatrix() Matrix {
	matrix := Matrix{Declared: len(Cells)}
	for _, cell := range Cells {
		converted := toCICell(cell)
		if converted.Runnable {
			matrix.Cells = append(matrix.Cells, converted)
			continue
		}
		matrix.Skipped = append(matrix.Skipped, converted)
	}
	return matrix
}

func toCICell(cell Cell) CICell {
	converted := CICell{
		ID:                 CellID(cell),
		Dialect:            cell.Dialect,
		Line:               cell.Line,
		Label:              cell.Label,
		Preset:             cell.PresetName,
		Support:            cell.Support,
		Refinement:         string(cell.Refinement),
		Image:              cell.Image,
		TagPinsLine:        tagPinsLine(cell),
		ResolveNewestPatch: cell.ResolveNewestPatch,
		Note:               cell.Note,
	}
	reasons := skipReasons(cell)
	if len(reasons) > 0 {
		converted.Skip = strings.Join(reasons, "; ")
		return converted
	}
	recipe := launchers[cell.Dialect]
	converted.Runnable = true
	converted.DockerRun = recipe.dockerRun(cell.Image)
	converted.URL = recipe.url
	converted.SuiteDatabase = recipe.suiteDatabase
	converted.SuiteURLEnv = recipe.suiteURLEnv
	converted.SuiteURL = recipe.suiteURL
	if converted.SuiteURL == "" {
		converted.SuiteURL = recipe.url
	}
	converted.SuiteCleanupURLEnv = recipe.suiteCleanupURLEnv
	converted.SuiteCleanupURL = recipe.suiteCleanupURL
	converted.SuiteSkip = recipe.suiteSkip
	// A recipe with no runner target carries no URL to hand it either; the
	// fallback above would otherwise give the suite the probe's own URL and
	// point it at a dialect it cannot run.
	if recipe.suiteDatabase == "" {
		converted.SuiteURL = ""
	}
	return converted
}

// skipReasons lists every reason a declared line cannot be executed, in the
// order a reader wants them: no server first, then nothing to ask it.
//
// A dialect with no probe plan is not also reported as having no launch
// recipe. The recipe is missing BECAUSE there is nothing to ask, so printing
// both states one fact twice and buries the one that has to change first.
func skipReasons(cell Cell) []string {
	var reasons []string
	if cell.Image == "" {
		reasons = append(reasons, "no container image is declared for this line")
	}
	if _, planned := planFor(cell.Dialect); !planned {
		return append(reasons, fmt.Sprintf(
			"the capability probe has no statement table for the %s dialect, so a server on this line would be asked nothing",
			cell.Dialect))
	}
	if _, known := launchers[cell.Dialect]; !known && cell.Image != "" {
		reasons = append(reasons, fmt.Sprintf(
			"the %s dialect has a probe plan and no launch recipe, so nothing here can start the server to run it",
			cell.Dialect))
	}
	return reasons
}

func (l launcher) dockerRun(image string) []string {
	argv := slices.Clone(l.flags)
	argv = append(argv, image)
	return append(argv, l.command...)
}

// CellID is the identifier a job, an artifact and a result file use for a
// cell. Dots become dashes so the id survives every one of those namespaces.
func CellID(cell Cell) string {
	return cell.Dialect + "-" + strings.ReplaceAll(cell.Line, ".", "-")
}

// tagPinsLine reports whether the image tag names the line itself.
//
// The comparison is equality and not a prefix on purpose. A prefix test would
// call cockroachdb/cockroach:v26.2.5 a pin on line 26.2, which is the exact
// frozen-patch shape the scoping rule rejects: the tag stops being the newest
// patch the moment the vendor ships v26.2.6.
func tagPinsLine(cell Cell) bool {
	_, tag := splitImageTag(cell.Image)
	return tag != "" && (tag == cell.Line || tag == "v"+cell.Line || tag == "latest-v"+cell.Line)
}

// splitImageTag separates an image reference into repository and tag. The
// colon counts only after the last slash, so a registry host with a port is
// not mistaken for a tag.
func splitImageTag(ref string) (repository, tag string) {
	colon := strings.LastIndex(ref, ":")
	if colon <= strings.LastIndex(ref, "/") {
		return ref, ""
	}
	return ref[:colon], ref[colon+1:]
}

// WriteMatrixMarkdown prints the matrix as the documentation table.
//
// The documentation matrix is generated from Cells rather than written beside
// it because a third hand-maintained list of supported versions is what
// stokaro/ptah#1341 exists to prevent. scripts/check-version-matrix.sh fails
// the build when the checked-in table and this output differ.
func WriteMatrixMarkdown(w io.Writer) {
	matrix := CIMatrix()
	fmt.Fprintf(w, "| Dialect | Release line | Support | Capability preset | Refinement | Container image | Tag names the line | Probed per pull request |\n")
	fmt.Fprintf(w, "| --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, cell := range append(slices.Clone(matrix.Cells), matrix.Skipped...) {
		fmt.Fprintf(w, "| `%s` | %s | %s | %s | %s | %s | %s | %s |\n",
			cell.Dialect, markdownLine(cell), markdownSupport(cell), markdownPreset(cell), cell.Refinement,
			markdownImage(cell), markdownTagPinsLine(cell), markdownProbed(cell))
	}
}

// markdownSupport is the one word both renderings print for a line's promise.
//
// It stays one word because the level's sentence lives in
// [capability.SupportLevel.Doc] and belongs under the table, not inside a cell
// the site's responsive check measures. A level the vocabulary does not define
// prints "undeclared" rather than nothing: the zero value renders as an empty
// cell, and an empty cell is equally consistent with a line nobody assigned and
// with a renderer that dropped the column, which are not the same bug.
func markdownSupport(cell CICell) string {
	if !cell.Support.Valid() {
		return "undeclared"
	}
	return cell.Support.String()
}

func markdownLine(cell CICell) string {
	if cell.Label == "" {
		return cell.Line
	}
	return fmt.Sprintf("%s (%s)", cell.Line, cell.Label)
}

func markdownPreset(cell CICell) string {
	if cell.Preset == "" {
		return "none yet"
	}
	return "`" + cell.Preset + "`"
}

func markdownImage(cell CICell) string {
	if cell.Image == "" {
		return "none"
	}
	return "`" + cell.Image + "`"
}

func markdownTagPinsLine(cell CICell) string {
	if cell.Image == "" {
		return "n/a"
	}
	if cell.TagPinsLine {
		return "yes"
	}
	return "no"
}

func markdownProbed(cell CICell) string {
	if cell.Runnable {
		return "yes"
	}
	return "no: " + cell.Skip
}

// WriteMatrixSummary prints the narrow rendering of the same matrix, for the
// documentation site.
//
// It is a second VIEW of one declaration, never a second list. The site's
// responsive check refuses a table wider than the reading column, and it is
// right to: the wide rendering's container-image and skip-reason columns are
// cut off at a phone width, which is worse than not showing them. So the
// columns that stay are the ones that answer "which versions are supported and
// which of them does CI measure", and the rest is written underneath as prose
// the same generator produces.
//
// Refinement is deliberately NOT among them, and its removal is what made room
// for Support. Measured: with both, this table renders 651px inside the site's
// 632px reading column and the responsive check fails the build. Of the two,
// Refinement is the one a reader of this page did not come for — it says how
// the resolver REACHES a preset, which is a fact about Ptah's internals, while
// Support says how much testing stands behind the line, which is the question
// the page exists to answer. The wide rendering keeps both.
func WriteMatrixSummary(w io.Writer) {
	matrix := CIMatrix()
	fmt.Fprintf(w, "| Dialect | Release line | Support | Capability preset | Probed |\n")
	fmt.Fprintf(w, "| --- | --- | --- | --- | --- |\n")
	for _, cell := range slices.Concat(matrix.Cells, matrix.Skipped) {
		fmt.Fprintf(w, "| `%s` | %s | %s | %s | %s |\n",
			cell.Dialect, markdownLine(cell), markdownSupport(cell), markdownPreset(cell),
			markdownProbedShort(cell))
	}
	writeSummaryNotes(w, matrix)
}

func markdownProbedShort(cell CICell) string {
	if cell.Runnable {
		return "yes"
	}
	return "no"
}

func writeSummaryNotes(w io.Writer, matrix Matrix) {
	fmt.Fprintf(w, "\nDeclared release lines: %d. Probed on every pull request: %d.\n",
		matrix.Declared, len(matrix.Cells))
	fmt.Fprintf(w, "\nSupport levels across the %d declared lines: %s.\n",
		matrix.Declared, strings.Join(supportCensus(slices.Concat(matrix.Cells, matrix.Skipped)), ", "))
	fmt.Fprintf(w, "\nLines that are declared and not probed, and why:\n\n")
	for _, cell := range matrix.Skipped {
		fmt.Fprintf(w, "- `%s` %s — %s.\n", cell.Dialect, cell.Line, cell.Skip)
	}
	fmt.Fprintf(w, "\nLines whose container tag does not name the line, so which patch it resolves to has to be read off the tag:\n\n")
	for _, cell := range slices.Concat(matrix.Cells, matrix.Skipped) {
		writeTagNote(w, cell)
	}
}

// supportCensus counts the rendered rows per promise, as the phrases the
// summary joins: "19 certified", "2 legacy-tested", "5 best-effort".
//
// It counts rather than states. A written-out tally is true on the day it is
// typed and silently false on the day a line moves to legacy-tested, which is
// exactly the drift the generated matrix exists to remove — and the tally is
// the number a reader checks the table against, so a stale one is worse than
// none.
//
// Every rendered row lands in some phrase: the levels the vocabulary defines
// come first, in the order [capability.SupportLevels] presents them, and a
// label outside it is appended rather than dropped, because a row counted
// nowhere makes the parts add up to less than the declared total with nothing
// to show for the difference. Levels nobody declared contribute no phrase; "0
// known-incompatible" is not a fact about this repository, only about a word.
func supportCensus(cells []CICell) []string {
	levels := capability.SupportLevels()
	counts := make(map[string]int, len(levels))
	labels := make([]string, 0, len(levels))
	for _, level := range levels {
		labels = append(labels, level.String())
		counts[level.String()] = 0
	}
	for _, cell := range cells {
		label := markdownSupport(cell)
		if _, known := counts[label]; !known {
			labels = append(labels, label)
		}
		counts[label]++
	}
	phrases := make([]string, 0, len(labels))
	for _, label := range labels {
		if counts[label] == 0 {
			continue
		}
		phrases = append(phrases, fmt.Sprintf("%d %s", counts[label], label))
	}
	return phrases
}

func writeTagNote(w io.Writer, cell CICell) {
	if cell.Image == "" || cell.TagPinsLine {
		return
	}
	fmt.Fprintf(w, "- `%s` %s, pinned as `%s`.\n", cell.Dialect, cell.Line, cell.Image)
}
